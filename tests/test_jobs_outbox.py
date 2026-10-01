from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta

import pytest

from app.core.clock import FrozenClock, to_storage
from app.core.errors import ConflictError
from app.database import get_connection, transaction
from app.germplasm.service import GermplasmService
from app.services.jobs import JobRunner, JobService
from app.services.outbox import OutboxService
from app.services.reminders import default_handlers, enqueue_retest_scan, handle_retest_reminder_scan
from test_germplasm_workflow import create_stored_lot

T0 = datetime(2026, 10, 1, 2, 0, tzinfo=UTC)


def make_lot_with_schedule(suffix: str, due_on: str, clock=None, *, status: str = "pending") -> tuple[int, int]:
    """建立已入库批次并直接登记一条复检日程，返回 (lot_id, schedule_id)。"""
    with transaction(immediate=True) as connection:
        service = GermplasmService(connection, clock)
        _, lot, _ = create_stored_lot(service, suffix)
        policy = service.viability.create_policy({
            "crop_name": "水稻", "risk_level": "medium", "interval_months": 12, "warning_days": 30,
            "minimum_germination_percent": 75, "effective_from": "2026-01-01", "effective_to": None,
            "created_by": "技术负责人",
        })
        timestamp = to_storage(datetime(2026, 1, 1, tzinfo=UTC))
        cursor = connection.execute(
            "INSERT INTO retest_schedules(lot_id,source_test_id,policy_id,due_on,status,reason,created_at,updated_at) "
            "VALUES(?,NULL,?,?,?,?,?,?)",
            (lot["id"], policy["id"], due_on, status, "周期复检", timestamp, timestamp),
        )
        return int(lot["id"]), int(cursor.lastrowid)


def technician_notifications() -> int:
    outbox = OutboxService(get_connection())
    _, total = outbox.list_notifications(recipient="technician", limit=500)
    return total


def test_scan_job_is_transactional_and_never_duplicates_notifications(client):
    clock = FrozenClock(T0)
    make_lot_with_schedule("A1", "2026-10-01", clock)
    make_lot_with_schedule("A2", "2026-09-30", clock)
    _, future_schedule = make_lot_with_schedule("A3", "2027-01-01", clock)

    with transaction(immediate=True) as connection:
        job = enqueue_retest_scan(connection, date(2026, 10, 1), clock)
    with transaction(immediate=True) as connection:
        again = enqueue_retest_scan(connection, date(2026, 10, 1), clock)
    assert again["id"] == job["id"], "同一日期的扫描作业应按去重键复用"

    runner = JobRunner(get_connection(), clock, handlers=default_handlers())
    outcomes = runner.tick("worker-a")
    assert [item["outcome"] for item in outcomes] == ["completed"]
    result = outcomes[0]["result"]
    assert result["newly_notified"] == 2
    assert result["events_created"] == 2
    assert result["lot_nos"] == ["LOT-A1", "LOT-A2"]

    connection = get_connection()
    notified = connection.execute(
        "SELECT COUNT(*) FROM retest_schedules WHERE status='notified'"
    ).fetchone()[0]
    assert notified == 2
    future = connection.execute(
        "SELECT status FROM retest_schedules WHERE id=?", (future_schedule,)
    ).fetchone()[0]
    assert future == "pending", "未到期日程不应被标记"
    events = connection.execute(
        "SELECT event_key,payload_json FROM outbox_events WHERE event_type='retest.reminder' ORDER BY id"
    ).fetchall()
    assert [row[0] for row in events] == [f"retest-reminder:{sid}" for sid in result["schedule_ids"]]
    # 事件按到期日先后写入，LOT-A2 到期更早
    assert [json.loads(row[1])["lot_no"] for row in events] == ["LOT-A2", "LOT-A1"]

    detail = JobService(get_connection(), clock).detail(job["id"])
    assert [item["outcome"] for item in detail["attempt_history"]] == ["completed"]
    assert detail["attempt_history"][0]["worker"] == "worker-a"

    # 业务提交后进程中断（尚未发布）：发布端补投，批次进入待办
    summary = OutboxService(get_connection(), clock).publish_batch("pub-1")
    assert summary["confirmed"] >= 2
    assert technician_notifications() == 2

    # 重启后第二天再次扫描：日程不重复标记、事件不重复写入、通知不重复投递
    with transaction(immediate=True) as connection:
        job2 = enqueue_retest_scan(connection, date(2026, 10, 2), clock)
    assert job2["id"] != job["id"]
    rerun = runner.tick("worker-a")
    assert rerun[0]["result"]["newly_notified"] == 0
    assert rerun[0]["result"]["events_created"] == 0
    assert rerun[0]["result"]["events_reused"] == 2
    follow_up = OutboxService(get_connection(), clock).publish_batch("pub-1")
    assert follow_up["confirmed"] == 0
    assert technician_notifications() == 2


def test_lease_takeover_and_late_receipt_cannot_overwrite(client):
    clock = FrozenClock(T0)
    make_lot_with_schedule("B1", "2026-10-01", clock)
    with transaction(immediate=True) as connection:
        job = enqueue_retest_scan(connection, date(2026, 10, 1), clock)

    with transaction(immediate=True) as connection:
        claimed_a = JobService(connection, clock).claim("worker-a", lease_seconds=60)
    assert claimed_a["attempts"] == 1

    with transaction(immediate=True) as connection:
        assert JobService(connection, clock).claim("worker-b", lease_seconds=60) is None

    clock.advance(seconds=61)
    with transaction(immediate=True) as connection:
        claimed_b = JobService(connection, clock).claim("worker-b", lease_seconds=60)
    assert claimed_b["attempts"] == 2
    assert claimed_b["locked_by"] == "worker-b"

    with transaction(immediate=True) as connection:
        result = handle_retest_reminder_scan(connection, json.loads(claimed_b["payload_json"]), clock)
        completed = JobService(connection, clock).complete(
            claimed_b["id"], "worker-b", claimed_b["lease_token"], result
        )
    assert completed["status"] == "completed"

    with pytest.raises(ConflictError), transaction(immediate=True) as connection:
        JobService(connection, clock).complete(job["id"], "worker-a", claimed_a["lease_token"], {"lot_nos": []})

    detail = JobService(get_connection(), clock).detail(job["id"])
    assert detail["status"] == "completed"
    assert detail["result"]["lot_nos"] == ["LOT-B1"], "旧执行者的迟到回执不得覆盖新结果"
    assert [item["outcome"] for item in detail["attempt_history"]] == ["expired", "completed"]
    assert [item["worker"] for item in detail["attempt_history"]] == ["worker-a", "worker-b"]


def test_failed_handler_rolls_back_schedule_and_outbox_changes(client):
    clock = FrozenClock(T0)
    _, schedule_id = make_lot_with_schedule("E1", "2026-10-01", clock)

    def poison(connection, payload, injected_clock):
        OutboxService(connection, injected_clock).emit(
            "poison-1", "retest.reminder", "retest_schedule", "1", {}
        )
        raise RuntimeError("写入中途失败")

    runner = JobRunner(get_connection(), clock, handlers={"poison.job": poison})
    with transaction(immediate=True) as connection:
        JobService(connection, clock).enqueue("poison.job", "poison-1", {})
    outcomes = runner.tick("worker-x")
    assert outcomes[0]["outcome"] == "failed"

    connection = get_connection()
    assert connection.execute(
        "SELECT COUNT(*) FROM outbox_events WHERE event_key='poison-1'"
    ).fetchone()[0] == 0, "失败作业的事件写入必须随事务回滚"
    status = connection.execute(
        "SELECT status FROM retest_schedules WHERE id=?", (schedule_id,)
    ).fetchone()[0]
    assert status == "pending", "失败作业的日程变更必须随事务回滚"


def test_crash_before_confirm_redelivers_without_duplicates(client):
    clock = FrozenClock(T0)
    make_lot_with_schedule("C1", "2026-10-01", clock)
    make_lot_with_schedule("C2", "2026-10-01", clock)
    # 先清空建库时产生的档案事件，聚焦复检提醒
    assert OutboxService(get_connection(), clock).publish_batch("pub-0")["confirmed"] == 4

    runner = JobRunner(get_connection(), clock, handlers=default_handlers())
    with transaction(immediate=True) as connection:
        enqueue_retest_scan(connection, date(2026, 10, 1), clock)
    assert runner.tick("worker-a")[0]["outcome"] == "completed"

    outbox = OutboxService(get_connection(), clock)
    with transaction(immediate=True):
        leased = outbox.lease_batch("pub-a", lease_seconds=60)
    assert len(leased) == 2
    for event in leased:
        with transaction(immediate=True):
            receipt = outbox.deliver_event(event["id"], "pub-a", event["lease_token"])
        assert receipt["created"] is True
    # 确认前进程崩溃：通知已入箱，事件仍处投递中
    assert technician_notifications() == 2
    processing, _ = outbox.list_events(status="processing")
    assert len(processing) == 2

    clock.advance(seconds=61)
    summary = outbox.publish_batch("pub-b")
    assert summary["leased"] == 2
    assert summary["confirmed"] == 2
    assert technician_notifications() == 2, "重复投递必须被信箱去重吸收"
    _, total = outbox.list_notifications(limit=500)
    assert total == 6
    assert outbox.cursor() == leased[-1]["id"]
    detail = outbox.detail(leased[0]["id"])
    assert [item["outcome"] for item in detail["attempt_history"]] == ["expired", "published"]
    assert detail["delivery"]["created"] is False


def test_stale_outbox_confirm_rejected_after_takeover(client):
    clock = FrozenClock(T0)
    outbox = OutboxService(get_connection(), clock)
    with transaction(immediate=True) as connection:
        event, _ = OutboxService(connection, clock).emit(
            "evt-stale", "retest.reminder", "retest_schedule", "1",
            {"lot_id": 1, "lot_no": "LOT-Z1", "recipient": "technician"},
        )
    with transaction(immediate=True):
        leased_a = outbox.lease_batch("pub-a", lease_seconds=60)
    token_a = leased_a[0]["lease_token"]

    clock.advance(seconds=61)
    with transaction(immediate=True):
        leased_b = outbox.lease_batch("pub-b", lease_seconds=60)
    assert leased_b[0]["attempts"] == 2

    with pytest.raises(ConflictError), transaction(immediate=True):
        outbox.confirm_batch("pub-a", [(event["id"], token_a)])
    still_processing = outbox.detail(event["id"])
    assert still_processing["status"] == "processing"
    assert still_processing["locked_by"] == "pub-b"

    with transaction(immediate=True):
        outbox.deliver_event(event["id"], "pub-b", leased_b[0]["lease_token"])
    with transaction(immediate=True):
        summary = outbox.confirm_batch("pub-b", [(event["id"], leased_b[0]["lease_token"])])
    assert summary["confirmed"] == 1
    assert outbox.detail(event["id"])["status"] == "published"


def test_job_backoff_dead_letter_and_requeue_with_frozen_clock(client):
    clock = FrozenClock(T0)
    jobs = JobService(get_connection(), clock, backoff_base_seconds=30, backoff_cap_seconds=120)
    with transaction(immediate=True):
        job = jobs.enqueue("flaky.job", "flaky-1", {}, max_attempts=3)

    with transaction(immediate=True):
        claimed = jobs.claim("worker")
        failed = jobs.fail(claimed["id"], "worker", claimed["lease_token"], "第一次失败")
    assert failed["status"] == "pending"
    assert failed["available_at"] == to_storage(T0 + timedelta(seconds=30))
    with transaction(immediate=True):
        assert jobs.claim("worker") is None, "退避期间不得被领取"

    clock.advance(seconds=30)
    with transaction(immediate=True):
        claimed = jobs.claim("worker")
        failed = jobs.fail(claimed["id"], "worker", claimed["lease_token"], "第二次失败")
    assert failed["available_at"] == to_storage(clock.now() + timedelta(seconds=60))

    clock.advance(seconds=60)
    with transaction(immediate=True):
        claimed = jobs.claim("worker")
        failed = jobs.fail(claimed["id"], "worker", claimed["lease_token"], "第三次失败")
    assert failed["status"] == "failed"
    assert failed["dead_lettered_at"] is not None, "超过上限应进入人工处理"

    clock.advance(days=1)
    with transaction(immediate=True):
        assert jobs.claim("worker") is None, "人工处理中的任务不再被领取"

    detail = jobs.detail(job["id"])
    assert [item["outcome"] for item in detail["attempt_history"]] == ["failed", "failed", "dead_lettered"]

    with transaction(immediate=True):
        requeued = jobs.requeue(job["id"])
    assert requeued["status"] == "pending"
    assert requeued["attempts"] == 0
    with transaction(immediate=True):
        assert jobs.claim("worker") is not None


def test_outbox_dead_letter_replay_does_not_touch_business_state(client):
    clock = FrozenClock(T0)
    make_lot_with_schedule("F1", "2026-10-01", clock)
    healthy = OutboxService(get_connection(), clock)
    assert healthy.publish_batch("pub-0")["confirmed"] == 2

    def broken_sink(event):
        raise RuntimeError("通知通道不可用")

    broken = OutboxService(get_connection(), clock, sink=broken_sink, backoff_base_seconds=15)
    with transaction(immediate=True) as connection:
        event, _ = OutboxService(connection, clock).emit(
            "evt-f1", "retest.reminder", "retest_schedule", "1",
            {"lot_id": 1, "lot_no": "LOT-F1", "recipient": "technician",
             "title": "批次 LOT-F1 活力复检提醒", "message": "应于 2026-10-01 前完成复检"},
            max_attempts=2,
        )
    first = broken.publish_batch("pub-x")
    assert first["failures"] == [{"event_id": event["id"], "error": "通知通道不可用"}]
    pending = broken.detail(event["id"])
    assert pending["status"] == "pending"
    assert pending["available_at"] == to_storage(T0 + timedelta(seconds=15))

    clock.advance(seconds=15)
    broken.publish_batch("pub-x")
    dead = broken.detail(event["id"])
    assert dead["status"] == "failed"
    assert dead["dead_lettered_at"] is not None
    assert [item["outcome"] for item in dead["attempt_history"]] == ["failed", "dead_lettered"]
    assert healthy.cursor() == event["id"] - 1, "未解决的事件会挡住游标"

    schedules_before = get_connection().execute("SELECT * FROM retest_schedules ORDER BY id").fetchall()
    jobs_before = get_connection().execute("SELECT * FROM background_jobs ORDER BY id").fetchall()

    with transaction(immediate=True):
        replayed = healthy.replay_event(event["id"], "admin")
    assert replayed["event"]["status"] == "published"
    assert replayed["delivery"]["created"] is True
    assert healthy.cursor() == event["id"], "重放成功后游标恢复推进"
    assert technician_notifications() == 1

    schedules_after = get_connection().execute("SELECT * FROM retest_schedules ORDER BY id").fetchall()
    jobs_after = get_connection().execute("SELECT * FROM background_jobs ORDER BY id").fetchall()
    assert [tuple(row) for row in schedules_before] == [tuple(row) for row in schedules_after]
    assert [tuple(row) for row in jobs_before] == [tuple(row) for row in jobs_after]

    with transaction(immediate=True):
        again = healthy.replay_event(event["id"], "admin")
    assert again["delivery"]["created"] is False, "重复重放不得产生重复通知"
    assert technician_notifications() == 1


def test_scan_recovers_legacy_notified_schedule_without_event(client):
    clock = FrozenClock(T0)
    _, schedule_id = make_lot_with_schedule("G1", "2026-10-01", clock, status="notified")

    runner = JobRunner(get_connection(), clock, handlers=default_handlers())
    with transaction(immediate=True) as connection:
        enqueue_retest_scan(connection, date(2026, 10, 1), clock)
    result = runner.tick("worker-a")[0]["result"]
    assert result["newly_notified"] == 0
    assert result["events_created"] == 1, "历史遗留的已提醒日程必须补齐事件"

    OutboxService(get_connection(), clock).publish_batch("pub-1")
    assert technician_notifications() == 1
    event = get_connection().execute(
        "SELECT status FROM outbox_events WHERE event_key=?", (f"retest-reminder:{schedule_id}",)
    ).fetchone()
    assert event[0] == "published"


def test_runner_retries_failed_handler_after_backoff(client):
    clock = FrozenClock(T0)
    calls = {"count": 0}

    def flaky(connection, payload, injected_clock):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("暂时性故障")
        return {"ok": True}

    runner = JobRunner(get_connection(), clock, handlers={"flaky.job": flaky}, backoff_base_seconds=30)
    with transaction(immediate=True) as connection:
        JobService(connection, clock).enqueue("flaky.job", "flaky-1", {})
    assert runner.tick("worker")[0]["outcome"] == "failed"
    assert runner.tick("worker") == [], "退避时间内不得重试"
    clock.advance(seconds=30)
    assert runner.tick("worker")[0]["outcome"] == "completed"
    assert calls["count"] == 2


def test_api_exposes_attempts_lease_owner_lot_and_delivery(client, admin):
    headers = admin["headers"]
    make_lot_with_schedule("H1", "2020-01-01")
    with transaction(immediate=True) as connection:
        enqueue_retest_scan(connection, date.today())

    run = client.post("/api/system/jobs/run", headers=headers, json={"limit": 5})
    assert run.status_code == 200, run.text
    outcome = run.json()["outcomes"][0]
    assert outcome["outcome"] == "completed"
    job_id = outcome["job_id"]

    jobs = client.get("/api/system/jobs", headers=headers).json()
    assert jobs["total"] == 1
    detail = client.get(f"/api/system/jobs/{job_id}", headers=headers).json()
    assert detail["attempt_history"][0]["worker"] == "api:admin"
    assert detail["result"]["lot_nos"] == ["LOT-H1"]

    published = client.post("/api/system/outbox/publish", headers=headers, json={})
    assert published.status_code == 200, published.text
    assert published.json()["confirmed"] >= 1

    listing = client.get("/api/system/outbox", headers=headers).json()
    reminder = next(item for item in listing["items"] if item["event_type"] == "retest.reminder")
    assert reminder["lot"]["lot_no"] == "LOT-H1"
    assert reminder["delivery"]["created"] is True
    assert listing["cursor"] == max(item["id"] for item in listing["items"])

    event_detail = client.get(f"/api/system/outbox/{reminder['id']}", headers=headers).json()
    assert event_detail["attempt_history"][0]["outcome"] == "published"
    assert event_detail["attempt_history"][0]["publisher"] == "api:admin"
    assert event_detail["payload"]["schedule_id"]

    notifications = client.get("/api/system/notifications", headers=headers, params={"recipient": "technician"})
    assert notifications.json()["total"] == 1
    assert "LOT-H1" in notifications.json()["items"][0]["title"]

    replay = client.post(f"/api/system/outbox/{reminder['id']}/replay", headers=headers)
    assert replay.status_code == 200, replay.text
    assert replay.json()["delivery"]["created"] is False
    assert client.get(
        "/api/system/notifications", headers=headers, params={"recipient": "technician"}
    ).json()["total"] == 1


def test_system_endpoints_require_authentication(client):
    assert client.get("/api/system/jobs").status_code == 401
    assert client.get("/api/system/outbox").status_code == 401
    assert client.get("/api/system/notifications").status_code == 401
