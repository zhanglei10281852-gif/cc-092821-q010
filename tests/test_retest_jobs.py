from __future__ import annotations

from datetime import date, timedelta

import pytest

from app.core.clock import FrozenClock, to_storage
from app.core.errors import ConflictError
from app.database import get_connection, transaction
from app.germplasm.jobs import RETEST_REMINDERS_JOB, build_executor, enqueue_retest_reminders
from app.germplasm.service import GermplasmService
from app.services.jobs import JobService
from app.services.notifications import RecordingGateway
from app.services.outbox import OutboxService
from conftest import T0, make_pending_schedule, schedule_status


def test_expired_lease_takeover_and_stale_receipt_rejected(client):
    """过期租约可被接管，旧执行者的迟到回执不得覆盖新结果。"""
    clock = FrozenClock(T0)
    connection = get_connection()
    service = JobService(connection, clock, lease_seconds=60)
    with transaction(immediate=True):
        job = service.enqueue("nightly.reminders", "reminders:2026-10-01", {"date": "2026-10-01"})
        first = service.claim("worker-a")
        assert first["id"] == job["id"] and first["attempt_no"] == 1
        # 租约未过期时其他执行者领不到
        assert service.claim("worker-b") is None
        clock.advance(seconds=61)
        second = service.claim("worker-b")
        assert second["id"] == job["id"] and second["attempt_no"] == 2
        # 旧执行者的迟到回执被拒绝
        with pytest.raises(ConflictError):
            service.complete(job["id"], "worker-a", first["lease_token"], {"count": 1})
        with pytest.raises(ConflictError):
            service.fail(job["id"], "worker-a", first["lease_token"], "迟到失败")
        completed = service.complete(job["id"], "worker-b", second["lease_token"], {"count": 2})
        assert completed["status"] == "completed" and completed["result"] == {"count": 2}
    attempts = service.attempts(job["id"])
    assert [item["outcome"] for item in attempts] == ["expired", "completed"]
    assert attempts[0]["worker"] == "worker-a" and attempts[1]["worker"] == "worker-b"


def test_crash_after_claim_recovers_via_lease_takeover(client):
    """进程领取后崩溃：租约过期被接管，日程变更与事件写入只生效一次。"""
    clock = FrozenClock(T0)
    connection = get_connection()
    with transaction(immediate=True):
        lot, schedule_id = make_pending_schedule(connection, clock, "101")
        job = enqueue_retest_reminders(connection, clock, due_before=date(2026, 10, 6))
    # 夜间进程领取后崩溃，业务事务从未开始
    with transaction(immediate=True):
        claimed = JobService(connection, clock, lease_seconds=60).claim("night-worker-1")
    assert claimed["id"] == job["id"]
    assert schedule_status(connection, schedule_id) == "pending"
    # 重启后租约过期，另一个执行者接管并完成
    clock.advance(seconds=61)
    executor = build_executor(connection, clock, lease_seconds=60)
    outcome = executor.run_next("night-worker-2")
    assert outcome["outcome"] == "completed"
    assert outcome["result"]["reminder_count"] == 1
    assert schedule_status(connection, schedule_id) == "notified"
    events = OutboxService(connection, clock).list_events()
    assert len(events) == 1 and events[0]["status"] == "pending"
    assert events[0]["event_key"] == f"retest-reminder:{schedule_id}"
    attempts = JobService(connection, clock).attempts(job["id"])
    assert [item["outcome"] for item in attempts] == ["expired", "completed"]


def test_business_failure_rolls_back_then_retry_succeeds(client):
    """业务中断整体回滚：没有半个批次落地，退避后重试成功。"""
    clock = FrozenClock(T0)
    connection = get_connection()
    with transaction(immediate=True):
        _, schedule_id = make_pending_schedule(connection, clock, "102")
        job = enqueue_retest_reminders(connection, clock, due_before=date(2026, 10, 6))
    executor = build_executor(connection, clock, lease_seconds=60)
    real_handler = executor.handlers[RETEST_REMINDERS_JOB]
    calls = {"count": 0}

    def flaky(conn, payload, context):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("模拟进程崩溃")
        return real_handler(conn, payload, context)

    executor.register(RETEST_REMINDERS_JOB, flaky)
    first = executor.run_next("night-worker")
    assert first["outcome"] == "retry"
    assert first["available_at"] == to_storage(T0 + timedelta(seconds=30))
    # 回滚干净：日程仍待处理，事件箱为空
    assert schedule_status(connection, schedule_id) == "pending"
    assert connection.execute("SELECT COUNT(*) FROM outbox_events").fetchone()[0] == 0
    clock.advance(seconds=31)
    second = executor.run_next("night-worker")
    assert second["outcome"] == "completed"
    assert schedule_status(connection, schedule_id) == "notified"
    assert connection.execute("SELECT COUNT(*) FROM outbox_events").fetchone()[0] == 1
    assert JobService(connection, clock).require(job["id"])["status"] == "completed"


def test_crash_after_business_commit_recovers_via_publisher(client):
    """业务提交后崩溃：作业已完成、事件待投递，重启后发布端补投。"""
    clock = FrozenClock(T0)
    connection = get_connection()
    with transaction(immediate=True):
        _, schedule_id = make_pending_schedule(connection, clock, "103")
        enqueue_retest_reminders(connection, clock, due_before=date(2026, 10, 6))
    executor = build_executor(connection, clock, lease_seconds=60)
    assert executor.run_next("night-worker")["outcome"] == "completed"
    # 崩溃并重启：新的发布者实例接手待投递事件
    gateway = RecordingGateway()
    publisher = OutboxService(connection, clock, lease_seconds=60)
    result = publisher.publish_batch("publisher-1", gateway)
    assert result["claimed"] == 1 and len(result["delivered"]) == 1
    event = publisher.detail(result["delivered"][0]["event_id"])
    assert event["status"] == "published" and event["final_result"] == "delivered"
    assert event["related_lot"]["lot_no"] == "LOT-J-103"
    assert [item["outcome"] for item in event["deliveries"]] == ["delivered"]
    assert len(gateway.sent) == 1
    # 重复生成不会制造新事件
    again = GermplasmService(connection, clock).viability.generate_retest_reminders(date(2026, 10, 6))
    assert again["reminder_count"] == 0


def test_crash_before_confirm_does_not_duplicate_notifications(client):
    """确认前崩溃：事件被重新领取，网关按事件键去重，通知只发一次。"""
    clock = FrozenClock(T0)
    connection = get_connection()
    with transaction(immediate=True):
        make_pending_schedule(connection, clock, "104")
        make_pending_schedule(connection, clock, "105")
        enqueue_retest_reminders(connection, clock, due_before=date(2026, 10, 6))
    executor = build_executor(connection, clock, lease_seconds=60)
    assert executor.run_next("night-worker")["outcome"] == "completed"
    gateway = RecordingGateway()
    outbox = OutboxService(connection, clock, lease_seconds=60)
    # 第一位发布者领取、投递并登记，但在批量确认前崩溃
    with transaction(immediate=True):
        claimed = outbox.claim_batch("publisher-1")
    assert len(claimed) == 2
    for event in claimed:
        outcome = gateway.send(
            delivery_key=event["event_key"], event_type=event["event_type"],
            channel="notification", payload=event["payload"],
        )
        with transaction(immediate=True):
            outbox.record_delivery(
                event, publisher="publisher-1", delivery_key=event["event_key"], outcome=outcome
            )
    # 重启：租约过期后第二位发布者重新领取同一批事件
    clock.advance(seconds=61)
    restarted = OutboxService(connection, clock, lease_seconds=60)
    result = restarted.publish_batch("publisher-2", gateway)
    assert len(result["duplicates"]) == 2
    assert len(gateway.sent) == 2  # 通知没有重复发出
    published = restarted.list_events(status="published")
    assert len(published) == 2
    deliveries = restarted.deliveries(published[0]["id"])
    assert [item["outcome"] for item in deliveries] == ["delivered", "duplicate"]
    # 旧发布者的迟到确认被丢弃，不覆盖新结果
    late = restarted.confirm_batch(
        "publisher-1", [{"id": event["id"], "lease_token": event["lease_token"]} for event in claimed]
    )
    assert late["confirmed"] == [] and len(late["rejected"]) == 2


def test_publish_confirms_in_cursor_batches(client):
    """发布端按游标分批确认，直到没有待投递事件。"""
    clock = FrozenClock(T0)
    connection = get_connection()
    with transaction(immediate=True):
        for index in range(5):
            make_pending_schedule(connection, clock, f"2{index}")
        enqueue_retest_reminders(connection, clock, due_before=date(2026, 10, 6))
    executor = build_executor(connection, clock, lease_seconds=60)
    assert executor.run_next("night-worker")["outcome"] == "completed"
    gateway = RecordingGateway()
    outbox = OutboxService(connection, clock, lease_seconds=60)
    cursor = 0
    batches: list[int] = []
    while True:
        result = outbox.publish_batch("publisher", gateway, limit=2, after_id=cursor)
        batches.append(result["claimed"])
        if result["claimed"] == 0:
            break
        cursor = result["next_cursor"]
    assert batches == [2, 2, 1, 0]
    assert len(gateway.sent) == 5
    assert len(outbox.list_events(status="published")) == 5


def test_event_failure_backoff_then_manual_replay(client):
    """事件投递失败按注入时钟退避，上限后人工处理，重放不改变业务状态。"""
    clock = FrozenClock(T0)
    connection = get_connection()
    with transaction(immediate=True):
        lot, schedule_id = make_pending_schedule(connection, clock, "106")
        outbox = OutboxService(connection, clock, lease_seconds=60)
        event = outbox.enqueue_event(
            "retest-reminder:manual-1", "viability.retest_reminder", "seed_lot", lot["id"],
            {"schedule_id": schedule_id, "lot_no": lot["lot_no"]}, max_attempts=2,
        )

    class FailingGateway:
        def send(self, **kwargs):
            raise RuntimeError("通知网关不可用")

    outbox = OutboxService(connection, clock, lease_seconds=60)
    first = outbox.publish_batch("publisher", FailingGateway())
    assert first["failed"][0]["status"] == "pending"
    row = outbox.require_event(event["id"])
    assert row["attempts"] == 1
    assert row["available_at"] == to_storage(T0 + timedelta(seconds=30))
    clock.advance(seconds=31)
    second = outbox.publish_batch("publisher", FailingGateway())
    assert second["failed"][0]["status"] == "failed"  # 达到上限，进入人工处理
    # 重放前业务状态快照
    before_schedules = connection.execute("SELECT * FROM retest_schedules ORDER BY id").fetchall()
    before_lots = connection.execute("SELECT * FROM seed_lots ORDER BY id").fetchall()
    gateway = RecordingGateway()
    with transaction(immediate=True):
        detail = outbox.replay(event["id"], gateway, actor="值班管理员")
    assert detail["status"] == "published" and detail["final_result"] == "delivered"
    assert [item["outcome"] for item in detail["deliveries"]] == ["failed", "failed", "replayed"]
    assert gateway.sent[0]["delivery_key"] == "retest-reminder:manual-1:replay:1"
    # 业务状态未被重放改变
    assert connection.execute("SELECT * FROM retest_schedules ORDER BY id").fetchall() == before_schedules
    assert connection.execute("SELECT * FROM seed_lots ORDER BY id").fetchall() == before_lots


def test_job_failure_backoff_and_manual_requeue(client):
    """作业失败按注入时钟退避，上限后进入人工处理，重新排队后可完成。"""
    clock = FrozenClock(T0)
    connection = get_connection()
    executor = build_executor(connection, clock, lease_seconds=60)

    def always_fails(conn, payload, context):
        raise RuntimeError("磁盘已满")

    executor.register("nightly.export", always_fails)
    with transaction(immediate=True):
        job = JobService(connection, clock).enqueue(
            "nightly.export", "export:2026-10-01", {"date": "2026-10-01"}, max_attempts=2
        )
    first = executor.run_next("worker")
    assert first["outcome"] == "retry"
    assert first["available_at"] == to_storage(T0 + timedelta(seconds=30))
    clock.advance(seconds=31)
    second = executor.run_next("worker")
    assert second["outcome"] == "failed"
    detail = JobService(connection, clock).detail(job["id"])
    assert detail["status"] == "failed"
    assert [item["outcome"] for item in detail["attempts_log"]] == ["retry", "failed"]
    # 人工处理：重新排队，修复后执行成功
    with transaction(immediate=True):
        JobService(connection, clock).requeue(job["id"])
    executor.register("nightly.export", lambda conn, payload, context: {"exported": 3})
    third = executor.run_next("worker")
    assert third["outcome"] == "completed" and third["result"] == {"exported": 3}
    final = JobService(connection, clock).detail(job["id"])
    assert [item["outcome"] for item in final["attempts_log"]] == ["retry", "failed", "completed"]
