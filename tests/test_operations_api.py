from __future__ import annotations

from app.core.clock import FrozenClock
from app.database import get_connection, transaction
from app.services.notifications import default_gateway
from conftest import T0, make_pending_schedule


def test_jobs_and_outbox_operations_api(client, admin):
    """运维 API：登记、执行、查看尝试与租约所有者、投递、重放、关联批次。"""
    default_gateway().reset()
    headers = admin["headers"]
    clock = FrozenClock(T0)
    with transaction(immediate=True):
        lot, schedule_id = make_pending_schedule(get_connection(), clock, "901")

    # 登记夜间复检提醒作业，重复登记命中去重键
    enqueued = client.post("/api/system/jobs/retest-reminders", headers=headers, json={"due_before": "2026-10-06"})
    assert enqueued.status_code == 201, enqueued.text
    job_id = enqueued.json()["id"]
    again = client.post("/api/system/jobs/retest-reminders", headers=headers, json={"due_before": "2026-10-06"})
    assert again.status_code == 201 and again.json()["id"] == job_id

    # 执行作业：日程变更与事件写入在同一事务落地
    ran = client.post("/api/system/jobs/run", headers=headers, json={"worker": "night-worker"})
    assert ran.status_code == 200, ran.text
    outcomes = ran.json()["outcomes"]
    assert outcomes[0]["outcome"] == "completed"
    assert outcomes[0]["result"]["marked_schedule_ids"] == [schedule_id]

    # 作业详情：每次尝试、租约所有者、关联批次
    detail = client.get(f"/api/system/jobs/{job_id}", headers=headers)
    assert detail.status_code == 200, detail.text
    body = detail.json()
    assert body["status"] == "completed"
    assert body["attempts_log"][0]["worker"] == "night-worker"
    assert body["attempts_log"][0]["outcome"] == "completed"
    assert body["related_lots"][0]["lot_no"] == lot["lot_no"]

    # 按游标分批投递
    published = client.post("/api/system/outbox/publish", headers=headers, json={"publisher": "pub-1"})
    assert published.status_code == 200, published.text
    payload = published.json()
    assert len(payload["delivered"]) == 1
    event_id = payload["delivered"][0]["event_id"]
    assert payload["next_cursor"] == event_id

    # 事件详情：投递记录、关联批次、最终投递结果
    event = client.get(f"/api/system/outbox/{event_id}", headers=headers)
    assert event.status_code == 200, event.text
    event_body = event.json()
    assert event_body["final_result"] == "delivered"
    assert event_body["related_lot"]["lot_no"] == lot["lot_no"]
    assert event_body["deliveries"][0]["outcome"] == "delivered"
    assert event_body["deliveries"][0]["publisher"] == "pub-1"

    # 管理员重放：追加投递记录，业务状态不变
    replayed = client.post(f"/api/system/outbox/{event_id}/replay", headers=headers, json={"actor": "值班管理员"})
    assert replayed.status_code == 200, replayed.text
    outcomes = [item["outcome"] for item in replayed.json()["deliveries"]]
    assert outcomes == ["delivered", "replayed"]
    schedule = get_connection().execute(
        "SELECT status FROM retest_schedules WHERE id=?", (schedule_id,)
    ).fetchone()
    assert schedule[0] == "notified"

    # 列表端点
    jobs = client.get("/api/system/jobs", headers=headers)
    assert jobs.status_code == 200 and jobs.json()["jobs"]
    events = client.get("/api/system/outbox", headers=headers, params={"status": "published"})
    assert events.status_code == 200 and len(events.json()["events"]) == 1


def test_operations_api_requires_authentication(client):
    assert client.get("/api/system/jobs").status_code == 401
    assert client.get("/api/system/outbox").status_code == 401
    assert client.post("/api/system/jobs/run", json={"worker": "x"}).status_code == 401


def test_operations_api_requires_permission(client, admin):
    headers = admin["headers"]
    created = client.post(
        "/api/users",
        headers=headers,
        json={"username": "field.clerk", "password": "Clerk!23456", "display_name": "登记员", "role_codes": ["registrar"]},
    )
    assert created.status_code == 201, created.text
    login = client.post("/api/auth/login", json={"username": "field.clerk", "password": "Clerk!23456", "client_label": "tests"})
    clerk_headers = {"Authorization": f"Bearer {login.json()['token']}"}
    assert client.get("/api/system/jobs", headers=clerk_headers).status_code == 403
    assert client.get("/api/system/outbox", headers=clerk_headers).status_code == 403
    assert client.post("/api/system/jobs/run", headers=clerk_headers, json={"worker": "x"}).status_code == 403
