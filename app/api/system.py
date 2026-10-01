from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from app.api.dependencies import current_principal
from app.core.security import Principal
from app.database import get_connection, transaction
from app.germplasm.jobs import build_executor, enqueue_retest_reminders
from app.services.jobs import JobService
from app.services.outbox import OutboxService

router = APIRouter(prefix="/api/system", tags=["系统运维"])


class ReminderEnqueueRequest(BaseModel):
    due_before: date
    limit: int = Field(default=500, ge=1, le=5000)


class JobRunRequest(BaseModel):
    worker: str = Field(default="api-worker", min_length=1, max_length=64)
    limit: int = Field(default=10, ge=1, le=100)
    lease_seconds: int = Field(default=60, ge=5, le=3600)


class OutboxPublishRequest(BaseModel):
    publisher: str = Field(default="api-publisher", min_length=1, max_length=64)
    limit: int = Field(default=100, ge=1, le=500)
    cursor: int = Field(default=0, ge=0)


class OutboxReplayRequest(BaseModel):
    actor: str = Field(default="管理员", min_length=1, max_length=64)


@router.get("/health")
def health() -> dict:
    connection = get_connection()
    foreign_keys = int(connection.execute("PRAGMA foreign_keys").fetchone()[0])
    journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
    return {"status": "ok", "foreign_keys": foreign_keys, "journal_mode": journal_mode}


@router.post("/jobs/example", status_code=201)
def enqueue_example(principal: Principal = Depends(current_principal)) -> dict:
    principal.require("jobs.run")
    with transaction(immediate=True) as connection:
        return JobService(connection).enqueue("system.example", f"example:{principal.user_id}", {"actor": principal.user_id})


@router.get("/jobs")
def list_jobs(
    status: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("jobs.read")
    return {"jobs": JobService(get_connection()).list_jobs(status=status, limit=limit)}


@router.post("/jobs/retest-reminders", status_code=201)
def enqueue_retest_reminder_job(
    data: ReminderEnqueueRequest, principal: Principal = Depends(current_principal)
) -> dict:
    principal.require("jobs.run")
    with transaction(immediate=True) as connection:
        return enqueue_retest_reminders(connection, due_before=data.due_before, limit=data.limit)


@router.post("/jobs/run")
def run_jobs(data: JobRunRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("jobs.run")
    executor = build_executor(get_connection(), lease_seconds=data.lease_seconds)
    return {"worker": data.worker, "outcomes": executor.run(data.worker, limit=data.limit)}


@router.get("/jobs/{job_id}")
def job_detail(job_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("jobs.read")
    connection = get_connection()
    job = JobService(connection).detail(job_id)
    job["related_lots"] = _related_lots(connection, job)
    return job


@router.post("/jobs/{job_id}/requeue")
def requeue_job(job_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("jobs.run")
    with transaction(immediate=True) as connection:
        return JobService(connection).requeue(job_id)


@router.get("/outbox")
def list_outbox_events(
    status: str | None = None,
    after_id: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("outbox.read")
    events = OutboxService(get_connection()).list_events(status=status, limit=limit, after_id=after_id)
    return {"events": events}


@router.post("/outbox/publish")
def publish_outbox(data: OutboxPublishRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("outbox.publish")
    return OutboxService(get_connection()).publish_batch(
        data.publisher, limit=data.limit, after_id=data.cursor
    )


@router.get("/outbox/{event_id}")
def outbox_detail(event_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("outbox.read")
    return OutboxService(get_connection()).detail(event_id)


@router.post("/outbox/{event_id}/replay")
def replay_outbox_event(
    event_id: int, data: OutboxReplayRequest, principal: Principal = Depends(current_principal)
) -> dict:
    principal.require("outbox.replay")
    with transaction(immediate=True) as connection:
        return OutboxService(connection).replay(event_id, actor=data.actor)


def _related_lots(connection, job: dict) -> list[dict]:
    """从作业负载与结果中提取关联的种子批次，便于运维核对。"""
    lot_ids: set[int] = set()
    payload = job.get("payload") or {}
    if payload.get("lot_id"):
        lot_ids.add(int(payload["lot_id"]))
    result = job.get("result") or {}
    schedule_ids = [int(item) for item in result.get("marked_schedule_ids") or []]
    if schedule_ids:
        placeholders = ",".join("?" for _ in schedule_ids)
        rows = connection.execute(
            f"SELECT DISTINCT lot_id FROM retest_schedules WHERE id IN ({placeholders})", schedule_ids
        ).fetchall()
        lot_ids.update(int(row[0]) for row in rows)
    event_keys = [str(item) for item in result.get("event_keys") or []]
    if event_keys:
        placeholders = ",".join("?" for _ in event_keys)
        rows = connection.execute(
            f"SELECT DISTINCT aggregate_id FROM outbox_events WHERE aggregate_type='seed_lot' "
            f"AND event_key IN ({placeholders})",
            event_keys,
        ).fetchall()
        lot_ids.update(int(row[0]) for row in rows)
    if not lot_ids:
        return []
    placeholders = ",".join("?" for _ in lot_ids)
    rows = connection.execute(
        f"SELECT l.id,l.lot_no,l.status,a.accession_no,a.crop_name FROM seed_lots l "
        f"JOIN accessions a ON a.id=l.accession_id WHERE l.id IN ({placeholders}) ORDER BY l.id",
        sorted(lot_ids),
    ).fetchall()
    return [dict(row) for row in rows]
