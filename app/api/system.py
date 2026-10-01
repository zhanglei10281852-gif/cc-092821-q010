from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from app.api.dependencies import current_principal
from app.core.security import Principal
from app.database import get_connection, transaction
from app.services.jobs import JobRunner, JobService
from app.services.outbox import OutboxService
from app.services.reminders import default_handlers

router = APIRouter(prefix="/api/system", tags=["系统运维"])


class JobRunRequest(BaseModel):
    limit: int = Field(default=10, ge=1, le=100)
    lease_seconds: int = Field(default=60, ge=5, le=3600)


class OutboxPublishRequest(BaseModel):
    limit: int = Field(default=50, ge=1, le=500)
    lease_seconds: int = Field(default=120, ge=5, le=3600)


@router.get("/health")
def health() -> dict:
    connection = get_connection()
    foreign_keys = int(connection.execute("PRAGMA foreign_keys").fetchone()[0])
    journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
    return {"status": "ok", "foreign_keys": foreign_keys, "journal_mode": journal_mode}


@router.get("/jobs")
def list_jobs(
    status: str | None = None,
    job_type: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("jobs.read")
    items, total = JobService(get_connection()).list_jobs(status=status, job_type=job_type, limit=limit, offset=offset)
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@router.post("/jobs/run")
def run_jobs(data: JobRunRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("jobs.run")
    runner = JobRunner(get_connection(), handlers=default_handlers(), lease_seconds=data.lease_seconds)
    outcomes = runner.tick(f"api:{principal.username}", limit=data.limit)
    return {"processed": len(outcomes), "outcomes": outcomes}


@router.get("/jobs/{job_id}")
def job_detail(job_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("jobs.read")
    return JobService(get_connection()).detail(job_id)


@router.post("/jobs/{job_id}/requeue")
def requeue_job(job_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("jobs.run")
    with transaction(immediate=True) as connection:
        return JobService(connection).requeue(job_id)


@router.post("/jobs/example", status_code=201)
def enqueue_example(principal: Principal = Depends(current_principal)) -> dict:
    principal.require("jobs.run")
    with transaction(immediate=True) as connection:
        return JobService(connection).enqueue("system.example", f"example:{principal.user_id}", {"actor": principal.user_id})


@router.get("/outbox")
def list_outbox(
    status: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("jobs.read")
    service = OutboxService(get_connection())
    items, total = service.list_events(status=status, limit=limit, offset=offset)
    return {"items": items, "total": total, "cursor": service.cursor(), "limit": limit, "offset": offset}


@router.post("/outbox/publish")
def publish_outbox(data: OutboxPublishRequest, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("jobs.run")
    return OutboxService(get_connection()).publish_batch(
        f"api:{principal.username}", limit=data.limit, lease_seconds=data.lease_seconds
    )


@router.get("/outbox/{event_id}")
def outbox_detail(event_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("jobs.read")
    return OutboxService(get_connection()).detail(event_id)


@router.post("/outbox/{event_id}/replay")
def replay_event(event_id: int, principal: Principal = Depends(current_principal)) -> dict:
    principal.require("jobs.run")
    with transaction(immediate=True) as connection:
        return OutboxService(connection).replay_event(event_id, principal.username)


@router.get("/notifications")
def list_notifications(
    recipient: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(current_principal),
) -> dict:
    principal.require("jobs.read")
    items, total = OutboxService(get_connection()).list_notifications(recipient=recipient, limit=limit, offset=offset)
    return {"items": items, "total": total, "limit": limit, "offset": offset}
