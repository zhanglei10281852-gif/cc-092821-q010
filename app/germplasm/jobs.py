from __future__ import annotations

import sqlite3
from datetime import date

from app.core.clock import Clock
from app.germplasm.viability import ViabilityService
from app.services.jobs import JobContext, JobExecutor, JobService

RETEST_REMINDERS_JOB = "viability.retest_reminders"


def enqueue_retest_reminders(
    connection: sqlite3.Connection,
    clock: Clock | None = None,
    *,
    due_before: date,
    limit: int = 500,
) -> dict:
    """登记夜间复检提醒作业；同一截止日期重复登记会命中去重键返回既有作业。"""
    service = JobService(connection, clock)
    return service.enqueue(
        RETEST_REMINDERS_JOB,
        f"retest-reminders:{due_before.isoformat()}",
        {"due_before": due_before.isoformat(), "limit": limit},
    )


def _handle_retest_reminders(connection: sqlite3.Connection, payload: dict, context: JobContext) -> dict:
    service = ViabilityService(connection, context.clock)
    return service.generate_retest_reminders(
        date.fromisoformat(payload["due_before"]), limit=int(payload.get("limit", 500))
    )


def build_executor(
    connection: sqlite3.Connection,
    clock: Clock | None = None,
    *,
    lease_seconds: int = 60,
    backoff_base_seconds: int = 30,
    backoff_cap_seconds: int = 1800,
) -> JobExecutor:
    executor = JobExecutor(
        connection,
        clock,
        lease_seconds=lease_seconds,
        backoff_base_seconds=backoff_base_seconds,
        backoff_cap_seconds=backoff_cap_seconds,
    )
    executor.register(RETEST_REMINDERS_JOB, _handle_retest_reminders)
    return executor
