from __future__ import annotations

import sqlite3
from datetime import date
from typing import Any

from app.core.clock import Clock
from app.germplasm.viability import ViabilityService
from app.services.jobs import JobHandler, JobService
from app.services.outbox import OutboxService

RETEST_SCAN_JOB_TYPE = "retest.reminder.scan"


def enqueue_retest_scan(connection: sqlite3.Connection, on_date: date, clock: Clock | None = None) -> dict:
    """为指定日期登记夜间复检提醒扫描作业，按日期去重，重复登记返回同一作业。"""
    jobs = JobService(connection, clock)
    return jobs.enqueue(
        RETEST_SCAN_JOB_TYPE,
        f"retest-reminder-scan:{on_date.isoformat()}",
        {"on_date": on_date.isoformat()},
    )


def handle_retest_reminder_scan(
    connection: sqlite3.Connection,
    payload: dict[str, Any],
    clock: Clock,
) -> dict[str, Any]:
    """把到期复检日程标记为已提醒并写入事件箱。

    日程变更与 outbox 写入由调用方放在同一事务提交；处理器可安全重跑：
    已提醒的日程不会重复标记，事件按 ``retest-reminder:{schedule_id}`` 去重，
    历史遗留的“已提醒但没有事件”的日程会在这里补齐事件。
    """
    viability = ViabilityService(connection, clock)
    outbox = OutboxService(connection, clock)
    on_date = date.fromisoformat(str(payload["on_date"]))
    due = viability.due_schedules(on_date, limit=500)
    pending_ids = [int(row["id"]) for row in due if row["status"] == "pending"]
    newly_notified = viability.mark_notifications(pending_ids)
    event_ids: list[int] = []
    created = 0
    for row in due:
        event, was_created = outbox.emit(
            f"retest-reminder:{row['id']}",
            "retest.reminder",
            "retest_schedule",
            row["id"],
            {
                "schedule_id": row["id"],
                "lot_id": row["lot_id"],
                "lot_no": row["lot_no"],
                "accession_no": row["accession_no"],
                "crop_name": row["crop_name"],
                "due_on": row["due_on"],
                "recipient": "technician",
                "title": f"批次 {row['lot_no']} 活力复检提醒",
                "message": f"批次 {row['lot_no']}（{row['crop_name']}）应于 {row['due_on']} 前完成活力复检",
            },
        )
        event_ids.append(int(event["id"]))
        created += 1 if was_created else 0
    return {
        "on_date": on_date.isoformat(),
        "schedules_due": len(due),
        "newly_notified": newly_notified,
        "events_created": created,
        "events_reused": len(due) - created,
        "schedule_ids": [int(row["id"]) for row in due],
        "lot_ids": sorted({int(row["lot_id"]) for row in due}),
        "lot_nos": sorted({str(row["lot_no"]) for row in due}),
        "event_ids": event_ids,
    }


def default_handlers() -> dict[str, JobHandler]:
    return {RETEST_SCAN_JOB_TYPE: handle_retest_reminder_scan}
