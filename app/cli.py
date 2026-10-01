from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import date
from pathlib import Path

from fastapi.testclient import TestClient

from app.core.clock import FrozenClock, from_storage
from app.core.errors import DomainError
from app.database import database_path, get_connection, init_db, transaction
from app.germplasm.jobs import build_executor, enqueue_retest_reminders
from app.germplasm.service import GermplasmService
from app.services.jobs import JobService
from app.services.outbox import OutboxService


def init_command() -> dict:
    init_db()
    return {"database": str(database_path()), "initialized": True}


def check_command() -> dict:
    init_db()
    connection = get_connection()
    integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
    foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
    required = {
        "accessions", "seed_lots", "storage_locations", "viability_tests",
        "retest_schedules", "quality_alerts", "outbox_events",
        "background_jobs", "job_attempts", "outbox_deliveries",
    }
    actual = {
        row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    return {
        "database": str(database_path()),
        "integrity": integrity,
        "foreign_key_errors": len(foreign_keys),
        "required_tables_present": required.issubset(actual),
        "table_count": len(actual),
    }


def smoke_command() -> dict:
    from app.main import app

    init_db()
    with TestClient(app) as client:
        root = client.get("/")
        health = client.get("/api/system/health")
    if root.status_code != 200 or health.status_code != 200:
        raise RuntimeError("HTTP 冒烟检查失败")
    return {"root": root.json(), "health": health.json(), "status": "ok"}


def demo_command() -> dict:
    init_db()
    service = GermplasmService(get_connection())
    suffix = get_connection().execute("SELECT COUNT(*) FROM accessions").fetchone()[0] + 1
    with transaction(immediate=True):
        source = service.accessions.create_source({
            "source_code": f"DEMO-{suffix:04d}",
            "provider_name": "示范采集队",
            "country_code": "CN",
            "locality": "示范农场",
            "collected_on": "2026-09-01",
            "permit_reference": None,
            "restrictions": {},
        })
        accession = service.accessions.create_accession({
            "accession_no": f"ACC-DEMO-{suffix:04d}",
            "scientific_name": "Oryza sativa",
            "crop_name": "水稻",
            "cultivar_name": "示范材料",
            "source_id": source["id"],
            "acquisition_type": "采集",
            "received_on": "2026-09-20",
            "passport": {"origin": "示范农场"},
            "created_by": "cli",
        })
        accepted = service.accessions.transition(accession["id"], {
            "target_status": "accepted", "reason": "资料齐全", "expected_version": 1, "actor": "cli"
        })
        location = service.inventory.create_location({
            "location_code": f"DEMO-COLD-{suffix:04d}", "facility": "示范库", "room": "低温间",
            "rack": "R1", "shelf": "S1", "capacity_grams": 5000,
            "temperature_c": -18, "humidity_percent": 30,
        })
        lot = service.inventory.create_lot({
            "lot_no": f"LOT-DEMO-{suffix:04d}", "accession_id": accepted["id"], "parent_lot_id": None,
            "harvest_year": 2026, "initial_weight_grams": 500, "moisture_percent": 7.2,
            "treatment": "清选干燥", "sealed_on": "2026-09-21", "created_by": "cli",
        })
        placement = service.inventory.place_lot({
            "lot_id": lot["id"], "location_id": location["id"], "weight_grams": 500,
            "container_code": f"BOX-DEMO-{suffix:04d}", "idempotency_key": f"demo-place-{suffix:08d}", "actor": "cli",
        })
    return {
        "accession_no": accepted["accession_no"],
        "lot_no": lot["lot_no"],
        "location": location["location_code"],
        "placement_id": placement["placement"]["id"],
        "dashboard": service.dashboard(),
    }


def export_command(path: str) -> dict:
    init_db()
    service = GermplasmService(get_connection())
    items, total = service.repository.list_accessions(status=None, crop=None, limit=10_000, offset=0)
    target = Path(path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"path": str(target), "count": total}


def _clock_from(now: str | None) -> FrozenClock | None:
    return FrozenClock(from_storage(now)) if now else None


def reminders_enqueue_command(due_before: str, limit: int) -> dict:
    init_db()
    with transaction(immediate=True):
        job = enqueue_retest_reminders(
            get_connection(), due_before=date.fromisoformat(due_before), limit=limit
        )
    return {"job": job}


def jobs_run_command(worker: str, limit: int, lease_seconds: int, now: str | None) -> dict:
    init_db()
    executor = build_executor(get_connection(), _clock_from(now), lease_seconds=lease_seconds)
    return {"worker": worker, "outcomes": executor.run(worker, limit=limit)}


def jobs_list_command(status: str | None, limit: int) -> dict:
    init_db()
    return {"jobs": JobService(get_connection()).list_jobs(status=status, limit=limit)}


def job_show_command(job_id: int) -> dict:
    init_db()
    return JobService(get_connection()).detail(job_id)


def job_requeue_command(job_id: int) -> dict:
    init_db()
    with transaction(immediate=True):
        job = JobService(get_connection()).requeue(job_id)
    return {"job": job}


def outbox_publish_command(publisher: str, limit: int, cursor: int, now: str | None) -> dict:
    init_db()
    service = OutboxService(get_connection(), _clock_from(now))
    return service.publish_batch(publisher, limit=limit, after_id=cursor)


def outbox_list_command(status: str | None, limit: int) -> dict:
    init_db()
    return {"events": OutboxService(get_connection()).list_events(status=status, limit=limit)}


def outbox_show_command(event_id: int) -> dict:
    init_db()
    return OutboxService(get_connection()).detail(event_id)


def outbox_replay_command(event_id: int, actor: str) -> dict:
    init_db()
    with transaction(immediate=True):
        detail = OutboxService(get_connection()).replay(event_id, actor=actor)
    return {"event": detail}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="种质资源库运维命令")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init-db", help="初始化数据库")
    subparsers.add_parser("check-db", help="检查数据库完整性")
    subparsers.add_parser("smoke", help="执行 HTTP 冒烟检查")
    subparsers.add_parser("demo", help="写入一组示范入库数据")
    export = subparsers.add_parser("export-accessions", help="导出资源档案")
    export.add_argument("path")

    reminders = subparsers.add_parser("reminders-enqueue", help="登记夜间复检提醒作业（按截止日期去重）")
    reminders.add_argument("--due-before", required=True, help="截止日期 YYYY-MM-DD")
    reminders.add_argument("--limit", type=int, default=500)

    jobs_run = subparsers.add_parser("jobs-run", help="按租约执行待处理后台作业")
    jobs_run.add_argument("--worker", default="cli-worker")
    jobs_run.add_argument("--limit", type=int, default=10)
    jobs_run.add_argument("--lease-seconds", type=int, default=60)
    jobs_run.add_argument("--now", default=None, help="注入当前时间（ISO 格式），用于确定性演练")

    jobs_list = subparsers.add_parser("jobs-list", help="列出后台作业")
    jobs_list.add_argument("--status", default=None)
    jobs_list.add_argument("--limit", type=int, default=50)

    job_show = subparsers.add_parser("job-show", help="查看作业详情与每次尝试")
    job_show.add_argument("job_id", type=int)

    job_requeue = subparsers.add_parser("job-requeue", help="人工处理：把失败作业重新排队")
    job_requeue.add_argument("job_id", type=int)

    outbox_publish = subparsers.add_parser("outbox-publish", help="按游标分批投递事件箱事件")
    outbox_publish.add_argument("--publisher", default="cli-publisher")
    outbox_publish.add_argument("--limit", type=int, default=100)
    outbox_publish.add_argument("--cursor", type=int, default=0)
    outbox_publish.add_argument("--now", default=None, help="注入当前时间（ISO 格式），用于确定性演练")

    outbox_list = subparsers.add_parser("outbox-list", help="列出事件箱事件")
    outbox_list.add_argument("--status", default=None)
    outbox_list.add_argument("--limit", type=int, default=50)

    outbox_show = subparsers.add_parser("outbox-show", help="查看事件详情、投递记录与关联批次")
    outbox_show.add_argument("event_id", type=int)

    outbox_replay = subparsers.add_parser("outbox-replay", help="人工重放单个事件（不改变业务状态）")
    outbox_replay.add_argument("event_id", type=int)
    outbox_replay.add_argument("--actor", default="运维")
    return parser


def dispatch(args: argparse.Namespace) -> dict:
    if args.command == "init-db":
        return init_command()
    if args.command == "check-db":
        return check_command()
    if args.command == "smoke":
        return smoke_command()
    if args.command == "demo":
        return demo_command()
    if args.command == "export-accessions":
        return export_command(args.path)
    if args.command == "reminders-enqueue":
        return reminders_enqueue_command(args.due_before, args.limit)
    if args.command == "jobs-run":
        return jobs_run_command(args.worker, args.limit, args.lease_seconds, args.now)
    if args.command == "jobs-list":
        return jobs_list_command(args.status, args.limit)
    if args.command == "job-show":
        return job_show_command(args.job_id)
    if args.command == "job-requeue":
        return job_requeue_command(args.job_id)
    if args.command == "outbox-publish":
        return outbox_publish_command(args.publisher, args.limit, args.cursor, args.now)
    if args.command == "outbox-list":
        return outbox_list_command(args.status, args.limit)
    if args.command == "outbox-show":
        return outbox_show_command(args.event_id)
    if args.command == "outbox-replay":
        return outbox_replay_command(args.event_id, args.actor)
    raise ValueError(f"未知命令：{args.command}")


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = dispatch(args)
    except (sqlite3.Error, DomainError, RuntimeError, ValueError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
