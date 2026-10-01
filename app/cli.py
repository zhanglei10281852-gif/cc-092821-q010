from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import date
from pathlib import Path

from fastapi.testclient import TestClient

from app.core.errors import DomainError
from app.database import database_path, get_connection, init_db, transaction
from app.germplasm.service import GermplasmService
from app.services.jobs import JobRunner, JobService
from app.services.outbox import OutboxService
from app.services.reminders import default_handlers, enqueue_retest_scan


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
        "retest_schedules", "quality_alerts", "outbox_events", "background_jobs",
        "job_attempts", "outbox_attempts", "notification_messages", "publisher_checkpoints",
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


def enqueue_reminder_scan_command(on_date: str | None) -> dict:
    init_db()
    target = date.fromisoformat(on_date) if on_date else date.today()
    with transaction(immediate=True):
        job = enqueue_retest_scan(get_connection(), target)
    return {"job": job}


def jobs_run_command(worker: str, limit: int, lease_seconds: int) -> dict:
    init_db()
    runner = JobRunner(get_connection(), handlers=default_handlers(), lease_seconds=lease_seconds)
    outcomes = runner.tick(worker, limit=limit)
    return {"worker": worker, "processed": len(outcomes), "outcomes": outcomes}


def jobs_list_command(status: str | None) -> dict:
    init_db()
    items, total = JobService(get_connection()).list_jobs(status=status, limit=200)
    return {"items": items, "total": total}


def job_show_command(job_id: int) -> dict:
    init_db()
    return JobService(get_connection()).detail(job_id)


def outbox_publish_command(publisher: str, limit: int, lease_seconds: int) -> dict:
    init_db()
    return OutboxService(get_connection()).publish_batch(publisher, limit=limit, lease_seconds=lease_seconds)


def outbox_list_command(status: str | None) -> dict:
    init_db()
    service = OutboxService(get_connection())
    items, total = service.list_events(status=status, limit=200)
    return {"items": items, "total": total, "cursor": service.cursor()}


def outbox_show_command(event_id: int) -> dict:
    init_db()
    return OutboxService(get_connection()).detail(event_id)


def outbox_replay_command(event_id: int, actor: str) -> dict:
    init_db()
    with transaction(immediate=True):
        return OutboxService(get_connection()).replay_event(event_id, actor)


def notifications_list_command(recipient: str | None) -> dict:
    init_db()
    items, total = OutboxService(get_connection()).list_notifications(recipient=recipient, limit=200)
    return {"items": items, "total": total}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="种质资源库运维命令")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init-db", help="初始化数据库")
    subparsers.add_parser("check-db", help="检查数据库完整性")
    subparsers.add_parser("smoke", help="执行 HTTP 冒烟检查")
    subparsers.add_parser("demo", help="写入一组示范入库数据")
    export = subparsers.add_parser("export-accessions", help="导出资源档案")
    export.add_argument("path")
    scan = subparsers.add_parser("enqueue-reminder-scan", help="登记指定日期的复检提醒扫描作业（按日期去重）")
    scan.add_argument("--date", default=None, help="扫描日期 YYYY-MM-DD，默认今天")
    jobs_run = subparsers.add_parser("jobs-run", help="领取并执行到期的后台作业")
    jobs_run.add_argument("--worker", default="cli-worker", help="执行者标识")
    jobs_run.add_argument("--limit", type=int, default=10, help="本轮最多执行的作业数")
    jobs_run.add_argument("--lease-seconds", type=int, default=60, help="租约时长（秒）")
    jobs_list = subparsers.add_parser("jobs-list", help="列出后台作业及其租约状态")
    jobs_list.add_argument("--status", default=None, help="按状态过滤")
    job_show = subparsers.add_parser("job-show", help="查看作业详情、每次尝试与最终结果")
    job_show.add_argument("job_id", type=int)
    outbox_publish = subparsers.add_parser("outbox-publish", help="按游标发布一批待投递事件")
    outbox_publish.add_argument("--publisher", default="cli-publisher", help="发布者标识")
    outbox_publish.add_argument("--limit", type=int, default=50, help="本批最多投递的事件数")
    outbox_publish.add_argument("--lease-seconds", type=int, default=120, help="租约时长（秒）")
    outbox_list = subparsers.add_parser("outbox-list", help="列出事件箱事件、关联批次与投递结果")
    outbox_list.add_argument("--status", default=None, help="按状态过滤")
    outbox_show = subparsers.add_parser("outbox-show", help="查看事件详情与每次投递尝试")
    outbox_show.add_argument("event_id", type=int)
    outbox_replay = subparsers.add_parser("outbox-replay", help="重放单个事件（不改变业务状态）")
    outbox_replay.add_argument("event_id", type=int)
    outbox_replay.add_argument("--actor", default="cli-admin", help="操作人标识")
    notifications = subparsers.add_parser("notifications-list", help="列出待办通知")
    notifications.add_argument("--recipient", default=None, help="按接收人过滤")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    handlers = {
        "init-db": lambda: init_command(),
        "check-db": lambda: check_command(),
        "smoke": lambda: smoke_command(),
        "demo": lambda: demo_command(),
        "export-accessions": lambda: export_command(args.path),
        "enqueue-reminder-scan": lambda: enqueue_reminder_scan_command(args.date),
        "jobs-run": lambda: jobs_run_command(args.worker, args.limit, args.lease_seconds),
        "jobs-list": lambda: jobs_list_command(args.status),
        "job-show": lambda: job_show_command(args.job_id),
        "outbox-publish": lambda: outbox_publish_command(args.publisher, args.limit, args.lease_seconds),
        "outbox-list": lambda: outbox_list_command(args.status),
        "outbox-show": lambda: outbox_show_command(args.event_id),
        "outbox-replay": lambda: outbox_replay_command(args.event_id, args.actor),
        "notifications-list": lambda: notifications_list_command(args.recipient),
    }
    try:
        result = handlers[args.command]()
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (sqlite3.Error, RuntimeError, ValueError, DomainError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
