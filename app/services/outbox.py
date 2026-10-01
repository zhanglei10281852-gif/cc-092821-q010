from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import timedelta
from typing import Any

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError
from app.database import transaction
from app.services.notifications import NotificationGateway, default_gateway


def _decode_event(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    data["payload"] = json.loads(data.pop("payload_json") or "{}")
    return data


class OutboxService:
    """事件箱的写入与发布：游标分批领取、分批确认、失败退避与人工重放。

    业务事务把事件写入 outbox_events 后即算落地；发布端按 id 游标分批领取
    （签发租约）、逐条投递并记录 deliveries，最后在一个事务里按游标批量确认。
    确认前崩溃的事件租约到期后会被重新领取，网关按 event_key 幂等去重，
    因此通知不会重复。失败按注入时钟退避，达到 max_attempts 后进入 failed
    等待人工处理；管理员可重放单个事件，只追加投递记录，不改变业务状态。
    """

    def __init__(
        self,
        connection: sqlite3.Connection,
        clock: Clock | None = None,
        *,
        lease_seconds: int = 60,
        backoff_base_seconds: int = 30,
        backoff_cap_seconds: int = 1800,
    ) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        self.lease_seconds = lease_seconds
        self.backoff_base_seconds = backoff_base_seconds
        self.backoff_cap_seconds = backoff_cap_seconds

    def retry_delay(self, attempt_no: int) -> int:
        return min(self.backoff_cap_seconds, self.backoff_base_seconds * (2 ** max(0, attempt_no - 1)))

    def enqueue_event(
        self,
        event_key: str,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str | int,
        payload: dict,
        *,
        channel: str = "notification",
        max_attempts: int | None = None,
    ) -> dict:
        """在调用方的事务里写入事件；event_key 重复时返回既有事件（幂等）。"""
        now = to_storage(self.clock.now())
        try:
            cursor = self.connection.execute(
                "INSERT INTO outbox_events(event_key,event_type,aggregate_type,aggregate_id,payload_json,channel,"
                "status,available_at,max_attempts,created_at) VALUES(?,?,?,?,?,?,'pending',?,?,?)",
                (
                    event_key, event_type, aggregate_type, str(aggregate_id),
                    json.dumps(payload, ensure_ascii=False, sort_keys=True), channel, now,
                    max_attempts or 8, now,
                ),
            )
        except sqlite3.IntegrityError:
            existing = self.connection.execute(
                "SELECT * FROM outbox_events WHERE event_key=?", (event_key,)
            ).fetchone()
            if existing:
                return _decode_event(existing)
            raise
        return self.require_event(int(cursor.lastrowid))

    def require_event(self, event_id: int) -> dict:
        row = self.connection.execute("SELECT * FROM outbox_events WHERE id=?", (event_id,)).fetchone()
        if row is None:
            raise NotFoundError("事件箱事件不存在")
        return _decode_event(row)

    def claim_batch(self, publisher: str, *, limit: int = 100, after_id: int = 0) -> list[dict]:
        """按 id 游标领取一批待投递事件并签发租约；过期 processing 事件被回收。

        必须在事务中调用，保证整批领取原子完成。
        """
        now = self.clock.now()
        now_text = to_storage(now)
        stale_text = to_storage(now - timedelta(seconds=self.lease_seconds))
        expires_text = to_storage(now + timedelta(seconds=self.lease_seconds))
        rows = self.connection.execute(
            "SELECT id FROM outbox_events WHERE id>? AND ("
            "(status='pending' AND available_at<=?) OR "
            "(status='processing' AND ((lease_expires_at IS NOT NULL AND lease_expires_at<?) "
            "OR (lease_expires_at IS NULL AND locked_at<?)))) ORDER BY id LIMIT ?",
            (after_id, now_text, now_text, stale_text, limit),
        ).fetchall()
        claimed: list[dict] = []
        for row in rows:
            token = uuid.uuid4().hex
            cursor = self.connection.execute(
                "UPDATE outbox_events SET status='processing',locked_by=?,locked_at=?,lease_token=?,"
                "lease_expires_at=?,attempts=attempts+1 WHERE id=? AND ("
                "(status='pending' AND available_at<=?) OR "
                "(status='processing' AND ((lease_expires_at IS NOT NULL AND lease_expires_at<?) "
                "OR (lease_expires_at IS NULL AND locked_at<?))))",
                (publisher, now_text, token, expires_text, int(row["id"]), now_text, now_text, stale_text),
            )
            if cursor.rowcount != 1:
                continue
            event = self.require_event(int(row["id"]))
            event["lease_token"] = token
            claimed.append(event)
        return claimed

    def record_delivery(
        self,
        event: dict,
        *,
        publisher: str,
        delivery_key: str,
        outcome: str,
        message: str | None = None,
        replay_of: int | None = None,
    ) -> dict:
        """登记一次投递尝试；首次成功投递由唯一索引保证只出现一次。"""
        now = to_storage(self.clock.now())
        try:
            cursor = self.connection.execute(
                "INSERT INTO outbox_deliveries(event_id,event_key,delivery_key,channel,publisher,attempt_no,"
                "outcome,message,replay_of,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    int(event["id"]), event["event_key"], delivery_key,
                    event.get("channel") or "notification", publisher, int(event.get("attempts") or 0),
                    outcome, message, replay_of, now,
                ),
            )
        except sqlite3.IntegrityError:
            if outcome != "delivered" or replay_of is not None:
                raise
            # 唯一索引拦下了重复的首次投递记录，降级为 duplicate 留痕
            return self.record_delivery(
                event, publisher=publisher, delivery_key=delivery_key,
                outcome="duplicate", message="投递记录已存在，按重复处理",
            )
        row = self.connection.execute(
            "SELECT * FROM outbox_deliveries WHERE id=?", (int(cursor.lastrowid),)
        ).fetchone()
        return dict(row)

    def confirm_batch(self, publisher: str, confirmations: list[dict]) -> dict:
        """在一个事务里按租约令牌批量确认；令牌不匹配的迟到确认被丢弃。"""
        now = to_storage(self.clock.now())
        confirmed: list[int] = []
        rejected: list[int] = []
        for item in confirmations:
            cursor = self.connection.execute(
                "UPDATE outbox_events SET status='published',published_at=?,locked_by=NULL,locked_at=NULL,"
                "lease_token=NULL,lease_expires_at=NULL WHERE id=? AND status='processing' "
                "AND locked_by=? AND lease_token=?",
                (now, int(item["id"]), publisher, str(item["lease_token"])),
            )
            (confirmed if cursor.rowcount == 1 else rejected).append(int(item["id"]))
        return {"confirmed": confirmed, "rejected": rejected}

    def fail_event(self, event_id: int, publisher: str, lease_token: str, message: str) -> dict:
        event = self.require_event(event_id)
        now = self.clock.now()
        now_text = to_storage(now)
        attempts = int(event["attempts"])
        if attempts >= int(event["max_attempts"]):
            status, available_at = "failed", now_text
        else:
            status = "pending"
            available_at = to_storage(now + timedelta(seconds=self.retry_delay(attempts)))
        cursor = self.connection.execute(
            "UPDATE outbox_events SET status=?,last_error=?,available_at=?,locked_by=NULL,locked_at=NULL,"
            "lease_token=NULL,lease_expires_at=NULL WHERE id=? AND status='processing' "
            "AND locked_by=? AND lease_token=?",
            (status, message[:1000], available_at, event_id, publisher, lease_token),
        )
        if cursor.rowcount != 1:
            raise ConflictError("事件租约已失效或被其他发布者接管，迟到回执被拒绝")
        return self.require_event(event_id)

    def publish_batch(
        self,
        publisher: str,
        gateway: NotificationGateway | None = None,
        *,
        limit: int = 100,
        after_id: int = 0,
    ) -> dict:
        """领取一批事件、逐条投递并按游标批量确认，返回下一游标。

        依赖 app.database 的线程本地连接划分事务：领取一个事务、每条投递
        记录一个事务、批量确认一个事务。确认前崩溃时事件保持 processing，
        租约到期后被重新领取并由网关幂等去重。
        """
        gateway = gateway or default_gateway()
        with transaction(immediate=True):
            claimed = self.claim_batch(publisher, limit=limit, after_id=after_id)
        delivered: list[dict] = []
        duplicates: list[dict] = []
        failed: list[dict] = []
        confirmations: list[dict] = []
        for event in claimed:
            channel = event.get("channel") or "notification"
            try:
                outcome = gateway.send(
                    delivery_key=event["event_key"],
                    event_type=event["event_type"],
                    channel=channel,
                    payload=event["payload"],
                )
            except Exception as exc:  # noqa: BLE001 - 投递异常统一转入退避
                message = str(exc)[:500]
                with transaction(immediate=True):
                    self.record_delivery(
                        event, publisher=publisher, delivery_key=event["event_key"],
                        outcome="failed", message=message,
                    )
                    final = self.fail_event(int(event["id"]), publisher, str(event["lease_token"]), message)
                failed.append({
                    "event_id": event["id"], "event_key": event["event_key"],
                    "status": final["status"], "available_at": final["available_at"], "error": message,
                })
                continue
            with transaction(immediate=True):
                self.record_delivery(
                    event, publisher=publisher, delivery_key=event["event_key"], outcome=outcome
                )
            confirmations.append({"id": int(event["id"]), "lease_token": str(event["lease_token"])})
            entry = {"event_id": event["id"], "event_key": event["event_key"]}
            (duplicates if outcome == "duplicate" else delivered).append(entry)
        with transaction(immediate=True):
            result = self.confirm_batch(publisher, confirmations)
        next_cursor = max((int(event["id"]) for event in claimed), default=after_id)
        return {
            "claimed": len(claimed),
            "delivered": delivered,
            "duplicates": duplicates,
            "failed": failed,
            "confirmed": result["confirmed"],
            "rejected": result["rejected"],
            "next_cursor": next_cursor,
        }

    def replay(self, event_id: int, gateway: NotificationGateway | None = None, *, actor: str) -> dict:
        """人工重放单个事件：重新投递并留痕，不改变任何业务表状态。"""
        gateway = gateway or default_gateway()
        event = self.require_event(event_id)
        if event["status"] not in {"published", "failed"}:
            raise ConflictError("只有已投递或进入人工处理的事件可以重放")
        replay_no = int(self.connection.execute(
            "SELECT COUNT(*) FROM outbox_deliveries WHERE event_id=? AND replay_of IS NOT NULL", (event_id,)
        ).fetchone()[0]) + 1
        delivery_key = f"{event['event_key']}:replay:{replay_no}"
        gateway.send(
            delivery_key=delivery_key,
            event_type=event["event_type"],
            channel=event.get("channel") or "notification",
            payload=event["payload"],
        )
        original = self.connection.execute(
            "SELECT id FROM outbox_deliveries WHERE event_id=? AND replay_of IS NULL AND outcome='delivered' "
            "ORDER BY id LIMIT 1", (event_id,),
        ).fetchone()
        now = to_storage(self.clock.now())
        self.connection.execute(
            "INSERT INTO outbox_deliveries(event_id,event_key,delivery_key,channel,publisher,attempt_no,"
            "outcome,message,replay_of,created_at) VALUES(?,?,?,?,?,?,'replayed',?,?,?)",
            (
                event_id, event["event_key"], delivery_key, event.get("channel") or "notification",
                actor, int(event["attempts"]), f"管理员 {actor} 重放",
                int(original[0]) if original else None, now,
            ),
        )
        if event["status"] == "failed":
            self.connection.execute(
                "UPDATE outbox_events SET status='published',published_at=?,last_error=NULL WHERE id=?",
                (now, event_id),
            )
        return self.detail(event_id)

    def deliveries(self, event_id: int) -> list[dict]:
        rows = self.connection.execute(
            "SELECT * FROM outbox_deliveries WHERE event_id=? ORDER BY id", (event_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    def detail(self, event_id: int) -> dict:
        event = self.require_event(event_id)
        event["deliveries"] = self.deliveries(event_id)
        event["related_lot"] = self._related_lot(event)
        status = event["status"]
        event["final_result"] = {
            "published": "delivered",
            "failed": "awaiting_manual",
        }.get(status, "in_transit")
        return event

    def list_events(self, *, status: str | None = None, limit: int = 50, after_id: int = 0) -> list[dict]:
        if status:
            rows = self.connection.execute(
                "SELECT * FROM outbox_events WHERE status=? AND id>? ORDER BY id LIMIT ?",
                (status, after_id, limit),
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM outbox_events WHERE id>? ORDER BY id LIMIT ?", (after_id, limit)
            ).fetchall()
        return [_decode_event(row) for row in rows]

    def _related_lot(self, event: dict) -> dict | None:
        if event.get("aggregate_type") != "seed_lot":
            return None
        try:
            lot_id = int(event["aggregate_id"])
        except (TypeError, ValueError):
            return None
        row = self.connection.execute(
            "SELECT l.id,l.lot_no,l.status,a.accession_no,a.crop_name FROM seed_lots l "
            "JOIN accessions a ON a.id=l.accession_id WHERE l.id=?",
            (lot_id,),
        ).fetchone()
        return dict(row) if row else None
