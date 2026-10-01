from __future__ import annotations

import json
import secrets
import sqlite3
from datetime import timedelta
from typing import Any, Callable

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import transaction

DeliverySink = Callable[[dict[str, Any]], dict[str, Any]]

DEFAULT_CHANNEL = "notification"
DEFAULT_MAX_ATTEMPTS = 8


def notification_sink(
    connection: sqlite3.Connection,
    clock: Clock,
    channel: str = DEFAULT_CHANNEL,
) -> DeliverySink:
    """默认投递端：把事件写入待办信箱。

    信箱按 event_key 去重，重复投递只会命中已有记录，因此发布端至少投递一次
    也不会产生重复通知。
    """

    def sink(event: dict[str, Any]) -> dict[str, Any]:
        payload = event.get("payload")
        if payload is None:
            payload = json.loads(event.get("payload_json") or "{}")
        now_s = to_storage(clock.now())
        recipient = str(payload.get("recipient") or "operations")
        title = str(payload.get("title") or f"{event['event_type']}:{event['aggregate_id']}")
        message = str(payload.get("message") or title)
        cursor = connection.execute(
            "INSERT OR IGNORE INTO notification_messages(event_key,event_id,channel,recipient,title,message,"
            "payload_json,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                event["event_key"],
                event["id"],
                channel,
                recipient,
                title,
                message,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                now_s,
            ),
        )
        row = connection.execute(
            "SELECT id FROM notification_messages WHERE event_key=?", (event["event_key"],)
        ).fetchone()
        return {
            "notification_id": int(row["id"]),
            "recipient": recipient,
            "created": cursor.rowcount == 1,
            "delivered_at": now_s,
        }

    return sink


class OutboxService:
    """事件箱的租约投递：按游标分批确认，崩溃后按租约恢复。

    一批事件先被租约锁定（processing + 令牌），逐事件投递到信箱并各自提交，
    最后在同一事务里确认整批并推进连续已投递游标。确认前崩溃的事件会在租约
    过期后被重新领取，重复投递由信箱去重吸收；超过尝试上限的事件进入人工
    处理（failed + dead_lettered_at），可由管理员重放。
    """

    def __init__(
        self,
        connection: sqlite3.Connection,
        clock: Clock | None = None,
        *,
        sink: DeliverySink | None = None,
        backoff_base_seconds: int = 15,
        backoff_cap_seconds: int = 900,
        channel: str = DEFAULT_CHANNEL,
    ) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        self.backoff_base_seconds = backoff_base_seconds
        self.backoff_cap_seconds = backoff_cap_seconds
        self.channel = channel
        self.sink = sink or notification_sink(connection, self.clock, channel)

    def emit(
        self,
        event_key: str,
        event_type: str,
        aggregate_type: str,
        aggregate_id: str | int,
        payload: dict,
        *,
        delay_seconds: int = 0,
        max_attempts: int | None = None,
    ) -> tuple[dict, bool]:
        if max_attempts is not None and max_attempts < 1:
            raise ValidationError("最大尝试次数必须大于等于 1")
        now = self.clock.now()
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO outbox_events(event_key,event_type,aggregate_type,aggregate_id,payload_json,"
            "status,available_at,max_attempts,created_at) VALUES(?,?,?,?,?,'pending',?,?,?)",
            (
                event_key,
                event_type,
                aggregate_type,
                str(aggregate_id),
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                to_storage(now + timedelta(seconds=delay_seconds)),
                max_attempts if max_attempts is not None else DEFAULT_MAX_ATTEMPTS,
                to_storage(now),
            ),
        )
        row = self.connection.execute("SELECT * FROM outbox_events WHERE event_key=?", (event_key,)).fetchone()
        return dict(row), cursor.rowcount == 1

    def lease_batch(self, publisher: str, *, limit: int = 50, lease_seconds: int = 120) -> list[dict]:
        now = self.clock.now()
        now_s = to_storage(now)
        self._reclaim_expired(now, lease_seconds)
        rows = self.connection.execute(
            "SELECT * FROM outbox_events WHERE status='pending' AND available_at<=? ORDER BY id LIMIT ?",
            (now_s, limit),
        ).fetchall()
        leased: list[dict] = []
        for row in rows:
            attempt_no = self._next_attempt_no(int(row["id"]))
            token = f"outbox-{row['id']}-{attempt_no}-{secrets.token_hex(8)}"
            expires_s = to_storage(now + timedelta(seconds=lease_seconds))
            cursor = self.connection.execute(
                "UPDATE outbox_events SET status='processing',attempts=attempts+1,locked_by=?,locked_at=?,lease_token=?,"
                "lease_expires_at=? WHERE id=? AND status='pending'",
                (publisher, now_s, token, expires_s, row["id"]),
            )
            if cursor.rowcount != 1:
                continue
            self.connection.execute(
                "INSERT INTO outbox_attempts(event_id,attempt_no,publisher,lease_token,leased_at,lease_expires_at) "
                "VALUES(?,?,?,?,?,?)",
                (row["id"], attempt_no, publisher, token, now_s, expires_s),
            )
            leased.append(self._require(int(row["id"])))
        return leased

    def deliver_event(
        self,
        event_id: int,
        publisher: str,
        lease_token: str,
        *,
        sink: DeliverySink | None = None,
    ) -> dict:
        event = self._require_leased(event_id, publisher, lease_token)
        receipt = dict((sink or self.sink)(event))
        self.connection.execute(
            "UPDATE outbox_events SET delivery_json=? WHERE id=?",
            (json.dumps(receipt, ensure_ascii=False, sort_keys=True), event_id),
        )
        return receipt

    def confirm_batch(self, publisher: str, leases: list[tuple[int, str]]) -> dict:
        now_s = to_storage(self.clock.now())
        confirmed: list[int] = []
        for event_id, token in leases:
            cursor = self.connection.execute(
                "UPDATE outbox_events SET status='published',published_at=?,locked_by=NULL,locked_at=NULL,"
                "lease_token=NULL,lease_expires_at=NULL WHERE id=? AND status='processing' AND locked_by=? AND lease_token=?",
                (now_s, event_id, publisher, token),
            )
            if cursor.rowcount != 1:
                raise ConflictError("事件租约已失效，迟到的确认回执被拒绝", context=self._event_context(event_id))
            self.connection.execute(
                "UPDATE outbox_attempts SET outcome='published',finished_at=?,"
                "delivery_json=(SELECT delivery_json FROM outbox_events WHERE id=?) "
                "WHERE event_id=? AND lease_token=? AND finished_at IS NULL",
                (now_s, event_id, event_id, token),
            )
            confirmed.append(event_id)
        cursor_value = self._advance_cursor()
        return {"confirmed_ids": confirmed, "confirmed": len(confirmed), "cursor": cursor_value}

    def fail_event(self, event_id: int, publisher: str, lease_token: str, message: str) -> dict:
        row = self.connection.execute("SELECT * FROM outbox_events WHERE id=?", (event_id,)).fetchone()
        if row is None:
            raise NotFoundError("事件不存在")
        now = self.clock.now()
        now_s = to_storage(now)
        attempts = int(row["attempts"])
        if attempts >= int(row["max_attempts"]):
            status, available_at, dead_lettered_at, outcome = "failed", now_s, now_s, "dead_lettered"
        else:
            status = "pending"
            available_at = to_storage(now + timedelta(seconds=self.backoff_seconds(attempts)))
            dead_lettered_at, outcome = None, "failed"
        cursor = self.connection.execute(
            "UPDATE outbox_events SET status=?,last_error=?,available_at=?,locked_by=NULL,locked_at=NULL,"
            "lease_token=NULL,lease_expires_at=NULL,dead_lettered_at=? "
            "WHERE id=? AND status='processing' AND locked_by=? AND lease_token=?",
            (status, message[:1000], available_at, dead_lettered_at, event_id, publisher, lease_token),
        )
        if cursor.rowcount != 1:
            raise ConflictError("事件租约已失效，迟到的失败回执被拒绝", context=self._event_context(event_id))
        self.connection.execute(
            "UPDATE outbox_attempts SET outcome=?,finished_at=?,error_message=? "
            "WHERE event_id=? AND lease_token=? AND finished_at IS NULL",
            (outcome, now_s, message[:1000], event_id, lease_token),
        )
        return self._require(event_id)

    def replay_event(self, event_id: int, actor: str, *, sink: DeliverySink | None = None) -> dict:
        """管理员重放单个事件：只重新投递，不触碰任何业务表。"""
        row = self.connection.execute("SELECT * FROM outbox_events WHERE id=?", (event_id,)).fetchone()
        if row is None:
            raise NotFoundError("事件不存在")
        event = dict(row)
        if event["status"] == "processing":
            raise ConflictError("事件正在投递中，不能重放", context={"locked_by": event["locked_by"]})
        event["payload"] = json.loads(event["payload_json"] or "{}")
        receipt = dict((sink or self.sink)(event))
        now_s = to_storage(self.clock.now())
        attempt_no = self._next_attempt_no(event_id)
        self.connection.execute(
            "UPDATE outbox_events SET status='published',published_at=COALESCE(published_at,?),attempts=?,"
            "delivery_json=?,last_error=NULL,dead_lettered_at=NULL,locked_by=NULL,locked_at=NULL,"
            "lease_token=NULL,lease_expires_at=NULL WHERE id=?",
            (now_s, attempt_no, json.dumps(receipt, ensure_ascii=False, sort_keys=True), event_id),
        )
        self.connection.execute(
            "INSERT INTO outbox_attempts(event_id,attempt_no,publisher,lease_token,leased_at,lease_expires_at,"
            "finished_at,outcome,delivery_json) VALUES(?,?,?,?,?,?,?,'replayed',?)",
            (
                event_id,
                attempt_no,
                actor,
                f"replay-{event_id}-{attempt_no}-{secrets.token_hex(4)}",
                now_s,
                now_s,
                now_s,
                json.dumps(receipt, ensure_ascii=False, sort_keys=True),
            ),
        )
        cursor_value = self._advance_cursor()
        return {"event": self._present(self._require(event_id), include_payload=True), "delivery": receipt, "cursor": cursor_value}

    def publish_batch(self, publisher: str, *, limit: int = 50, lease_seconds: int = 120) -> dict:
        """执行一轮发布：租约 → 逐事件投递（各自提交）→ 同批确认并推进游标。"""
        with transaction(self.connection, immediate=True):
            leased = self.lease_batch(publisher, limit=limit, lease_seconds=lease_seconds)
        delivered: list[tuple[int, str]] = []
        failures: list[dict] = []
        for event in leased:
            try:
                with transaction(self.connection, immediate=True):
                    self.deliver_event(event["id"], publisher, event["lease_token"])
                delivered.append((event["id"], event["lease_token"]))
            except ConflictError:
                continue
            except Exception as exc:
                try:
                    with transaction(self.connection, immediate=True):
                        self.fail_event(event["id"], publisher, event["lease_token"], str(exc))
                    failures.append({"event_id": event["id"], "error": str(exc)[:500]})
                except ConflictError:
                    continue
        with transaction(self.connection, immediate=True):
            summary = self.confirm_batch(publisher, delivered)
        return {"leased": len(leased), "delivered": len(delivered), "failures": failures, **summary}

    def backoff_seconds(self, attempts: int) -> int:
        return min(self.backoff_base_seconds * (2 ** max(0, attempts - 1)), self.backoff_cap_seconds)

    def cursor(self) -> int:
        row = self.connection.execute(
            "SELECT last_confirmed_id FROM publisher_checkpoints WHERE channel=?", (self.channel,)
        ).fetchone()
        return int(row[0]) if row else 0

    def list_events(
        self,
        *,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict], int]:
        params: list[Any] = []
        where = ""
        if status:
            where = " WHERE status=?"
            params.append(status)
        total = int(self.connection.execute(f"SELECT COUNT(*) FROM outbox_events{where}", params).fetchone()[0])
        rows = self.connection.execute(
            f"SELECT * FROM outbox_events{where} ORDER BY id LIMIT ? OFFSET ?", (*params, limit, offset)
        ).fetchall()
        return [self._present(dict(row)) for row in rows], total

    def detail(self, event_id: int) -> dict:
        event = self._present(self._require(event_id), include_payload=True)
        rows = self.connection.execute(
            "SELECT * FROM outbox_attempts WHERE event_id=? ORDER BY attempt_no", (event_id,)
        ).fetchall()
        event["attempt_history"] = [dict(row) for row in rows]
        return event

    def list_notifications(
        self,
        *,
        recipient: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict], int]:
        params: list[Any] = []
        where = ""
        if recipient:
            where = " WHERE recipient=?"
            params.append(recipient)
        total = int(self.connection.execute(f"SELECT COUNT(*) FROM notification_messages{where}", params).fetchone()[0])
        rows = self.connection.execute(
            f"SELECT * FROM notification_messages{where} ORDER BY id LIMIT ? OFFSET ?", (*params, limit, offset)
        ).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item["payload_json"] or "{}")
            items.append(item)
        return items, total

    def _next_attempt_no(self, event_id: int) -> int:
        row = self.connection.execute(
            "SELECT COALESCE(MAX(attempt_no),0) FROM outbox_attempts WHERE event_id=?", (event_id,)
        ).fetchone()
        return int(row[0]) + 1

    def _reclaim_expired(self, now, lease_seconds: int) -> None:
        now_s = to_storage(now)
        legacy_stale = to_storage(now - timedelta(seconds=lease_seconds))
        expired = self.connection.execute(
            "SELECT id FROM outbox_events WHERE status='processing' AND "
            "(lease_expires_at<=? OR (lease_expires_at IS NULL AND locked_at<=?))",
            (now_s, legacy_stale),
        ).fetchall()
        for row in expired:
            self.connection.execute(
                "UPDATE outbox_attempts SET outcome='expired',finished_at=? WHERE event_id=? AND finished_at IS NULL",
                (now_s, row["id"]),
            )
            self.connection.execute(
                "UPDATE outbox_events SET status='pending',available_at=?,locked_by=NULL,locked_at=NULL,"
                "lease_token=NULL,lease_expires_at=NULL WHERE id=? AND status='processing'",
                (now_s, row["id"]),
            )

    def _advance_cursor(self) -> int:
        gap = self.connection.execute("SELECT MIN(id) FROM outbox_events WHERE status!='published'").fetchone()[0]
        if gap is None:
            target = int(self.connection.execute("SELECT COALESCE(MAX(id),0) FROM outbox_events").fetchone()[0])
        else:
            target = int(gap) - 1
        now_s = to_storage(self.clock.now())
        self.connection.execute(
            "INSERT INTO publisher_checkpoints(channel,last_confirmed_id,updated_at) VALUES(?,?,?) "
            "ON CONFLICT(channel) DO UPDATE SET last_confirmed_id=MAX(last_confirmed_id,excluded.last_confirmed_id),"
            "updated_at=excluded.updated_at",
            (self.channel, target, now_s),
        )
        return self.cursor()

    def _require_leased(self, event_id: int, publisher: str, lease_token: str) -> dict:
        row = self.connection.execute("SELECT * FROM outbox_events WHERE id=?", (event_id,)).fetchone()
        if row is None:
            raise NotFoundError("事件不存在")
        event = dict(row)
        if (
            event["status"] != "processing"
            or event["locked_by"] != publisher
            or event["lease_token"] != lease_token
        ):
            raise ConflictError("事件租约已失效，迟到的投递回执被拒绝", context=self._event_context(event_id))
        event["payload"] = json.loads(event["payload_json"] or "{}")
        return event

    def _event_context(self, event_id: int) -> dict:
        row = self.connection.execute(
            "SELECT status,locked_by,attempts FROM outbox_events WHERE id=?", (event_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError("事件不存在")
        return {"status": row["status"], "locked_by": row["locked_by"], "attempts": row["attempts"]}

    def _require(self, event_id: int) -> dict:
        row = self.connection.execute("SELECT * FROM outbox_events WHERE id=?", (event_id,)).fetchone()
        if row is None:
            raise NotFoundError("事件不存在")
        return dict(row)

    def _present(self, event: dict, *, include_payload: bool = False) -> dict:
        payload = json.loads(event["payload_json"] or "{}")
        event["delivery"] = json.loads(event["delivery_json"]) if event.get("delivery_json") else None
        lot: dict[str, Any] = {}
        if payload.get("lot_id") is not None:
            lot["lot_id"] = payload["lot_id"]
        if payload.get("lot_no") is not None:
            lot["lot_no"] = payload["lot_no"]
        event["lot"] = lot or None
        if include_payload:
            event["payload"] = payload
        return event
