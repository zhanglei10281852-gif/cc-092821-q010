from __future__ import annotations

import json
import secrets
import sqlite3
from datetime import timedelta
from typing import Any, Callable

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import transaction

JobHandler = Callable[[sqlite3.Connection, dict[str, Any], Clock], dict[str, Any]]

DEFAULT_MAX_ATTEMPTS = 5


class JobService:
    """后台作业的租约执行。

    领取（claim）自成一个事务并写入租约令牌与尝试记录；业务变更与完成/失败回执
    由调用方放在同一个事务里提交。回执必须携带当前租约令牌：租约过期后其他执行者
    可以接管，旧执行者的迟到回执会因为令牌不匹配而被拒绝，不会覆盖新结果。
    """

    def __init__(
        self,
        connection: sqlite3.Connection,
        clock: Clock | None = None,
        *,
        backoff_base_seconds: int = 30,
        backoff_cap_seconds: int = 1800,
    ) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        self.backoff_base_seconds = backoff_base_seconds
        self.backoff_cap_seconds = backoff_cap_seconds

    def enqueue(
        self,
        job_type: str,
        deduplication_key: str,
        payload: dict,
        *,
        delay_seconds: int = 0,
        max_attempts: int | None = None,
    ) -> dict:
        if max_attempts is not None and max_attempts < 1:
            raise ValidationError("最大尝试次数必须大于等于 1")
        now = self.clock.now()
        try:
            cursor = self.connection.execute(
                "INSERT INTO background_jobs(job_type,deduplication_key,payload_json,status,available_at,max_attempts,"
                "created_at,updated_at) VALUES(?,?,?,'pending',?,?,?,?)",
                (
                    job_type,
                    deduplication_key,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    to_storage(now + timedelta(seconds=delay_seconds)),
                    max_attempts if max_attempts is not None else DEFAULT_MAX_ATTEMPTS,
                    to_storage(now),
                    to_storage(now),
                ),
            )
        except sqlite3.IntegrityError as exc:
            row = self.connection.execute(
                "SELECT * FROM background_jobs WHERE deduplication_key=?", (deduplication_key,)
            ).fetchone()
            if row:
                return dict(row)
            raise ConflictError("后台任务去重键冲突") from exc
        return self._require(int(cursor.lastrowid))

    def claim(self, worker: str, *, lease_seconds: int = 60, job_types: list[str] | None = None) -> dict | None:
        now = self.clock.now()
        now_s = to_storage(now)
        self._reclaim_expired(now, lease_seconds)
        clause = ""
        params: list[Any] = [now_s]
        if job_types:
            clause = " AND job_type IN (" + ",".join("?" for _ in job_types) + ")"
            params.extend(job_types)
        row = self.connection.execute(
            f"SELECT * FROM background_jobs WHERE status='pending' AND available_at<=?{clause} "
            "ORDER BY available_at,id LIMIT 1",
            params,
        ).fetchone()
        if row is None:
            return None
        attempt_no = self._next_attempt_no(int(row["id"]))
        token = f"job-{row['id']}-{attempt_no}-{secrets.token_hex(8)}"
        expires_s = to_storage(now + timedelta(seconds=lease_seconds))
        cursor = self.connection.execute(
            "UPDATE background_jobs SET status='running',attempts=attempts+1,locked_at=?,locked_by=?,lease_token=?,"
            "lease_expires_at=?,updated_at=? WHERE id=? AND status='pending'",
            (now_s, worker, token, expires_s, now_s, row["id"]),
        )
        if cursor.rowcount != 1:
            return None
        self.connection.execute(
            "INSERT INTO job_attempts(job_id,attempt_no,worker,lease_token,leased_at,lease_expires_at) VALUES(?,?,?,?,?,?)",
            (row["id"], attempt_no, worker, token, now_s, expires_s),
        )
        return self._require(int(row["id"]))

    def complete(self, job_id: int, worker: str, lease_token: str, result: dict) -> dict:
        now_s = to_storage(self.clock.now())
        payload = json.dumps(result, ensure_ascii=False, sort_keys=True)
        cursor = self.connection.execute(
            "UPDATE background_jobs SET status='completed',result_json=?,locked_at=NULL,locked_by=NULL,"
            "lease_token=NULL,lease_expires_at=NULL,updated_at=? "
            "WHERE id=? AND status='running' AND locked_by=? AND lease_token=?",
            (payload, now_s, job_id, worker, lease_token),
        )
        if cursor.rowcount != 1:
            raise ConflictError("任务租约已失效，迟到的完成回执被拒绝", context=self._lease_context(job_id))
        self.connection.execute(
            "UPDATE job_attempts SET outcome='completed',finished_at=?,result_json=? "
            "WHERE job_id=? AND lease_token=? AND finished_at IS NULL",
            (now_s, payload, job_id, lease_token),
        )
        return self._require(job_id)

    def fail(self, job_id: int, worker: str, lease_token: str, message: str) -> dict:
        row = self.connection.execute("SELECT * FROM background_jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise NotFoundError("后台任务不存在")
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
            "UPDATE background_jobs SET status=?,error_message=?,available_at=?,locked_at=NULL,locked_by=NULL,"
            "lease_token=NULL,lease_expires_at=NULL,dead_lettered_at=?,updated_at=? "
            "WHERE id=? AND status='running' AND locked_by=? AND lease_token=?",
            (status, message[:1000], available_at, dead_lettered_at, now_s, job_id, worker, lease_token),
        )
        if cursor.rowcount != 1:
            raise ConflictError("任务租约已失效，迟到的失败回执被拒绝", context=self._lease_context(job_id))
        self.connection.execute(
            "UPDATE job_attempts SET outcome=?,finished_at=?,error_message=? "
            "WHERE job_id=? AND lease_token=? AND finished_at IS NULL",
            (outcome, now_s, message[:1000], job_id, lease_token),
        )
        return self._require(job_id)

    def requeue(self, job_id: int) -> dict:
        row = self.connection.execute("SELECT * FROM background_jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise NotFoundError("后台任务不存在")
        if row["status"] != "failed" or row["dead_lettered_at"] is None:
            raise ConflictError("只有进入人工处理的失败任务可以重新排队")
        now_s = to_storage(self.clock.now())
        self.connection.execute(
            "UPDATE background_jobs SET status='pending',attempts=0,available_at=?,error_message=NULL,"
            "dead_lettered_at=NULL,updated_at=? WHERE id=?",
            (now_s, now_s, job_id),
        )
        return self._require(job_id)

    def backoff_seconds(self, attempts: int) -> int:
        return min(self.backoff_base_seconds * (2 ** max(0, attempts - 1)), self.backoff_cap_seconds)

    def list_jobs(
        self,
        *,
        status: str | None = None,
        job_type: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[dict], int]:
        conditions: list[str] = []
        params: list[Any] = []
        if status:
            conditions.append("status=?")
            params.append(status)
        if job_type:
            conditions.append("job_type=?")
            params.append(job_type)
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        total = int(self.connection.execute(f"SELECT COUNT(*) FROM background_jobs{where}", params).fetchone()[0])
        rows = self.connection.execute(
            f"SELECT * FROM background_jobs{where} ORDER BY id DESC LIMIT ? OFFSET ?",
            (*params, limit, offset),
        ).fetchall()
        return [dict(row) for row in rows], total

    def detail(self, job_id: int) -> dict:
        job = self._require(job_id)
        job["payload"] = json.loads(job["payload_json"] or "{}")
        job["result"] = json.loads(job["result_json"]) if job.get("result_json") else None
        rows = self.connection.execute(
            "SELECT * FROM job_attempts WHERE job_id=? ORDER BY attempt_no", (job_id,)
        ).fetchall()
        job["attempt_history"] = [dict(row) for row in rows]
        return job

    def _next_attempt_no(self, job_id: int) -> int:
        row = self.connection.execute(
            "SELECT COALESCE(MAX(attempt_no),0) FROM job_attempts WHERE job_id=?", (job_id,)
        ).fetchone()
        return int(row[0]) + 1

    def _reclaim_expired(self, now, lease_seconds: int) -> None:
        now_s = to_storage(now)
        legacy_stale = to_storage(now - timedelta(seconds=lease_seconds))
        expired = self.connection.execute(
            "SELECT id FROM background_jobs WHERE status='running' AND "
            "(lease_expires_at<=? OR (lease_expires_at IS NULL AND locked_at<=?))",
            (now_s, legacy_stale),
        ).fetchall()
        for row in expired:
            self.connection.execute(
                "UPDATE job_attempts SET outcome='expired',finished_at=? WHERE job_id=? AND finished_at IS NULL",
                (now_s, row["id"]),
            )
            self.connection.execute(
                "UPDATE background_jobs SET status='pending',available_at=?,locked_at=NULL,locked_by=NULL,"
                "lease_token=NULL,lease_expires_at=NULL,updated_at=? WHERE id=? AND status='running'",
                (now_s, now_s, row["id"]),
            )

    def _lease_context(self, job_id: int) -> dict:
        row = self.connection.execute(
            "SELECT status,locked_by,attempts FROM background_jobs WHERE id=?", (job_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError("后台任务不存在")
        return {"status": row["status"], "locked_by": row["locked_by"], "attempts": row["attempts"]}

    def _require(self, job_id: int) -> dict:
        row = self.connection.execute("SELECT * FROM background_jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise NotFoundError("后台任务不存在")
        return dict(row)


class JobRunner:
    """按事务边界执行作业：领取一个事务，业务变更与完成回执同一个事务。

    进程在领取后崩溃：租约到期后由其他执行者接管重跑；在业务提交后崩溃：作业已
    完成，事件留在事件箱等待发布；处理器抛错：按可注入时钟退避，超过上限进入
    人工处理。tick 必须在事务外调用。
    """

    def __init__(
        self,
        connection: sqlite3.Connection,
        clock: Clock | None = None,
        *,
        handlers: dict[str, JobHandler] | None = None,
        lease_seconds: int = 60,
        backoff_base_seconds: int = 30,
        backoff_cap_seconds: int = 1800,
    ) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        self.jobs = JobService(
            connection,
            self.clock,
            backoff_base_seconds=backoff_base_seconds,
            backoff_cap_seconds=backoff_cap_seconds,
        )
        self.handlers = dict(handlers or {})
        self.lease_seconds = lease_seconds

    def tick(self, worker: str, *, limit: int = 10) -> list[dict]:
        outcomes: list[dict] = []
        for _ in range(limit):
            with transaction(self.connection, immediate=True):
                job = self.jobs.claim(worker, lease_seconds=self.lease_seconds)
            if job is None:
                break
            outcomes.append(self._execute(worker, job))
        return outcomes

    def _execute(self, worker: str, job: dict) -> dict:
        handler = self.handlers.get(job["job_type"])
        try:
            with transaction(self.connection, immediate=True):
                if handler is None:
                    raise ValidationError(f"未注册的后台作业类型：{job['job_type']}")
                payload = json.loads(job["payload_json"] or "{}")
                result = handler(self.connection, payload, self.clock)
                completed = self.jobs.complete(job["id"], worker, job["lease_token"], result)
            return {
                "outcome": "completed",
                "job_id": job["id"],
                "job_type": job["job_type"],
                "attempts": completed["attempts"],
                "result": result,
            }
        except Exception as exc:
            try:
                with transaction(self.connection, immediate=True):
                    failed = self.jobs.fail(job["id"], worker, job["lease_token"], str(exc))
                return {
                    "outcome": "dead_lettered" if failed["dead_lettered_at"] else "failed",
                    "job_id": job["id"],
                    "job_type": job["job_type"],
                    "error": str(exc)[:500],
                    "available_at": failed["available_at"],
                }
            except ConflictError:
                return {
                    "outcome": "lease_lost",
                    "job_id": job["id"],
                    "job_type": job["job_type"],
                    "error": str(exc)[:500],
                }
