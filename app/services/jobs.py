from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError
from app.database import transaction


def _decode_job(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    data["payload"] = json.loads(data.pop("payload_json") or "{}")
    raw_result = data.pop("result_json")
    data["result"] = json.loads(raw_result) if raw_result else None
    return data


class JobService:
    """后台作业的租约执行：领取、完成、失败退避与人工处理。

    每次领取都会签发唯一的租约令牌（fencing token）并写入 job_attempts 留痕。
    租约过期后其他执行者可以接管，旧执行者持过期令牌提交的迟到回执会被拒绝，
    不会覆盖新执行者的结果。失败按注入时钟指数退避，达到 max_attempts 后
    进入 failed 状态等待人工处理（requeue 重新排队）。
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

    def enqueue(
        self,
        job_type: str,
        deduplication_key: str,
        payload: dict,
        *,
        delay_seconds: int = 0,
        max_attempts: int | None = None,
    ) -> dict:
        now = self.clock.now()
        try:
            cursor = self.connection.execute(
                "INSERT INTO background_jobs(job_type,deduplication_key,payload_json,status,available_at,max_attempts,"
                "created_at,updated_at) VALUES(?,?,?,'pending',?,?,?,?)",
                (
                    job_type, deduplication_key, json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    to_storage(now + timedelta(seconds=delay_seconds)), max_attempts or 5,
                    to_storage(now), to_storage(now),
                ),
            )
        except sqlite3.IntegrityError as exc:
            row = self.connection.execute(
                "SELECT * FROM background_jobs WHERE deduplication_key=?", (deduplication_key,)
            ).fetchone()
            if row:
                return _decode_job(row)
            raise ConflictError("后台任务去重键冲突") from exc
        return self.require(int(cursor.lastrowid))

    def require(self, job_id: int) -> dict:
        row = self.connection.execute("SELECT * FROM background_jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise NotFoundError("后台作业不存在")
        return _decode_job(row)

    def claim(self, worker: str, *, lease_seconds: int | None = None) -> dict | None:
        """领取一个可执行作业并签发租约；过期租约的作业会被接管。

        必须在事务中调用（执行器使用 IMMEDIATE 事务），保证选择、旧尝试作废、
        新尝试登记三步原子完成。
        """
        now = self.clock.now()
        lease = lease_seconds or self.lease_seconds
        now_text = to_storage(now)
        stale_text = to_storage(now - timedelta(seconds=lease))
        expires_text = to_storage(now + timedelta(seconds=lease))
        row = self.connection.execute(
            "SELECT id,status FROM background_jobs WHERE (status='pending' AND available_at<=?) "
            "OR (status='running' AND ((lease_expires_at IS NOT NULL AND lease_expires_at<?) "
            "OR (lease_expires_at IS NULL AND locked_at<?))) ORDER BY available_at,id LIMIT 1",
            (now_text, now_text, stale_text),
        ).fetchone()
        if row is None:
            return None
        job_id = int(row["id"])
        if row["status"] == "running":
            # 接管过期租约：把旧执行者未完成的尝试标记为 expired
            self.connection.execute(
                "UPDATE job_attempts SET outcome='expired',finished_at=? WHERE job_id=? AND finished_at IS NULL",
                (now_text, job_id),
            )
        attempt_no = int(self.connection.execute(
            "SELECT COALESCE(MAX(attempt_no),0)+1 FROM job_attempts WHERE job_id=?", (job_id,)
        ).fetchone()[0])
        token = uuid.uuid4().hex
        cursor = self.connection.execute(
            "UPDATE background_jobs SET status='running',attempts=attempts+1,locked_by=?,locked_at=?,"
            "lease_token=?,lease_expires_at=?,updated_at=? "
            "WHERE id=? AND (status='pending' OR (status='running' AND "
            "((lease_expires_at IS NOT NULL AND lease_expires_at<?) OR (lease_expires_at IS NULL AND locked_at<?))))",
            (worker, now_text, token, expires_text, now_text, job_id, now_text, stale_text),
        )
        if cursor.rowcount != 1:
            return None
        self.connection.execute(
            "INSERT INTO job_attempts(job_id,attempt_no,worker,lease_token,leased_at,lease_expires_at,created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (job_id, attempt_no, worker, token, now_text, expires_text, now_text),
        )
        claimed = self.require(job_id)
        claimed["lease_token"] = token
        claimed["attempt_no"] = attempt_no
        return claimed

    def complete(self, job_id: int, worker: str, lease_token: str, result: dict) -> dict:
        now = to_storage(self.clock.now())
        cursor = self.connection.execute(
            "UPDATE background_jobs SET status='completed',result_json=?,locked_at=NULL,locked_by=NULL,"
            "lease_token=NULL,lease_expires_at=NULL,updated_at=? "
            "WHERE id=? AND status='running' AND locked_by=? AND lease_token=?",
            (json.dumps(result, ensure_ascii=False, sort_keys=True), now, job_id, worker, lease_token),
        )
        if cursor.rowcount != 1:
            raise ConflictError("租约已失效或被其他执行者接管，迟到回执被拒绝")
        self.connection.execute(
            "UPDATE job_attempts SET outcome='completed',finished_at=? WHERE job_id=? AND lease_token=? AND finished_at IS NULL",
            (now, job_id, lease_token),
        )
        return self.require(job_id)

    def fail(self, job_id: int, worker: str, lease_token: str, message: str, *, retry_seconds: int | None = None) -> dict:
        job = self.require(job_id)
        now = self.clock.now()
        now_text = to_storage(now)
        attempts = int(job["attempts"])
        max_attempts = int(job["max_attempts"])
        if attempts >= max_attempts:
            status, available_at, outcome = "failed", now_text, "failed"
        else:
            delay = retry_seconds if retry_seconds is not None else self.retry_delay(attempts)
            status = "pending"
            available_at = to_storage(now + timedelta(seconds=delay))
            outcome = "retry"
        cursor = self.connection.execute(
            "UPDATE background_jobs SET status=?,error_message=?,available_at=?,locked_at=NULL,locked_by=NULL,"
            "lease_token=NULL,lease_expires_at=NULL,updated_at=? "
            "WHERE id=? AND status='running' AND locked_by=? AND lease_token=?",
            (status, message[:1000], available_at, now_text, job_id, worker, lease_token),
        )
        if cursor.rowcount != 1:
            raise ConflictError("租约已失效或被其他执行者接管，迟到回执被拒绝")
        self.connection.execute(
            "UPDATE job_attempts SET outcome=?,finished_at=?,message=? WHERE job_id=? AND lease_token=? AND finished_at IS NULL",
            (outcome, now_text, message[:1000], job_id, lease_token),
        )
        return self.require(job_id)

    def requeue(self, job_id: int) -> dict:
        """人工处理：把失败或取消的作业重新排队，尝试计数归零重新开始。"""
        now = to_storage(self.clock.now())
        cursor = self.connection.execute(
            "UPDATE background_jobs SET status='pending',attempts=0,available_at=?,error_message=NULL,"
            "locked_at=NULL,locked_by=NULL,lease_token=NULL,lease_expires_at=NULL,updated_at=? "
            "WHERE id=? AND status IN ('failed','cancelled')",
            (now, now, job_id),
        )
        if cursor.rowcount != 1:
            raise ConflictError("只有失败或取消的作业可以重新排队")
        return self.require(job_id)

    def attempts(self, job_id: int) -> list[dict]:
        rows = self.connection.execute(
            "SELECT * FROM job_attempts WHERE job_id=? ORDER BY attempt_no", (job_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    def detail(self, job_id: int) -> dict:
        job = self.require(job_id)
        job["attempts_log"] = self.attempts(job_id)
        return job

    def list_jobs(self, *, status: str | None = None, limit: int = 50) -> list[dict]:
        if status:
            rows = self.connection.execute(
                "SELECT * FROM background_jobs WHERE status=? ORDER BY updated_at DESC,id DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM background_jobs ORDER BY updated_at DESC,id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [_decode_job(row) for row in rows]


@dataclass(slots=True)
class JobContext:
    worker: str
    attempt_no: int
    lease_token: str
    clock: Clock


JobHandler = Callable[[sqlite3.Connection, dict, JobContext], dict]


class JobExecutor:
    """租约执行器：领取（事务一）→ 业务与 outbox 写入及完成（事务二）→ 失败退避（事务三）。

    进程在领取后崩溃：租约到期后由其他执行者接管，业务事务未提交所以无残留。
    进程在业务提交后崩溃：作业已完成，outbox 事件等待发布端投递。
    执行器依赖 app.database 的线程本地连接划分事务。
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
        self.handlers: dict[str, JobHandler] = {}

    def register(self, job_type: str, handler: JobHandler) -> None:
        self.handlers[job_type] = handler

    def _service(self, connection: sqlite3.Connection) -> JobService:
        return JobService(
            connection,
            self.clock,
            lease_seconds=self.lease_seconds,
            backoff_base_seconds=self.backoff_base_seconds,
            backoff_cap_seconds=self.backoff_cap_seconds,
        )

    def run_next(self, worker: str) -> dict | None:
        with transaction(immediate=True) as connection:
            claimed = self._service(connection).claim(worker)
        if claimed is None:
            return None
        job_id = int(claimed["id"])
        context = JobContext(
            worker=worker,
            attempt_no=int(claimed["attempt_no"]),
            lease_token=str(claimed["lease_token"]),
            clock=self.clock,
        )
        handler = self.handlers.get(str(claimed["job_type"]))
        if handler is None:
            message = f"没有注册的作业处理器：{claimed['job_type']}"
            with transaction(immediate=True) as connection:
                final = self._service(connection).fail(job_id, worker, context.lease_token, message)
            return {
                "job_id": job_id, "job_type": claimed["job_type"],
                "outcome": "retry" if final["status"] == "pending" else "failed",
                "attempt_no": context.attempt_no, "error": message,
            }
        try:
            with transaction(immediate=True) as connection:
                result = handler(connection, dict(claimed["payload"]), context) or {}
                self._service(connection).complete(job_id, worker, context.lease_token, result)
            return {
                "job_id": job_id, "job_type": claimed["job_type"], "outcome": "completed",
                "attempt_no": context.attempt_no, "result": result,
            }
        except ConflictError as exc:
            # 业务冲突或租约被接管：本次执行作废，租约到期后由其他执行者重试
            return {"job_id": job_id, "job_type": claimed["job_type"], "outcome": "conflict", "error": exc.message}
        except Exception as exc:  # noqa: BLE001 - 业务异常统一转入退避
            message = str(exc)[:500]
            with transaction(immediate=True) as connection:
                final = self._service(connection).fail(job_id, worker, context.lease_token, message)
            return {
                "job_id": job_id, "job_type": claimed["job_type"],
                "outcome": "retry" if final["status"] == "pending" else "failed",
                "attempt_no": context.attempt_no, "error": message,
                "available_at": final["available_at"],
            }

    def run(self, worker: str, *, limit: int = 10) -> list[dict]:
        outcomes: list[dict] = []
        for _ in range(limit):
            outcome = self.run_next(worker)
            if outcome is None:
                break
            outcomes.append(outcome)
        return outcomes
