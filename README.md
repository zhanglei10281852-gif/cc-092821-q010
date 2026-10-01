# 种质资源入库与活力复检服务

本项目是面向种质资源库的 Python 后端服务，用于登记采集或引进材料、建立种子批次、管理低温库位和容器移动、执行发芽活力检测、生成复检日程并处理环境与质量告警。档案、库存、检测和发放审批都保存在本地 SQLite 中，关键写入带版本或幂等键，适合在单个 Linux 应用容器内运行。

## 运行环境

- Python 3.11
- FastAPI 与 Uvicorn
- SQLite 3，由 Python 标准库提供

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `data/germplasm.db`，也可以通过 `GERMPLASM_DATABASE_PATH` 指向其他 `.db`、`.sqlite` 或 `.sqlite3` 文件。

## 初始化与启动

```bash
python -m app.cli init-db
python -m app.cli check-db
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查为 `GET /api/system/health`。首次使用可调用 `POST /api/auth/bootstrap` 创建管理员，再通过 `POST /api/auth/login` 取得 Bearer 会话令牌。种质业务接口统一位于 `/api/germplasm`。

## 测试与构建检查

```bash
python -m pytest
python -m compileall -q app tests
```

下面两条命令分别检查 HTTP 入口和完整的入库演示链路：

```bash
python -m app.cli smoke
python -m app.cli demo
```

## 业务边界

- `app/germplasm/accessions.py` 管理来源、资源档案、护照信息与接收状态。
- `app/germplasm/inventory.py` 管理批次、库位容量、容器摆放、移动、领用和冻结。
- `app/germplasm/viability.py` 管理检测规程、取样、重复计数、活力结果与复检日程。
- `app/germplasm/quality.py` 管理温湿度读数、偏离告警和种质发放审批。
- `app/services/jobs.py` 提供后台作业的租约执行，`app/services/outbox.py` 提供事件箱投递，`app/services/reminders.py` 定义夜间复检提醒扫描作业。
- `app/api`、`app/services` 和 `app/repositories` 提供身份、权限、审计、后台作业及维护能力。

## 后台作业与事件投递

复检提醒的夜间批量生成由 `retest.reminder.scan` 作业完成，按 `retest-reminder-scan:{日期}` 去重登记。作业执行满足以下恢复约定：

- **事务边界**：领取作业自成一个事务并写入租约令牌与尝试记录（`job_attempts`）；复检日程变更、outbox 事件写入和完成回执在同一个事务提交，失败时整体回滚，不会出现"日程已标记但事件缺失"的中间状态。
- **租约接管**：租约过期后其他执行者可接管（旧式无租约列的 running 记录按 `locked_at` 兜底回收）；完成/失败回执必须携带当前租约令牌，旧执行者的迟到回执会被拒绝，不会覆盖新结果。
- **退避与人工处理**：失败按可注入时钟指数退避（`backoff_base_seconds` 起步、翻倍、封顶），达到 `max_attempts` 后进入人工处理（`failed` + `dead_lettered_at`），管理员确认后可重新排队（requeue）。
- **事件发布**：发布端先租约一批 `pending` 事件，逐事件投递到待办信箱（`notification_messages` 按 `event_key` 去重，重复投递不产生重复通知），再同事务确认整批并推进 `publisher_checkpoints` 中的连续已投递游标。领取后、业务提交后或确认前中断，都会在租约过期后安全重放。事件超过尝试上限进入人工处理，管理员可重放单个事件（replay），只重新投递、不改变任何业务表。
- **可观测**：`GET /api/system/jobs`、`/api/system/jobs/{id}`、`/api/system/outbox`、`/api/system/outbox/{id}`、`/api/system/notifications` 展示每次尝试、租约所有者、关联批次（lot）与最终投递结果；对应 CLI 为 `jobs-list`/`job-show`/`outbox-list`/`outbox-show`/`notifications-list`。

常用运维命令：

```bash
python -m app.cli enqueue-reminder-scan --date 2026-10-01   # 登记当夜扫描作业（幂等）
python -m app.cli jobs-run --worker nightly-1               # 领取并执行到期作业
python -m app.cli outbox-publish --publisher ops-1          # 按游标发布一批事件
python -m app.cli job-show 1                                # 查看尝试记录与结果
python -m app.cli outbox-show 1                             # 查看投递尝试与最终结果
python -m app.cli outbox-replay 1 --actor ops-admin         # 重放单个事件
```

## 一致性约定

SQLite 连接启用外键、WAL、忙等待和即时写事务。资源档案、库位、容器摆放、检测任务和发放申请采用版本号防止旧请求覆盖新状态；入库、移库、取样和传感读数使用业务键去重。活力检测保留采用的规程版本和每个重复的观察计数，完成后可依据作物及风险策略生成下一次复检日期。会话令牌只保存摘要，审计记录不保存明文密码或令牌。
