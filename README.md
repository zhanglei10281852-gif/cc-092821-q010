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
- `app/germplasm/jobs.py` 登记夜间复检提醒作业并装配租约执行器。
- `app/api`、`app/services` 和 `app/repositories` 提供身份、权限、审计、后台作业及维护能力。

## 后台作业与事件投递

夜间复检提醒等后台作业采用租约执行：每次领取签发唯一租约令牌并写入 `job_attempts` 留痕；业务变更（复检日程标记）与事件箱写入在同一事务提交，进程在领取后、业务提交后或确认前崩溃都能安全恢复。租约过期后其他执行者可以接管，旧执行者持过期令牌的迟到回执会被拒绝，不会覆盖新结果。失败按指数退避重试（时钟可注入，便于确定性测试），达到 `max_attempts` 后进入 `failed` 等待人工处理，可用 `job-requeue` 或 `POST /api/system/jobs/{id}/requeue` 重新排队。

事件箱发布端按 id 游标分批领取、逐条投递并在一个事务里批量确认；通知网关按事件键幂等去重，确认前崩溃不会导致重复通知。每次投递（成功、去重、失败、重放）都写入 `outbox_deliveries`，首次成功投递由唯一索引保证只出现一次。投递失败同样退避并在上限后进入人工处理；管理员可重放单个已终结事件（`outbox-replay` 或 `POST /api/system/outbox/{id}/replay`），只追加投递记录，不改变业务状态。

运维命令：

```bash
python -m app.cli reminders-enqueue --due-before 2026-10-02   # 登记夜间复检提醒作业（按日期去重）
python -m app.cli jobs-run --worker night-shift-1             # 按租约执行待处理作业
python -m app.cli jobs-list / job-show 1 / job-requeue 1      # 查看与人工处理
python -m app.cli outbox-publish --publisher cron-publisher   # 按游标分批投递
python -m app.cli outbox-list / outbox-show 3 / outbox-replay 3
```

`jobs-run` 与 `outbox-publish` 支持 `--now` 注入当前时间，配合 `job-show`（每次尝试、租约所有者）与 `outbox-show`（投递记录、关联批次、最终投递结果）可对重启场景做确定性演练。对应 HTTP 接口位于 `/api/system/jobs` 与 `/api/system/outbox`，权限分别为 `jobs.read`/`jobs.run` 与 `outbox.read`/`outbox.publish`/`outbox.replay`。

## 一致性约定

SQLite 连接启用外键、WAL、忙等待和即时写事务。资源档案、库位、容器摆放、检测任务和发放申请采用版本号防止旧请求覆盖新状态；入库、移库、取样和传感读数使用业务键去重。活力检测保留采用的规程版本和每个重复的观察计数，完成后可依据作物及风险策略生成下一次复检日期。会话令牌只保存摘要，审计记录不保存明文密码或令牌。认证依赖对会话的触达更新独立提交，保证后续业务事务真实落盘；后台作业凭租约令牌执行，事件箱以事件键去重并按游标确认，重启后未完成的步骤可依据 `job_attempts` 与 `outbox_deliveries` 判断并安全续跑。
