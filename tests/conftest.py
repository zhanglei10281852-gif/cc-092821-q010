from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

T0 = datetime(2026, 10, 1, 2, 0, tzinfo=UTC)  # 夜间批处理时刻


@pytest.fixture()
def client(tmp_path: Path):
    os.environ["GERMPLASM_DATABASE_PATH"] = str(tmp_path / "test.db")
    from app.database import close_connection
    close_connection()
    from app.main import app
    with TestClient(app) as test_client:
        yield test_client
    close_connection()


@pytest.fixture()
def admin(client: TestClient) -> dict:
    response = client.post("/api/auth/bootstrap", json={"username": "admin", "password": "Admin!23456", "client_label": "tests"})
    assert response.status_code == 201, response.text
    login = client.post("/api/auth/login", json={"username": "admin", "password": "Admin!23456", "client_label": "tests"})
    assert login.status_code == 200, login.text
    return {"token": login.json()["token"], "headers": {"Authorization": f"Bearer {login.json()['token']}"}}


def make_pending_schedule(connection, clock, suffix: str, due_on: str = "2026-10-05") -> tuple[dict, int]:
    """登记一个批次并直接落库一条到期复检日程。"""
    from app.core.clock import to_storage
    from app.germplasm.service import GermplasmService

    service = GermplasmService(connection, clock)
    source = service.accessions.create_source({
        "source_code": f"SRC-J-{suffix}", "provider_name": "夜间采集队", "country_code": "CN",
        "locality": "北站", "collected_on": "2025-09-01", "permit_reference": None, "restrictions": {},
    })
    accession = service.accessions.create_accession({
        "accession_no": f"ACC-J-{suffix}", "scientific_name": "Oryza sativa", "crop_name": "水稻",
        "cultivar_name": "夜班材料", "source_id": source["id"], "acquisition_type": "采集",
        "received_on": "2025-09-02", "passport": {}, "created_by": "测试",
    })
    accepted = service.accessions.transition(accession["id"], {
        "target_status": "accepted", "reason": "资料齐全", "expected_version": 1, "actor": "测试",
    })
    lot = service.inventory.create_lot({
        "lot_no": f"LOT-J-{suffix}", "accession_id": accepted["id"], "parent_lot_id": None,
        "harvest_year": 2025, "initial_weight_grams": 300, "moisture_percent": 7.0,
        "treatment": "干燥", "sealed_on": "2025-09-03", "created_by": "测试",
    })
    policy = service.viability.create_policy({
        "crop_name": "水稻", "risk_level": "medium", "interval_months": 12, "warning_days": 30,
        "minimum_germination_percent": 75, "effective_from": "2025-01-01", "effective_to": None,
        "created_by": "测试",
    })
    timestamp = to_storage(clock.now())
    cursor = connection.execute(
        "INSERT INTO retest_schedules(lot_id,source_test_id,policy_id,due_on,status,reason,created_at,updated_at) "
        "VALUES(?,NULL,?,?,'pending','周期复检到期',?,?)",
        (lot["id"], policy["id"], due_on, timestamp, timestamp),
    )
    # 建档过程产生的领域事件不参与本测试的投递断言
    connection.execute("DELETE FROM outbox_events")
    return lot, int(cursor.lastrowid)


def schedule_status(connection, schedule_id: int) -> str:
    return connection.execute("SELECT status FROM retest_schedules WHERE id=?", (schedule_id,)).fetchone()[0]
