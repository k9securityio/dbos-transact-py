import time
import uuid

import pytest
import sqlalchemy as sa

# noinspection PyProtectedMember
from dbos import DBOS, SetWorkflowID, _workflow_commands
from dbos._schemas.system_database import SystemSchema

# noinspection PyProtectedMember
from dbos._sys_db import SystemDatabase


def test_simple_workflow(dbos_mysql: DBOS, sys_db_mysql: SystemDatabase) -> None:
    sys_db = sys_db_mysql
    print(sys_db.engine)
    assert sys_db.engine is not None

    @DBOS.workflow()
    def simple_workflow() -> None:
        print("Executed Simple workflow")
        return

    # run the workflow
    simple_workflow()
    time.sleep(1)

    # get the workflow list
    output = _workflow_commands.list_workflows(sys_db)
    assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"

    assert output[0] is not None, "Expected output to be not None"


def test_dbos_simple_workflow(dbos_mysql: DBOS) -> None:
    # copied from test_debos.py::test_simple_workflow

    txn_counter: int = 0
    wf_counter: int = 0
    step_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> str:
        DBOS.logger.info("start test_workflow")
        nonlocal wf_counter
        wf_counter += 1
        res = test_transaction(var2)
        DBOS.logger.info(f"test_transaction res: {res}")
        res2 = test_step(var)
        DBOS.logger.info(f"test_step res2: {res2}")
        DBOS.logger.info("I'm test_workflow")
        DBOS.logger.info("end test_workflow")
        return res + res2

    @DBOS.transaction(isolation_level="REPEATABLE READ")
    def test_transaction(var2: str) -> str:
        DBOS.logger.info("start test_transaction")
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        DBOS.logger.info("I'm test_transaction")
        DBOS.logger.info("end test_transaction")
        return var2 + str(rows[0][0])

    @DBOS.step()
    def test_step(var: str) -> str:
        DBOS.logger.info("start test_step")
        nonlocal step_counter
        step_counter += 1
        DBOS.logger.info("I'm test_step")
        DBOS.logger.info("end test_step")
        return var

    assert test_workflow("bob", "bob") == "bob1bob"

    # Test OAOO
    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        assert test_workflow("alice", "alice") == "alice1alice"
    with SetWorkflowID(wfuuid):
        assert test_workflow("alice", "alice") == "alice1alice"
    assert txn_counter == 2  # Only increment once
    assert step_counter == 2  # Only increment once

    # Test we can execute the workflow by uuid
    handle = DBOS.execute_workflow_id(wfuuid)
    assert handle.get_result() == "alice1alice"
    assert wf_counter == 4


@pytest.mark.skip(
    reason="Skipping this test because while recovery_attempts is being incremented,"
    " it doesn't seem to be visible here."
    " This test will be re-enabled once the issue is resolved."
)
def test_simple_workflow_attempts_counter(dbos_mysql: DBOS) -> None:

    @DBOS.workflow()
    def noop() -> None:
        DBOS.logger.info(f"Executing noop {dbos_mysql.workflow_id}")
        pass

    wfuuid = str(uuid.uuid4())
    DBOS.logger.info(f"Workflow id: {wfuuid}")
    with dbos_mysql._sys_db.engine.connect() as c:
        stmt = sa.select(
            SystemSchema.workflow_status.c.recovery_attempts,
            SystemSchema.workflow_status.c.created_at,
            SystemSchema.workflow_status.c.updated_at,
        ).where(SystemSchema.workflow_status.c.workflow_uuid == wfuuid)
        for i in range(10):
            with SetWorkflowID(wfuuid):
                noop()
            txn_id_stmt = sa.text(
                "SELECT TRX_ID FROM INFORMATION_SCHEMA.INNODB_TRX WHERE TRX_MYSQL_THREAD_ID = CONNECTION_ID()"
            )
            txn_id_result = c.execute(txn_id_stmt).fetchone()
            txn_id = txn_id_result[0] if txn_id_result else None
            DBOS.logger.info(f"Transaction id: {txn_id}")

            result = c.execute(stmt).fetchone()
            assert result is not None
            recovery_attempts, created_at, updated_at = result
            assert recovery_attempts == i + 1
            if i == 0:
                assert created_at == updated_at
            else:
                assert updated_at > created_at


def test_child_workflow(dbos_mysql: DBOS) -> None:
    # copied from test_dbos::test_child_workflow
    dbos: DBOS = dbos_mysql

    txn_counter: int = 0
    wf_counter: int = 0
    step_counter: int = 0

    @DBOS.transaction()
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        DBOS.logger.info("I'm test_transaction")
        return var2 + str(rows[0][0])

    @DBOS.step()
    def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        DBOS.logger.info("I'm test_step")
        return var

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> str:
        DBOS.logger.info("I'm test_workflow")
        if len(DBOS.parent_workflow_id):
            DBOS.logger.info("  This is a child test_workflow")
            # Note this assertion is only true if child wasn't assigned an ID explicitly
            assert DBOS.workflow_id.startswith(DBOS.parent_workflow_id)
        nonlocal wf_counter
        wf_counter += 1
        res = test_transaction(var2)
        res2 = test_step(var)
        return res + res2

    @DBOS.workflow()
    def test_workflow_child() -> str:
        nonlocal wf_counter
        wf_counter += 1
        res1 = test_workflow("child1", "child1")
        return res1

    wf_ac_counter: int = 0
    txn_ac_counter: int = 0

    @DBOS.workflow()
    def test_workflow_children() -> str:
        nonlocal wf_counter
        wf_counter += 1
        res1 = test_workflow("child1", "child1")
        wfh1 = dbos.start_workflow(test_workflow, "child2a", "child2a")
        wfh2 = dbos.start_workflow(test_workflow, "child2b", "child2b")
        res2 = wfh1.get_result()
        res3 = wfh2.get_result()
        return res1 + res2 + res3

    @DBOS.transaction()
    def test_transaction_ac(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_ac_counter
        txn_ac_counter += 1
        return var2 + str(rows[0][0])

    @DBOS.workflow()
    def test_workflow_ac(var: str, var2: str) -> str:
        DBOS.logger.info("I'm test_workflow assigned child id")
        assert DBOS.workflow_id == "run_me_just_once"
        res = test_transaction_ac(var2)
        return var + res

    @DBOS.workflow()
    def test_workflow_assignchild() -> str:
        nonlocal wf_ac_counter
        wf_ac_counter += 1
        with SetWorkflowID("run_me_just_once"):
            res1 = test_workflow_ac("child1", "child1")
        with SetWorkflowID("run_me_just_once"):
            wfh = dbos.start_workflow(test_workflow_ac, "child1", "child1")
            res2 = wfh.get_result()
        return res1 + res2

    # Test child wf
    assert test_workflow_child() == "child11child1"
    assert test_workflow_children() == "child11child1child2a1child2achild2b1child2b"

    # Test child wf with assigned ID
    assert test_workflow_assignchild() == "child1child11child1child11"
    assert test_workflow_assignchild() == "child1child11child1child11"
    assert wf_ac_counter == 2
    assert txn_ac_counter == 1  # Only ran tx once
