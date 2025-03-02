import time
import uuid

import sqlalchemy as sa

# noinspection PyProtectedMember
from dbos import DBOS, SetWorkflowID, _workflow_commands

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
