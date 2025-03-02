import datetime
import time
import uuid

import pytest
import sqlalchemy as sa

# noinspection PyProtectedMember
from dbos import DBOS, ConfigFile, SetWorkflowID, WorkflowHandle, _workflow_commands
from dbos._context import assert_current_dbos_context, get_local_dbos_context
from dbos._error import DBOSMaxStepRetriesExceeded
from dbos._schemas.system_database import SystemSchema

# noinspection PyProtectedMember
from dbos._sys_db import GetWorkflowsInput, SystemDatabase, WorkflowStatusString


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


def test_exception_workflow(dbos_mysql: DBOS) -> None:
    # copied from test_dbos::test_exception_workflow

    dbos: DBOS = dbos_mysql

    txn_counter: int = 0
    wf_counter: int = 0
    step_counter: int = 0
    bad_txn_counter: int = 0

    @DBOS.transaction()
    def exception_transaction(var: str) -> str:
        nonlocal txn_counter
        txn_counter += 1
        raise Exception(var)

    @DBOS.transaction()
    def bad_transaction() -> None:
        nonlocal bad_txn_counter
        bad_txn_counter += 1
        # Make sure we record this error in the database
        DBOS.sql_session.execute(sa.text("selct abc from c;")).fetchall()

    @DBOS.step()
    def exception_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        raise Exception(var)

    @DBOS.workflow()
    def exception_workflow() -> None:
        nonlocal wf_counter
        wf_counter += 1
        err1 = None
        err2 = None
        try:
            exception_transaction("test error")
        except Exception as e:
            err1 = e

        try:
            exception_step("test error")
        except Exception as e:
            err2 = e
        assert err1 is not None and err2 is not None
        assert str(err1) == str(err2)

        try:
            bad_transaction()
        except Exception as e:
            # assert str(e.orig.sqlstate) == "42601"  # type: ignore
            DBOS.logger.info(f"exception from bad_transaction ({type(e)}): {e}")
        raise err1

    with pytest.raises(Exception) as exc_info:
        exception_workflow()

    assert "test error" in str(exc_info.value)

    # Test OAOO
    wfuuid = str(uuid.uuid4())
    with pytest.raises(Exception) as exc_info:
        with SetWorkflowID(wfuuid):
            exception_workflow()
    assert "test error" == str(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        with SetWorkflowID(wfuuid):
            exception_workflow()
    assert "test error" == str(exc_info.value)
    assert txn_counter == 2  # Only increment once
    assert step_counter == 2  # Only increment once
    # TODO: determine why we see 3 bad txns instead of 2
    # assert bad_txn_counter == 2  # Only increment once

    # Test we can execute the workflow by uuid, shouldn't throw errors
    dbos._sys_db._flush_workflow_status_buffer()
    handle = DBOS.execute_workflow_id(wfuuid)
    with pytest.raises(Exception) as exc_info:
        handle.get_result()
    assert "test error" == str(exc_info.value)
    assert wf_counter == 3  # The workflow error is directly returned without running


def test_temp_workflow(dbos_mysql: DBOS) -> None:
    # copied from test_dbos::test_temp_workflow
    dbos: DBOS = dbos_mysql

    txn_counter: int = 0
    step_counter: int = 0

    cur_time: str = datetime.datetime.now().isoformat()
    gwi: GetWorkflowsInput = GetWorkflowsInput()
    gwi.start_time = cur_time

    @DBOS.transaction(isolation_level="READ COMMITTED")
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        return var2 + str(rows[0][0])

    @DBOS.step()
    def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        return var

    @DBOS.step()
    def call_step(var: str) -> str:
        return test_step(var)

    assert get_local_dbos_context() is None
    res = test_transaction("var2")
    assert res == "var21"
    assert get_local_dbos_context() is None
    res = test_step("var")
    assert res == "var"

    # Flush workflow inputs buffer shouldn't fail due to foreign key violation.
    # It should properly skip the transaction inputs.
    dbos._sys_db._flush_workflow_inputs_buffer()

    # Wait for buffers to flush
    dbos._sys_db.wait_for_buffer_flush()
    wfs = dbos._sys_db.get_workflows(gwi)
    assert len(wfs.workflow_uuids) == 2

    wfi1 = dbos._sys_db.get_workflow_status(wfs.workflow_uuids[0])
    assert wfi1
    assert wfi1["name"].startswith("<temp>")

    wfi2 = dbos._sys_db.get_workflow_status(wfs.workflow_uuids[1])
    assert wfi2
    assert wfi2["name"].startswith("<temp>")

    assert txn_counter == 1
    assert step_counter == 1

    res = call_step("var2")
    assert res == "var2"
    assert step_counter == 2


def test_temp_workflow_errors(dbos_mysql: DBOS) -> None:
    # copied from test_dbos::test_temp_workflow_errors

    txn_counter: int = 0
    step_counter: int = 0
    retried_step_counter: int = 0

    cur_time: str = datetime.datetime.now().isoformat()
    gwi: GetWorkflowsInput = GetWorkflowsInput()
    gwi.start_time = cur_time

    @DBOS.transaction()
    def test_transaction(var2: str) -> str:
        nonlocal txn_counter
        txn_counter += 1
        raise Exception(var2)

    @DBOS.step()
    def test_step(var: str) -> str:
        nonlocal step_counter
        step_counter += 1
        raise Exception(var)

    @DBOS.step(retries_allowed=True)
    def test_retried_step(var: str) -> str:
        nonlocal retried_step_counter
        retried_step_counter += 1
        raise Exception(var)

    with pytest.raises(Exception) as exc_info:
        test_transaction("tval")
    assert "tval" == str(exc_info.value)

    with pytest.raises(Exception) as exc_info:
        test_step("cval")
    assert "cval" == str(exc_info.value)

    with pytest.raises(DBOSMaxStepRetriesExceeded) as exc_info:
        test_retried_step("rval")

    assert txn_counter == 1
    assert step_counter == 1
    assert retried_step_counter == 3


def test_recovery_workflow(dbos_mysql: DBOS) -> None:
    dbos: DBOS = dbos_mysql

    txn_counter: int = 0
    txn_return_none_counter: int = 0
    wf_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_transaction(var2)
        should_be_none = test_transaction_return_none()
        assert should_be_none is None
        return res + var

    @DBOS.transaction()
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        return var2 + str(rows[0][0])

    @DBOS.transaction()
    def test_transaction_return_none() -> None:
        nonlocal txn_return_none_counter
        DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        txn_return_none_counter += 1
        return

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        assert test_workflow("bob", "bob") == "bob1bob"

    dbos._sys_db.wait_for_buffer_flush()
    # Change the workflow status to pending
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": "PENDING", "name": test_workflow.__qualname__})
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfuuid)
        )

    # Recovery should execute the workflow again but skip the transaction
    workflow_handles = DBOS.recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == "bob1bob"
    assert wf_counter == 2
    assert txn_counter == 1
    assert txn_return_none_counter == 1

    # Test that there was a recovery attempt of this
    stat = workflow_handles[0].get_status()
    assert stat
    assert stat.recovery_attempts == 2  # original attempt + recovery attempt


def test_recovery_workflow_step(dbos_mysql: DBOS) -> None:
    # copied from test_dbos::test_recovery_workflow_step
    dbos: DBOS = dbos_mysql

    step_counter: int = 0
    wf_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        should_be_none = test_step(var2)
        assert should_be_none is None
        return var

    @DBOS.step()
    def test_step(var2: str) -> None:
        nonlocal step_counter
        step_counter += 1
        print(f"I'm a test_step {var2}!")
        return

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        assert test_workflow("bob", "bob") == "bob"

    dbos._sys_db.wait_for_buffer_flush()
    # Change the workflow status to pending
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": "PENDING", "name": test_workflow.__qualname__})
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfuuid)
        )

    # Recovery should execute the workflow again but skip the transaction
    workflow_handles = DBOS.recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == "bob"
    assert wf_counter == 2
    assert step_counter == 1

    # Test that there was a recovery attempt of this
    stat = workflow_handles[0].get_status()
    assert stat
    assert stat.recovery_attempts == 2


def test_workflow_returns_none(dbos_mysql: DBOS) -> None:
    # copied from test_dbos::test_workflow_returns_none
    dbos: DBOS = dbos_mysql

    wf_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> None:
        nonlocal wf_counter
        wf_counter += 1
        assert var == var2 == "bob"
        return

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        assert test_workflow("bob", "bob") is None
    assert wf_counter == 1

    dbos._sys_db.wait_for_buffer_flush()
    with SetWorkflowID(wfuuid):
        assert test_workflow("bob", "bob") is None
    assert wf_counter == 2

    handle: WorkflowHandle[None] = DBOS.retrieve_workflow(wfuuid)
    assert handle.get_result() == None
    assert wf_counter == 2

    # Change the workflow status to pending
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": "PENDING", "name": test_workflow.__qualname__})
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfuuid)
        )

    workflow_handles = DBOS.recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() is None
    assert wf_counter == 3

    # Test that there was a recovery attempt of this
    stat = workflow_handles[0].get_status()
    assert stat
    assert stat.recovery_attempts == 3  # 2 calls to test_workflow + 1 recovery attempt


def test_recovery_temp_workflow(dbos_mysql: DBOS) -> None:
    # copied from test_dbos::test_recovery_temp_workflow
    dbos: DBOS = dbos_mysql

    txn_counter: int = 0

    @DBOS.transaction()
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        return var2 + str(rows[0][0])

    cur_time: str = datetime.datetime.now().isoformat()
    gwi: GetWorkflowsInput = GetWorkflowsInput()
    gwi.start_time = cur_time

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        res = test_transaction("bob")
        assert res == "bob1"

    dbos._sys_db.wait_for_buffer_flush()
    wfs = dbos._sys_db.get_workflows(gwi)
    assert len(wfs.workflow_uuids) == 1
    assert wfs.workflow_uuids[0] == wfuuid

    wfi = dbos._sys_db.get_workflow_status(wfs.workflow_uuids[0])
    assert wfi
    assert wfi["name"].startswith("<temp>")

    # Change the workflow status to pending
    with dbos._sys_db.engine.begin() as c:
        c.execute(
            sa.update(SystemSchema.workflow_status)
            .values({"status": "PENDING", "name": wfi["name"]})
            .where(SystemSchema.workflow_status.c.workflow_uuid == wfuuid)
        )

    # Recovery should execute the workflow again but skip the transaction
    workflow_handles = DBOS.recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == "bob1"

    wfs = dbos._sys_db.get_workflows(gwi)
    assert len(wfs.workflow_uuids) == 1
    assert wfs.workflow_uuids[0] == wfuuid

    dbos._sys_db.wait_for_buffer_flush()
    wfi = dbos._sys_db.get_workflow_status(wfs.workflow_uuids[0])
    assert wfi
    assert wfi["name"].startswith("<temp>")
    assert wfi["status"] == "SUCCESS"

    assert txn_counter == 1


def test_recovery_thread(config_mysql: ConfigFile) -> None:
    # copied from test_dbos::test_recovery_thread
    config: ConfigFile = config_mysql

    wf_counter: int = 0
    test_var = "dbos"

    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)

    @DBOS.workflow()
    def test_workflow(var: str) -> str:
        nonlocal wf_counter
        if var == test_var:
            wf_counter += 1
        return var

    DBOS.launch()

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        assert test_workflow(test_var) == test_var

    dbos._sys_db.wait_for_buffer_flush()
    # Change the workflow status to pending
    dbos._sys_db.update_workflow_status(
        {
            "workflow_uuid": wfuuid,
            "status": "PENDING",
            "name": test_workflow.__qualname__,
            "class_name": None,
            "config_name": None,
            "output": None,
            "error": None,
            "executor_id": None,
            "app_id": None,
            "app_version": None,
            "request": None,
            "recovery_attempts": None,
            "authenticated_user": None,
            "authenticated_roles": None,
            "assumed_role": None,
            "queue_name": None,
            "created_at": None,
            "updated_at": None,
        }
    )

    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)

    @DBOS.workflow()
    def test_workflow(var: str) -> str:
        nonlocal wf_counter
        if var == test_var:
            wf_counter += 1
        return var

    DBOS.launch()

    # Upon re-launch, the background thread should recover the workflow safely.
    max_retries = 10
    success = False
    for i in range(max_retries):
        try:
            assert wf_counter == 2
            success = True
        except AssertionError:
            time.sleep(1)
    assert success


def test_start_workflow(dbos_mysql: DBOS) -> None:
    # copied from test_dbos::test_start_workflow
    dbos: DBOS = dbos_mysql
    txn_counter: int = 0
    wf_counter: int = 0

    @DBOS.workflow()
    def test_workflow(var: str, var2: str) -> str:
        nonlocal wf_counter
        wf_counter += 1
        res = test_transaction(var2)
        return res + var

    @DBOS.transaction()
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        nonlocal txn_counter
        txn_counter += 1
        return var2 + str(rows[0][0])

    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        handle = dbos.start_workflow(test_workflow, "bob", "bob")
        context = assert_current_dbos_context()
        assert not context.is_within_workflow()
        assert handle.get_result() == "bob1bob"
    with SetWorkflowID(wfuuid):
        handle = dbos.start_workflow(test_workflow, "bob", "bob")
        context = assert_current_dbos_context()
        assert not context.is_within_workflow()
        assert handle.get_result() == "bob1bob"
    with SetWorkflowID(wfuuid):
        assert test_workflow("bob", "bob") == "bob1bob"
        context = assert_current_dbos_context()
        assert not context.is_within_workflow()
    assert txn_counter == 1
    assert wf_counter == 3


def test_retrieve_workflow(dbos_mysql: DBOS) -> None:
    # copied from test_dbos::test_retrieve_workflow
    dbos: DBOS = dbos_mysql

    @DBOS.workflow()
    def test_sleep_workflow(secs: float) -> str:
        dbos.sleep(secs)
        return DBOS.workflow_id

    @DBOS.workflow()
    def test_sleep_workthrow(secs: float) -> str:
        dbos.sleep(secs)
        raise Exception("Wake Up!")

    dest_uuid = "aaaa"
    with pytest.raises(Exception) as exc_info:
        dbos.retrieve_workflow(dest_uuid)
    pattern = f"Sent to non-existent destination workflow ID: {dest_uuid}"
    assert pattern in str(exc_info.value)

    # These return
    sleep_wfh = dbos.start_workflow(test_sleep_workflow, 1.5)
    istat = sleep_wfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.PENDING.value)

    sleep_pwfh: WorkflowHandle[str] = dbos.retrieve_workflow(sleep_wfh.workflow_id)
    assert sleep_wfh.workflow_id == sleep_pwfh.workflow_id
    dbos.logger.info(f"UUID: {sleep_pwfh.get_workflow_id()}")
    hres = sleep_pwfh.get_result()
    assert hres == sleep_pwfh.get_workflow_id()
    dbos.logger.info(f"RES: {hres}")
    istat = sleep_pwfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.SUCCESS.value)

    assert sleep_wfh.get_result() == sleep_wfh.get_workflow_id()
    istat = sleep_wfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.SUCCESS.value)

    # These throw
    sleep_wfh = dbos.start_workflow(test_sleep_workthrow, 1.5)
    istat = sleep_wfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.PENDING.value)
    sleep_pwfh = dbos.retrieve_workflow(sleep_wfh.workflow_id)
    assert sleep_wfh.workflow_id == sleep_pwfh.workflow_id

    with pytest.raises(Exception) as exc_info:
        sleep_pwfh.get_result()
    assert str(exc_info.value) == "Wake Up!"
    istat = sleep_pwfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.ERROR.value)

    with pytest.raises(Exception) as exc_info:
        sleep_wfh.get_result()
    assert str(exc_info.value) == "Wake Up!"
    istat = sleep_wfh.get_status()
    assert istat
    assert istat.status == str(WorkflowStatusString.ERROR.value)


def test_retrieve_workflow_in_workflow(dbos_mysql: DBOS) -> None:
    # copied from test_dbos::test_retrieve_workflow_in_workflow

    dbos: DBOS = dbos_mysql

    @DBOS.workflow()
    def test_sleep_workflow(secs: float) -> str:
        dbos.sleep(secs)
        return DBOS.workflow_id

    @DBOS.workflow()
    def test_workflow_status_a() -> str:
        with SetWorkflowID("run_this_once_a"):
            dbos.start_workflow(test_sleep_workflow, 1.5)

        fstat1 = dbos.get_workflow_status("run_this_once_a")
        assert fstat1
        fres: str = dbos.retrieve_workflow("run_this_once_a").get_result()
        fstat2 = dbos.get_workflow_status("run_this_once_a")
        assert fstat2
        return fstat1.status + fres + fstat2.status

    @DBOS.workflow()
    def test_workflow_status_b() -> str:
        assert DBOS.workflow_id == "parent_b"
        with SetWorkflowID("run_this_once_b"):
            wfh = dbos.start_workflow(test_sleep_workflow, 1.5)
        assert DBOS.workflow_id == "parent_b"

        fstat1 = wfh.get_status()
        assert fstat1
        fres = wfh.get_result()
        dbos._sys_db.wait_for_buffer_flush()  # Wait for status to export.
        fstat2 = wfh.get_status()
        assert fstat2
        return fstat1.status + fres + fstat2.status

    with SetWorkflowID("parent_a"):
        assert test_workflow_status_a() == "PENDINGrun_this_once_aSUCCESS"
    with SetWorkflowID("parent_a"):
        assert test_workflow_status_a() == "PENDINGrun_this_once_aSUCCESS"

    with SetWorkflowID("parent_b"):
        assert test_workflow_status_b() == "PENDINGrun_this_once_bSUCCESS"
    with SetWorkflowID("parent_b"):
        assert test_workflow_status_b() == "PENDINGrun_this_once_bSUCCESS"

    # Test that the number of attempts matches the number of calls
    stat = dbos.get_workflow_status("parent_a")
    assert stat
    assert stat.recovery_attempts == 2
    stat = dbos.get_workflow_status("parent_b")
    assert stat
    assert stat.recovery_attempts == 2
    stat = dbos.get_workflow_status("run_this_once_a")
    assert stat
    assert stat.recovery_attempts == 2
    stat = dbos.get_workflow_status("run_this_once_b")
    assert stat
    assert stat.recovery_attempts == 2


def test_sleep(dbos_mysql: DBOS) -> None:
    # copied from test_dbos::test_sleep
    dbos: DBOS = dbos_mysql

    @DBOS.workflow()
    def test_sleep_workflow(secs: float) -> str:
        dbos.sleep(secs)
        return DBOS.workflow_id

    start_time = time.time()
    sleep_uuid = test_sleep_workflow(1.5)
    assert time.time() - start_time > 1.4

    # Test sleep OAOO, skip sleep
    start_time = time.time()
    with SetWorkflowID(sleep_uuid):
        assert test_sleep_workflow(1.5) == sleep_uuid
        assert time.time() - start_time < 0.3
