# Public API
import time

# Private API just for testing
# noinspection PyProtectedMember
from dbos import DBOS, _workflow_commands

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
    # time.sleep(1)
    #
    # # get the workflow list
    # output = _workflow_commands.list_workflows(sys_db)
    # assert len(output) == 1, f"Expected list length to be 1, but got {len(output)}"
    #
    # assert output[0] is not None, "Expected output to be not None"
