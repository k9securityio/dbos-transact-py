from dbos._sys_db import SystemDatabase


def test_admin_workflow_resume(sys_db_mysql: SystemDatabase) -> None:
    sys_db = sys_db_mysql
    print(sys_db.engine)
    assert sys_db.engine is not None
