from typing import Optional, TypedDict

import sqlalchemy as sa
import sqlalchemy.dialects.mysql as mysql
import sqlalchemy.dialects.postgresql as pg
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from ._dbos_config import ConfigFile
from ._error import DBOSWorkflowConflictIDError
from ._logger import dbos_logger
from ._schemas.application_database import ApplicationSchema


class TransactionResultInternal(TypedDict):
    workflow_uuid: str
    function_id: int
    output: Optional[str]  # JSON (jsonpickle)
    error: Optional[str]  # JSON (jsonpickle)
    txn_id: Optional[str]
    txn_snapshot: str
    executor_id: Optional[str]


class RecordedResult(TypedDict):
    output: Optional[str]  # JSON (jsonpickle)
    error: Optional[str]  # JSON (jsonpickle)


class ApplicationDatabase:

    def __init__(self, config: ConfigFile):
        self.config = config
        self.db_type = config["database"]["type"]

        app_db_name = config["database"]["app_db_name"]
        if "postgresql" == self.db_type:
            # If the application database does not already exist, create it
            postgres_db_url = sa.URL.create(
                "postgresql+psycopg",
                username=config["database"]["username"],
                password=config["database"]["password"],
                host=config["database"]["hostname"],
                port=config["database"]["port"],
                database="postgres",
            )
            postgres_db_engine = sa.create_engine(postgres_db_url)
            with postgres_db_engine.connect() as conn:
                conn.execution_options(isolation_level="AUTOCOMMIT")
                if not conn.execute(
                    sa.text("SELECT 1 FROM pg_database WHERE datname=:db_name"),
                    parameters={"db_name": app_db_name},
                ).scalar():
                    conn.execute(sa.text(f"CREATE DATABASE {app_db_name}"))
            postgres_db_engine.dispose()

            # Create a connection pool for the application database
            app_db_url = sa.URL.create(
                "postgresql+psycopg",
                username=config["database"]["username"],
                password=config["database"]["password"],
                host=config["database"]["hostname"],
                port=config["database"]["port"],
                database=app_db_name,
            )
        elif "mysql" == self.db_type:
            db_url_args = {
                "drivername": "mysql+pymysql",
                "username": config["database"]["username"],
                "password": config["database"]["password"],
                "host": config["database"]["hostname"],
                "port": config["database"]["port"],
            }
            mysql_db_url = sa.URL.create(**db_url_args)
            engine = sa.create_engine(mysql_db_url)
            with engine.connect() as conn:
                conn.execution_options(isolation_level="AUTOCOMMIT")
                conn.execute(
                    sa.text(
                        f"""
                                CREATE DATABASE IF NOT EXISTS `{app_db_name}`
                                CHARACTER SET utf8mb4
                                COLLATE utf8mb4_bin ;
                            """
                    )
                )
                dbos_logger.info(f"application database exists: {app_db_name}")
            engine.dispose()

            db_url_args["database"] = app_db_name
            app_db_url = sa.URL.create(**db_url_args)
        else:
            raise RuntimeError(
                f"unsupported database type: {config['database']['type']}"
            )

        self.engine = sa.create_engine(
            app_db_url, pool_size=20, max_overflow=5, pool_timeout=30
        )
        self.sessionmaker = sessionmaker(bind=self.engine)

        # Create the dbos schema and transaction_outputs table in the application database
        with self.engine.begin() as conn:
            schema_creation_query = sa.text(
                f"CREATE SCHEMA IF NOT EXISTS {ApplicationSchema.schema}"
            )
            conn.execute(schema_creation_query)
        ApplicationSchema.metadata_obj.create_all(self.engine)

    def destroy(self) -> None:
        self.engine.dispose()

    def _raise_unsupported_db_type(self):
        raise RuntimeError(
            f"unsupported database type: {self.db_type} (configured: {self.config['database']['type']})"
        )

    def record_transaction_output(
        self, session: Session, output: TransactionResultInternal
    ) -> None:
        if "postgresql" == self.db_type:
            self._record_transaction_output_pg(session, output)
        elif "mysql" == self.db_type:
            self._record_transaction_output_mysql(session, output)
        else:
            self._raise_unsupported_db_type()

    def _record_transaction_output_pg(self, session, output):
        try:
            session.execute(
                pg.insert(ApplicationSchema.transaction_outputs).values(
                    workflow_uuid=output["workflow_uuid"],
                    function_id=output["function_id"],
                    output=output["output"],
                    error=None,
                    txn_id=sa.text("(select pg_current_xact_id_if_assigned()::text)"),
                    txn_snapshot=output["txn_snapshot"],
                    executor_id=(
                        output["executor_id"] if output["executor_id"] else None
                    ),
                )
            )
        except DBAPIError as dbapi_error:
            if dbapi_error.orig.sqlstate == "23505":  # type: ignore
                raise DBOSWorkflowConflictIDError(output["workflow_uuid"])
            raise

    def _record_transaction_output_mysql(self, session, output):
        try:
            session.execute(
                mysql.insert(ApplicationSchema.transaction_outputs).values(
                    workflow_uuid=output["workflow_uuid"],
                    function_id=output["function_id"],
                    output=output["output"],
                    error=None,
                    txn_id=sa.text(
                        "(SELECT TRX_ID FROM INFORMATION_SCHEMA.INNODB_TRX WHERE TRX_MYSQL_THREAD_ID = CONNECTION_ID())"
                    ),
                    txn_snapshot=output["txn_snapshot"],
                    executor_id=(
                        output["executor_id"] if output["executor_id"] else None
                    ),
                )
            )
        except DBAPIError as dbapi_error:
            if dbapi_error.orig.sqlstate == "23505":  # type: ignore
                raise DBOSWorkflowConflictIDError(output["workflow_uuid"])
            raise

    def record_transaction_error(self, output: TransactionResultInternal) -> None:
        if "postgresql" == self.db_type:
            self._record_transaction_error_pg(output)
        elif "mysql" == self.db_type:
            self._record_transaction_error_mysql(output)
        else:
            self._raise_unsupported_db_type()

    def _record_transaction_error_pg(self, output):
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    pg.insert(ApplicationSchema.transaction_outputs).values(
                        workflow_uuid=output["workflow_uuid"],
                        function_id=output["function_id"],
                        output=None,
                        error=output["error"],
                        txn_id=sa.text(
                            "(select pg_current_xact_id_if_assigned()::text)"
                        ),
                        txn_snapshot=output["txn_snapshot"],
                        executor_id=(
                            output["executor_id"] if output["executor_id"] else None
                        ),
                    )
                )
        except DBAPIError as dbapi_error:
            if dbapi_error.orig.sqlstate == "23505":  # type: ignore
                raise DBOSWorkflowConflictIDError(output["workflow_uuid"])
            raise

    def _record_transaction_error_mysql(self, output):
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    pg.insert(ApplicationSchema.transaction_outputs).values(
                        workflow_uuid=output["workflow_uuid"],
                        function_id=output["function_id"],
                        output=None,
                        error=output["error"],
                        txn_id=sa.text(
                            "(SELECT TRX_ID FROM INFORMATION_SCHEMA.INNODB_TRX WHERE TRX_MYSQL_THREAD_ID = CONNECTION_ID())"
                        ),
                        txn_snapshot=output["txn_snapshot"],
                        executor_id=(
                            output["executor_id"] if output["executor_id"] else None
                        ),
                    )
                )
        except DBAPIError as dbapi_error:
            if dbapi_error.orig.sqlstate == "23505":  # type: ignore
                raise DBOSWorkflowConflictIDError(output["workflow_uuid"])
            raise

    @staticmethod
    def check_transaction_execution(
        session: Session, workflow_uuid: str, function_id: int
    ) -> Optional[RecordedResult]:
        rows = session.execute(
            sa.select(
                ApplicationSchema.transaction_outputs.c.output,
                ApplicationSchema.transaction_outputs.c.error,
            ).where(
                ApplicationSchema.transaction_outputs.c.workflow_uuid == workflow_uuid,
                ApplicationSchema.transaction_outputs.c.function_id == function_id,
            )
        ).all()
        if len(rows) == 0:
            return None
        result: RecordedResult = {
            "output": rows[0][0],
            "error": rows[0][1],
        }
        return result
