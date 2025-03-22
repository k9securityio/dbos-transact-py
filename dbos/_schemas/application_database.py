from typing import Type

from sqlalchemy import (
    BigInteger,
    Column,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    UnicodeText,
    text,
)

from ._mysql import Expressions


class ApplicationSchema:
    schema = "dbos"
    metadata_obj: MetaData
    transaction_outputs: Table


def configure_application_schema_mysql(db_schema_name: str) -> Type[ApplicationSchema]:
    ApplicationSchema.metadata_obj = metadata_obj = MetaData(schema=db_schema_name)

    ApplicationSchema.transaction_outputs = Table(
        "transaction_outputs",
        metadata_obj,
        Column("workflow_uuid", String(46)),
        Column("function_id", Integer),
        Column("output", UnicodeText, nullable=True),
        Column("error", UnicodeText, nullable=True),
        Column("txn_id", String(128), nullable=True),
        Column("txn_snapshot", UnicodeText),
        Column("executor_id", String(128), nullable=True),
        Column(
            "created_at",
            BigInteger,
            nullable=False,
            server_default=text(Expressions.epoch_time_millis_biginteger),
        ),
        Index("transaction_outputs_created_at_index", "created_at"),
        PrimaryKeyConstraint("workflow_uuid", "function_id"),
    )
    return ApplicationSchema
