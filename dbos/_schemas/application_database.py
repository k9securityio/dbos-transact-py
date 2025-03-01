from sqlalchemy import (
    BigInteger,
    Column,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    text,
)

from ._mysql import Expressions


class ApplicationSchema:
    schema = "dbos"
    metadata_obj = MetaData(schema=schema)

    transaction_outputs = Table(
        "transaction_outputs",
        metadata_obj,
        Column("workflow_uuid", String(36)),
        Column("function_id", Integer),
        Column("output", Text, nullable=True),
        Column("error", Text, nullable=True),
        Column("txn_id", String(128), nullable=True),
        Column("txn_snapshot", Text),
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
