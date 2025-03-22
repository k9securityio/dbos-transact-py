from typing import Type

from sqlalchemy import (
    BigInteger,
    Column,
    ForeignKey,
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

_col_len_workflow_uuid = 100  # len(uuid) + delimiter + up to a billion children
_col_type_workflow_uuid = String(_col_len_workflow_uuid)


class SystemSchema:
    # System table schema
    metadata_obj: MetaData
    sysdb_suffix: str = "_dbos_sys"

    workflow_status: Table
    operation_outputs: Table
    workflow_inputs: Table
    notifications: Table
    workflow_events: Table
    scheduler_state: Table
    workflow_queue: Table


def configure_system_schema_mysql(db_schema_name: str) -> Type[SystemSchema]:
    SystemSchema.metadata_obj = metadata_obj = MetaData(schema=db_schema_name)

    SystemSchema.workflow_status = Table(
        "workflow_status",
        metadata_obj,
        Column("workflow_uuid", _col_type_workflow_uuid, primary_key=True),
        Column("status", String(20), nullable=True),
        Column("name", String(128), nullable=True),
        Column("authenticated_user", String(32), nullable=True),
        Column("assumed_role", String(32), nullable=True),
        Column("authenticated_roles", String(128), nullable=True),
        Column("request", UnicodeText(), nullable=True),
        Column("output", UnicodeText(), nullable=True),
        Column("error", UnicodeText(), nullable=True),
        Column("executor_id", String(128), nullable=True),
        Column(
            "created_at",
            BigInteger,
            nullable=False,
            server_default=text(Expressions.epoch_time_millis_biginteger),
        ),
        Column(
            "updated_at",
            BigInteger,
            nullable=False,
            server_default=text(Expressions.epoch_time_millis_biginteger),
        ),
        Column("application_version", String(128), nullable=True),
        Column("application_id", String(128), nullable=True),
        Column("class_name", String(255), nullable=True, server_default=text("NULL")),
        Column("config_name", String(255), nullable=True, server_default=text("NULL")),
        Column(
            "recovery_attempts",
            BigInteger,
            nullable=True,
            server_default=text("0"),
        ),
        Column("queue_name", String(128)),
        Index("workflow_status_created_at_index", "created_at"),
        Index("workflow_status_executor_id_index", "executor_id"),
    )

    SystemSchema.operation_outputs = Table(
        "operation_outputs",
        metadata_obj,
        Column(
            "workflow_uuid",
            _col_type_workflow_uuid,
            ForeignKey(
                "workflow_status.workflow_uuid", onupdate="CASCADE", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        Column("function_id", Integer, nullable=False),
        Column("output", UnicodeText, nullable=True),
        Column("error", UnicodeText, nullable=True),
        PrimaryKeyConstraint("workflow_uuid", "function_id"),
    )

    SystemSchema.workflow_inputs = Table(
        "workflow_inputs",
        metadata_obj,
        Column(
            "workflow_uuid",
            _col_type_workflow_uuid,
            ForeignKey(
                "workflow_status.workflow_uuid", onupdate="CASCADE", ondelete="CASCADE"
            ),
            primary_key=True,
            nullable=False,
        ),
        Column("inputs", UnicodeText, nullable=False),
    )

    SystemSchema.notifications = Table(
        "notifications",
        metadata_obj,
        Column(
            "destination_uuid",
            String(36),
            ForeignKey(
                "workflow_status.workflow_uuid", onupdate="CASCADE", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        Column("topic", String(128), nullable=True),
        Column("message", UnicodeText, nullable=False),
        Column(
            "created_at_epoch_ms",
            BigInteger,
            nullable=False,
            server_default=text(Expressions.epoch_time_millis_biginteger),
        ),
        Column(
            "message_uuid",
            String(36),
            nullable=False,
            server_default=text(Expressions.generate_uuid_string),
        ),
        Index("idx_workflow_topic", "destination_uuid", "topic"),
    )

    SystemSchema.workflow_events = Table(
        "workflow_events",
        metadata_obj,
        Column(
            "workflow_uuid",
            _col_type_workflow_uuid,
            ForeignKey(
                "workflow_status.workflow_uuid", onupdate="CASCADE", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        Column("key", String(128), nullable=False),
        Column("value", UnicodeText, nullable=False),
        PrimaryKeyConstraint("workflow_uuid", "key"),
    )

    SystemSchema.scheduler_state = Table(
        "scheduler_state",
        metadata_obj,
        Column("workflow_fn_name", String(255), primary_key=True, nullable=False),
        Column("last_run_time", BigInteger, nullable=False),
    )

    SystemSchema.workflow_queue = Table(
        "workflow_queue",
        metadata_obj,
        Column(
            "workflow_uuid",
            _col_type_workflow_uuid,
            ForeignKey(
                "workflow_status.workflow_uuid", onupdate="CASCADE", ondelete="CASCADE"
            ),
            nullable=False,
            primary_key=True,
        ),
        Column("executor_id", String(128)),
        Column("queue_name", String(128), nullable=False),
        Column(
            "created_at_epoch_ms",
            BigInteger,
            nullable=False,
            server_default=text(Expressions.epoch_time_millis_biginteger),
        ),
        Column(
            "started_at_epoch_ms",
            BigInteger(),
        ),
        Column(
            "completed_at_epoch_ms",
            BigInteger(),
        ),
    )

    return SystemSchema
