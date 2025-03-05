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
    Text,
    UnicodeText,
    text,
)

from ._mysql import Expressions

_col_len_workflow_uuid = 100  # len(uuid) + delimiter + up to a billion children
_col_type_workflow_uuid = String(_col_len_workflow_uuid)


class SystemSchema:
    ### System table schema
    metadata_obj = MetaData(schema="dbos")
    sysdb_suffix = "_dbos_sys"

    workflow_status = Table(
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

    operation_outputs = Table(
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
        Column("output", Text, nullable=True),
        Column("error", Text, nullable=True),
        PrimaryKeyConstraint("workflow_uuid", "function_id"),
    )

    workflow_inputs = Table(
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
        Column("inputs", Text, nullable=False),
    )

    notifications = Table(
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
        Column("message", Text, nullable=False),
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

    workflow_events = Table(
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
        Column("value", Text, nullable=False),
        PrimaryKeyConstraint("workflow_uuid", "key"),
    )

    scheduler_state = Table(
        "scheduler_state",
        metadata_obj,
        Column("workflow_fn_name", String(255), primary_key=True, nullable=False),
        Column("last_run_time", BigInteger, nullable=False),
    )

    workflow_queue = Table(
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
