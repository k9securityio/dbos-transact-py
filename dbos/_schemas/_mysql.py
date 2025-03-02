from typing import Final


class Expressions:
    epoch_time_millis_biginteger: Final[str] = "(UNIX_TIMESTAMP(NOW(3)) * 1000)"
    generate_uuid_string: Final[str] = "(UUID())"
    get_current_txid_string: Final[str] = (
        "(SELECT TRX_ID FROM INFORMATION_SCHEMA.INNODB_TRX WHERE TRX_MYSQL_THREAD_ID = CONNECTION_ID())"
    )
