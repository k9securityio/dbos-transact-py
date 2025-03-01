from typing import Final


class Expressions:
    epoch_time_millis_biginteger: Final[str] = "(UNIX_TIMESTAMP(NOW(3)) * 1000)"
