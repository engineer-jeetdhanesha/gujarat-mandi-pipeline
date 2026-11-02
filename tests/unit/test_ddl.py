"""Tests for lib_mandi.ddl.grant__volume_access.

Local Spark has no Unity Catalog, so GRANT can't run here: a fake session records the SQL instead.
"""

from lib_mandi.ddl import grant__volume_access


class FakeSpark:
    """Records every SQL statement; raises on the statement number given in fail_on (1-based)."""

    def __init__(self, fail_on: int = 0) -> None:
        self.statements: list[str] = []
        self.fail_on = fail_on

    def sql(self, statement: str) -> None:
        self.statements.append(statement)
        if len(self.statements) == self.fail_on:
            raise RuntimeError("PERMISSION_DENIED")


def test__grant_volume_access_grants_use_schema_and_read_write_on_the_volume() -> None:
    spark = FakeSpark()

    ok = grant__volume_access(spark, "cat", "bronze", "landing_zone", "user@example.com")

    assert ok is True
    assert spark.statements == [
        "GRANT USE SCHEMA ON SCHEMA cat.bronze TO `user@example.com`",
        "GRANT READ VOLUME, WRITE VOLUME ON VOLUME cat.bronze.landing_zone TO `user@example.com`",
    ]


def test__grant_volume_access_returns_false_when_a_grant_fails() -> None:
    spark = FakeSpark(fail_on=2)

    ok = grant__volume_access(spark, "cat", "bronze", "archive", "user@example.com")

    assert ok is False
