"""Signing in: the bound parts with a token from the workload identity, or a URL locally."""

from __future__ import annotations

import pytest
from psycopg.conninfo import conninfo_to_dict

from reporting import connect
from reporting.settings import Settings


def test_in_environments_the_password_is_a_token_from_the_workload_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(connect, "token", lambda auth: f"token-for-{auth}")
    url = connect.database_url(Settings(pg_host="db", pg_database="emr", pg_user="sa@p.iam", pg_auth="gcp-iam"))
    assert conninfo_to_dict(url) == {
        "host": "db",
        "port": "5432",
        "dbname": "emr",
        "user": "sa@p.iam",
        "password": "token-for-gcp-iam",
        "sslmode": "require",
    }


def test_locally_a_url_or_a_password() -> None:
    assert connect.database_url(Settings(database_url="postgresql://x@y/z")) == "postgresql://x@y/z"
    url = connect.database_url(Settings(pg_host="db", pg_database="emr", pg_user="u", pg_password="pw"))
    assert conninfo_to_dict(url)["password"] == "pw"
    with pytest.raises(connect.SignInError, match="_PASSWORD"):
        connect.database_url(Settings(pg_host="db", pg_database="emr", pg_user="u"))
    with pytest.raises(connect.SignInError, match="_USER"):
        connect.database_url(Settings(pg_host="db"))
    with pytest.raises(connect.SignInError, match="gcp-iam, azure-entra or password"):
        connect.token("kerberos")
