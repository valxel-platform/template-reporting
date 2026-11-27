"""Signing in to the store: the component's own login, as its workload identity (constitution VX-20)."""

from __future__ import annotations

from psycopg.conninfo import make_conninfo

from .settings import Settings

GCP_SCOPE = "https://www.googleapis.com/auth/sqlservice.login"
AZURE_SCOPE = "https://ossrdbms-aad.database.windows.net/.default"


class SignInError(RuntimeError):
    pass


def token(auth: str) -> str:
    """A short-lived access token for the pod's own workload identity: its database password."""
    if auth == "gcp-iam":
        import google.auth
        import google.auth.transport.requests

        creds, _ = google.auth.default(scopes=[GCP_SCOPE])
        creds.refresh(google.auth.transport.requests.Request())  # type: ignore[no-untyped-call]
        return str(creds.token)
    if auth == "azure-entra":
        from azure.identity import DefaultAzureCredential

        return str(DefaultAzureCredential().get_token(AZURE_SCOPE).token)
    raise SignInError(f"REPORTING_PG_AUTH: {auth!r} is not gcp-iam, azure-entra or password")


def database_url(s: Settings) -> str:
    """The bound parts when there are any (a token for a password), else REPORTING_DATABASE_URL."""
    if not s.pg_host:
        return s.database_url
    if not (s.pg_user and s.pg_database):
        raise SignInError("REPORTING_PG_HOST is set: REPORTING_PG_USER and _DATABASE are needed too")
    if s.pg_auth == "password":
        if not s.pg_password:
            raise SignInError("REPORTING_PG_AUTH is password: REPORTING_PG_PASSWORD is needed (local only)")
        secret = s.pg_password
    else:
        secret = token(s.pg_auth)
    return make_conninfo(
        host=s.pg_host, port=s.pg_port, dbname=s.pg_database, user=s.pg_user, password=secret, sslmode=s.pg_sslmode
    )
