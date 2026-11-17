"""Error reports to Sentry (or GlitchTip, which speaks its protocol).

With SENTRY_DSN set, unhandled exceptions and ERROR logs (an event the sink can't apply, a store
that keeps failing) go to Sentry, grouped into issues, and Sentry's
issue alerts reach the SDLC tool as incident signals. An event keeps the exception type, its stack,
the log message's pattern, a `trace_id` tag and a `component` tag (component.py,
so the SDLC tool routes the issue here); never exception messages, formatted messages or their
arguments, local variables, users or breadcrumbs: any of them can carry PHI or a token. Unset,
Sentry is off. Also ENVIRONMENT and SENTRY_RELEASE (the image tag).
"""

from __future__ import annotations

import os
from typing import Any, cast

from .component import COMPONENT
from .telemetry import trace_ids


def scrub(event: dict[str, Any], hint: Any = None) -> dict[str, Any]:
    """What a Sentry event may carry (before_send)."""
    for key in ("user", "extra", "breadcrumbs", "server_name", "request"):
        event.pop(key, None)
    for ex in (event.get("exception") or {}).get("values") or []:
        ex.pop("value", None)
        for frame in (ex.get("stacktrace") or {}).get("frames") or []:
            frame.pop("vars", None)
    logentry = event.get("logentry")
    if isinstance(logentry, dict):
        event["logentry"] = {"message": logentry.get("message")}
    event.pop("message", None)
    trace_id, _ = trace_ids()
    if trace_id:
        event.setdefault("tags", {})["trace_id"] = trace_id
    return event


def start_errors(environ: dict[str, str] | None = None) -> bool:
    """Start Sentry when SENTRY_DSN is set; returns whether it did."""
    env = os.environ if environ is None else environ
    dsn = (env.get("SENTRY_DSN") or "").strip()
    if not dsn:
        return False
    import sentry_sdk

    sentry_sdk.init(
        dsn=dsn,
        environment=env.get("ENVIRONMENT") or "local",
        release=env.get("SENTRY_RELEASE") or None,
        send_default_pii=False,
        include_local_variables=False,
        traces_sample_rate=None,  # traces go through OpenTelemetry (telemetry.py)
        before_send=cast(Any, scrub),
        before_breadcrumb=lambda crumb, hint: None,
    )
    sentry_sdk.set_tag("component", COMPONENT)
    return True


def flush() -> None:
    """Send what's queued, before the process exits."""
    try:
        import sentry_sdk

        sentry_sdk.flush(timeout=5)
    except ImportError:  # pragma: no cover - sentry-sdk is a dependency
        pass
