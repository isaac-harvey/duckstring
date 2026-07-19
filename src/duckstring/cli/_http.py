from __future__ import annotations

import typer


def pond_params(major: int | None = None, version: str | None = None) -> dict:
    """Query params targeting one major line of a Pond (omitted = the server's default, the
    highest deployed major)."""
    params: dict = {}
    if major is not None:
        params["major"] = major
    if version is not None:
        params["version"] = version
    return params


def request(method: str, url: str, auth: dict | None = None, **kwargs):
    """``auth`` is the registered catchment's config dict — its `key` and/or custom `headers`
    (``catchment connect --key/--header``) are attached to the request via
    :func:`duckstring.cli.config.auth_headers`."""
    import httpx

    from .config import auth_headers

    raw_timeout = kwargs.pop("timeout", None)
    if raw_timeout is None:
        timeout = httpx.Timeout(60.0, connect=5.0)
    elif isinstance(raw_timeout, (int, float)):
        timeout = httpx.Timeout(float(raw_timeout), connect=5.0)
    else:
        timeout = raw_timeout

    if auth:
        headers = kwargs.pop("headers", None) or {}
        for name, value in auth_headers(auth).items():
            headers.setdefault(name, value)
        if headers:
            kwargs["headers"] = headers

    try:
        resp = httpx.request(method, url, timeout=timeout, **kwargs)
        resp.raise_for_status()
        return resp
    except httpx.ConnectError:
        typer.echo(f"Error: could not connect to {url}", err=True)
        typer.echo("Is the Catchment running? Start it with: duckstring catchment start <name>", err=True)
        raise typer.Exit(1) from None
    except httpx.TimeoutException:
        typer.echo(f"Error: request to {url} timed out", err=True)
        raise typer.Exit(1) from None
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 401:
            typer.echo("Error: the Catchment (or the service in front of it) rejected the request (401).", err=True)
            typer.echo("Set its credentials on the registration: duckstring catchment connect --name <name> "
                       "--path <url> --key <key>  (or --header 'Name: value' for platform auth)", err=True)
        elif exc.response.status_code == 404:
            typer.echo(f"Error: endpoint not found — {exc.request.url}", err=True)
            try:
                detail = exc.response.json().get("detail")
                if detail:
                    typer.echo(f"  {detail}", err=True)
            except Exception:
                pass
        else:
            typer.echo(f"Error: {exc.response.status_code} from Catchment", err=True)
            try:
                detail = exc.response.json().get("detail", exc.response.text)
            except Exception:
                detail = exc.response.text[:300]
            typer.echo(f"  {detail}", err=True)
        raise typer.Exit(1) from None


def get(url: str, auth: dict | None = None, **kwargs):
    return request("GET", url, auth=auth, **kwargs)


def post(url: str, auth: dict | None = None, **kwargs):
    return request("POST", url, auth=auth, **kwargs)


def put(url: str, auth: dict | None = None, **kwargs):
    return request("PUT", url, auth=auth, **kwargs)


def delete(url: str, auth: dict | None = None, **kwargs):
    return request("DELETE", url, auth=auth, **kwargs)
