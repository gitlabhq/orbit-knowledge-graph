from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.global_session import GlobalSession
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    directory: str | Unset = UNSET,
    workspace: str | Unset = UNSET,
    roots: bool | Unset = UNSET,
    start: float | Unset = UNSET,
    cursor: float | Unset = UNSET,
    search: str | Unset = UNSET,
    limit: float | Unset = UNSET,
    archived: bool | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["directory"] = directory

    params["workspace"] = workspace

    params["roots"] = roots

    params["start"] = start

    params["cursor"] = cursor

    params["search"] = search

    params["limit"] = limit

    params["archived"] = archived

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/experimental/session",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> list[GlobalSession] | None:
    if response.status_code == 200:
        response_200 = []
        _response_200 = response.json()
        for response_200_item_data in _response_200:
            response_200_item = GlobalSession.from_dict(response_200_item_data)

            response_200.append(response_200_item)

        return response_200

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[list[GlobalSession]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    directory: str | Unset = UNSET,
    workspace: str | Unset = UNSET,
    roots: bool | Unset = UNSET,
    start: float | Unset = UNSET,
    cursor: float | Unset = UNSET,
    search: str | Unset = UNSET,
    limit: float | Unset = UNSET,
    archived: bool | Unset = UNSET,
) -> Response[list[GlobalSession]]:
    """List sessions

     Get a list of all OpenCode sessions across projects, sorted by most recently updated. Archived
    sessions are excluded by default.

    Args:
        directory (str | Unset):
        workspace (str | Unset):
        roots (bool | Unset):
        start (float | Unset):
        cursor (float | Unset):
        search (str | Unset):
        limit (float | Unset):
        archived (bool | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[list[GlobalSession]]
    """

    kwargs = _get_kwargs(
        directory=directory,
        workspace=workspace,
        roots=roots,
        start=start,
        cursor=cursor,
        search=search,
        limit=limit,
        archived=archived,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient | Client,
    directory: str | Unset = UNSET,
    workspace: str | Unset = UNSET,
    roots: bool | Unset = UNSET,
    start: float | Unset = UNSET,
    cursor: float | Unset = UNSET,
    search: str | Unset = UNSET,
    limit: float | Unset = UNSET,
    archived: bool | Unset = UNSET,
) -> list[GlobalSession] | None:
    """List sessions

     Get a list of all OpenCode sessions across projects, sorted by most recently updated. Archived
    sessions are excluded by default.

    Args:
        directory (str | Unset):
        workspace (str | Unset):
        roots (bool | Unset):
        start (float | Unset):
        cursor (float | Unset):
        search (str | Unset):
        limit (float | Unset):
        archived (bool | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        list[GlobalSession]
    """

    return sync_detailed(
        client=client,
        directory=directory,
        workspace=workspace,
        roots=roots,
        start=start,
        cursor=cursor,
        search=search,
        limit=limit,
        archived=archived,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    directory: str | Unset = UNSET,
    workspace: str | Unset = UNSET,
    roots: bool | Unset = UNSET,
    start: float | Unset = UNSET,
    cursor: float | Unset = UNSET,
    search: str | Unset = UNSET,
    limit: float | Unset = UNSET,
    archived: bool | Unset = UNSET,
) -> Response[list[GlobalSession]]:
    """List sessions

     Get a list of all OpenCode sessions across projects, sorted by most recently updated. Archived
    sessions are excluded by default.

    Args:
        directory (str | Unset):
        workspace (str | Unset):
        roots (bool | Unset):
        start (float | Unset):
        cursor (float | Unset):
        search (str | Unset):
        limit (float | Unset):
        archived (bool | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[list[GlobalSession]]
    """

    kwargs = _get_kwargs(
        directory=directory,
        workspace=workspace,
        roots=roots,
        start=start,
        cursor=cursor,
        search=search,
        limit=limit,
        archived=archived,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    directory: str | Unset = UNSET,
    workspace: str | Unset = UNSET,
    roots: bool | Unset = UNSET,
    start: float | Unset = UNSET,
    cursor: float | Unset = UNSET,
    search: str | Unset = UNSET,
    limit: float | Unset = UNSET,
    archived: bool | Unset = UNSET,
) -> list[GlobalSession] | None:
    """List sessions

     Get a list of all OpenCode sessions across projects, sorted by most recently updated. Archived
    sessions are excluded by default.

    Args:
        directory (str | Unset):
        workspace (str | Unset):
        roots (bool | Unset):
        start (float | Unset):
        cursor (float | Unset):
        search (str | Unset):
        limit (float | Unset):
        archived (bool | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        list[GlobalSession]
    """

    return (
        await asyncio_detailed(
            client=client,
            directory=directory,
            workspace=workspace,
            roots=roots,
            start=start,
            cursor=cursor,
            search=search,
            limit=limit,
            archived=archived,
        )
    ).parsed
