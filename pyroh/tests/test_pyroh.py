import asyncio

import pytest

import pyroh

ALPN = b"pyroh/1"


@pytest.fixture(scope="session")
async def server_ep() -> pyroh.Endpoint:  # type: ignore[misc]
    ep = await pyroh.Endpoint.bind(alpns=[ALPN])
    yield ep
    await ep._endpoint.close()


@pytest.fixture(scope="session")
async def client_ep() -> pyroh.Endpoint:  # type: ignore[misc]
    ep = await pyroh.Endpoint.bind(alpns=[ALPN])
    yield ep
    await ep._endpoint.close()


async def echo_handler(conn: pyroh.Connection) -> None:
    reader, writer = await conn.accept_bi()
    data = await reader.read(-1)
    writer.write(data)
    await writer.drain()
    writer.write_eof()
    writer.close()
    await writer.wait_closed()


@pytest.mark.asyncio
async def test_endpoint_bind():
    ep = await pyroh.Endpoint.bind(alpns=[ALPN])
    async with ep:
        assert isinstance(ep.id, str)
        assert len(ep.id) > 0


@pytest.mark.asyncio
async def test_connect(server_ep: pyroh.Endpoint, client_ep: pyroh.Endpoint):
    server = server_ep.start_server(echo_handler)
    conn = await client_ep.connect(server_ep.id, alpn=ALPN)
    assert conn is not None
    server.close()


@pytest.mark.asyncio
async def test_bi_stream_echo(server_ep: pyroh.Endpoint, client_ep: pyroh.Endpoint):
    server = server_ep.start_server(echo_handler)

    conn = await client_ep.connect(server_ep.id, alpn=ALPN)
    reader, writer = await conn.open_bi()

    payload = b"hello pyroh"
    writer.write(payload)
    await writer.drain()
    writer.write_eof()

    response = await reader.read(-1)
    assert response == payload

    writer.close()
    await writer.wait_closed()
    server.close()


@pytest.mark.asyncio
async def test_large_payload(server_ep: pyroh.Endpoint, client_ep: pyroh.Endpoint):
    server = server_ep.start_server(echo_handler)

    conn = await client_ep.connect(server_ep.id, alpn=ALPN)
    reader, writer = await conn.open_bi()

    payload = b"x" * 2_000_000  # 2 MB
    writer.write(payload)
    await writer.drain()
    writer.write_eof()

    response = await reader.read(-1)
    assert response == payload

    writer.close()
    await writer.wait_closed()
    server.close()


@pytest.mark.asyncio
async def test_uni_stream(server_ep: pyroh.Endpoint, client_ep: pyroh.Endpoint):
    received: list[bytes] = []
    got_data = asyncio.Event()

    async def uni_handler(conn: pyroh.Connection) -> None:
        uni_reader = await conn.accept_uni()
        data = await uni_reader.read(-1)
        received.append(data)
        got_data.set()

    server = server_ep.start_server(uni_handler)

    conn = await client_ep.connect(server_ep.id, alpn=ALPN)

    payload = b"fire and forget"
    writer = await conn.open_uni()
    writer.write(payload)
    await writer.drain()
    writer.write_eof()
    writer.close()
    await writer.wait_closed()

    await asyncio.wait_for(got_data.wait(), timeout=5)
    assert received == [payload]
    server.close()


@pytest.mark.asyncio
async def test_multiplexed_bi_streams(
    server_ep: pyroh.Endpoint, client_ep: pyroh.Endpoint
):
    """Multiple concurrent bidirectional streams over one connection all round-trip."""
    N = 5

    async def multi_echo_handler(conn: pyroh.Connection) -> None:
        tasks = [asyncio.create_task(_accept_and_echo(conn)) for _ in range(N)]
        await asyncio.gather(*tasks)

    async def _accept_and_echo(conn: pyroh.Connection) -> None:
        reader, writer = await conn.accept_bi()
        data = await reader.read(-1)
        writer.write(data)
        await writer.drain()
        writer.write_eof()
        writer.close()
        await writer.wait_closed()

    async def _open_and_send(conn: pyroh.Connection, i: int) -> bytes:
        reader, writer = await conn.open_bi()
        msg = f"stream-{i}".encode()
        writer.write(msg)
        await writer.drain()
        writer.write_eof()
        response = await reader.read(-1)
        writer.close()
        await writer.wait_closed()
        return response

    server = server_ep.start_server(multi_echo_handler)

    conn = await client_ep.connect(server_ep.id, alpn=ALPN)
    results: list[bytes] = list(
        await asyncio.gather(*[_open_and_send(conn, i) for i in range(N)])
    )

    for i, result in enumerate(results):
        assert result == f"stream-{i}".encode()

    server.close()


@pytest.mark.asyncio
async def test_bidirectional_both_sides():
    async def handler(conn: pyroh.Connection) -> None:
        reader, writer = await conn.accept_bi()
        data = await reader.read(-1)
        writer.write(b"echo:" + data)
        await writer.drain()
        writer.write_eof()
        writer.close()
        await writer.wait_closed()

    async def connect_and_send(ep: pyroh.Endpoint, remote_id: str, msg: bytes) -> bytes:
        conn = await ep.connect(remote_id, alpn=ALPN)
        reader, writer = await conn.open_bi()
        writer.write(msg)
        await writer.drain()
        writer.write_eof()
        response = await reader.read(-1)
        writer.close()
        await writer.wait_closed()
        return response

    ep_a, ep_b = await asyncio.gather(
        pyroh.Endpoint.bind(alpns=[ALPN]),
        pyroh.Endpoint.bind(alpns=[ALPN]),
    )
    async with ep_a, ep_b:
        server_a = ep_a.start_server(handler)
        server_b = ep_b.start_server(handler)

        resp_a, resp_b = await asyncio.gather(
            connect_and_send(ep_a, ep_b.id, b"from A"),
            connect_and_send(ep_b, ep_a.id, b"from B"),
        )

        assert resp_a == b"echo:from A"
        assert resp_b == b"echo:from B"

        server_a.close()
        server_b.close()


@pytest.mark.asyncio
async def test_empty_payload(server_ep: pyroh.Endpoint, client_ep: pyroh.Endpoint):
    server = server_ep.start_server(echo_handler)

    conn = await client_ep.connect(server_ep.id, alpn=ALPN)
    reader, writer = await conn.open_bi()

    writer.write(b"")
    await writer.drain()
    writer.write_eof()

    response = await reader.read(-1)
    assert response == b""

    writer.close()
    await writer.wait_closed()
    server.close()


@pytest.mark.asyncio
async def test_multiple_connections_to_same_server(server_ep: pyroh.Endpoint):
    N = 3
    results: list[bytes] = []

    async def collecting_handler(conn: pyroh.Connection) -> None:
        reader, writer = await conn.accept_bi()
        data = await reader.read(-1)
        results.append(data)
        writer.write(b"ok")
        await writer.drain()
        writer.write_eof()
        writer.close()
        await writer.wait_closed()

    client_eps: list[pyroh.Endpoint] = list(
        await asyncio.gather(*[pyroh.Endpoint.bind(alpns=[ALPN]) for _ in range(N)])
    )

    async def client_send(ep: pyroh.Endpoint, msg: bytes) -> bytes:
        conn = await ep.connect(server_ep.id, alpn=ALPN)
        reader, writer = await conn.open_bi()
        writer.write(msg)
        await writer.drain()
        writer.write_eof()
        response = await reader.read(-1)
        writer.close()
        await writer.wait_closed()
        return response

    server = server_ep.start_server(collecting_handler)
    try:
        responses = await asyncio.gather(
            *[
                client_send(ep, f"client-{i}".encode())
                for i, ep in enumerate(client_eps)
            ]
        )

        assert all(r == b"ok" for r in responses)
        assert len(results) == N
    finally:
        server.close()
        await asyncio.gather(*[ep._endpoint.close() for ep in client_eps])
