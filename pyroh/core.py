import asyncio
from collections.abc import Callable, Coroutine
from typing import Any, Optional

from . import _iroh
from ._transport import IrohRecvTransport, IrohSendTransport, IrohStreamTransport

DEFAULT_ALPN = b"pyroh/1"

type ConnectionHandler = Callable[[Connection], Coroutine[Any, Any, Any]]


class Endpoint:
    """An iroh endpoint. Create with `bind`."""

    _endpoint: _iroh.IrohEndpoint

    @classmethod
    # TODO: support custom keys
    async def bind(cls, *, alpns: list[bytes] = [DEFAULT_ALPN]) -> Endpoint:
        iendpoint = await _iroh.IrohEndpoint.bind(alpns)
        await iendpoint.wait_online()
        return cls(iendpoint)

    def __init__(self, iendpoint: _iroh.IrohEndpoint):
        self._endpoint = iendpoint

    @property
    def id(self) -> str:
        """The endpoint's public key (node ID) as a string."""
        return self._endpoint.addr

    async def connect(self, addr: str, *, alpn: bytes = DEFAULT_ALPN) -> Connection:
        rust_conn = await self._endpoint.connect(addr, alpn)
        return Connection(rust_conn)

    def set_alpns(self, alpns: list[bytes]) -> None:
        """Update the set of accepted ALPNs at runtime."""
        self._endpoint.set_alpns(alpns)

    def start_server(self, handler: ConnectionHandler) -> Server:
        server = Server(self._endpoint)
        server.start(handler)
        return server

    async def __aenter__(self) -> Endpoint:
        return self

    async def __aexit__(self, *args):
        await self._endpoint.close()


class Connection:
    """QUIC connection. Can multiplex streams."""

    _conn: _iroh.IrohConnection

    def __init__(self, rust_conn: _iroh.IrohConnection):
        self._conn = rust_conn

        # (for now?) does not clean up on individual stream close, only on self.close()
        self._transports: set[
            IrohStreamTransport | IrohSendTransport | IrohRecvTransport
        ] = set()

    async def open_bi(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Open a bidirectional stream."""
        rust_send, rust_recv = await self._conn.open_bi()
        return await _init_streams(rust_send, rust_recv)

    async def accept_bi(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Accept a bidirectional stream."""
        rust_send, rust_recv = await self._conn.accept_bi()
        return await _init_streams(rust_send, rust_recv)

    async def open_uni(self) -> asyncio.StreamWriter:
        """Open a send-only unidirectional stream."""
        rust_send = await self._conn.open_uni()
        return _init_send_stream(rust_send)

    async def accept_uni(self) -> asyncio.StreamReader:
        """Accept a receive-only unidirectional stream."""
        rust_recv = await self._conn.accept_uni()
        return await _init_recv_stream(rust_recv)

    async def abort(self, error_code: int = 0, reason: str = ""):
        """Forcefully close the connection with an application error code."""
        await self._conn.close(error_code, reason.encode())

    def close(self):
        for transport in self._transports:
            transport.close()

    async def wait_closed(self):
        await asyncio.gather(*(t.wait_closed() for t in self._transports))

    async def __aenter__(self) -> Connection:
        return self

    async def __aexit__(self, *args):
        self.close()
        await self.wait_closed()


class Server:
    """Accepts incoming QUIC connections on an endpoint."""

    _endpoint: _iroh.IrohEndpoint
    _closed: bool

    def __init__(self, rust_endpoint: _iroh.IrohEndpoint):
        self._endpoint = rust_endpoint
        self._closed = False

        self.server_task: Optional[asyncio.Task[None]] = None
        self.handler: Optional[ConnectionHandler] = None

        self._serving_forever_fut: Optional[asyncio.Future] = None

    @property
    def id(self) -> str:
        return self._endpoint.addr

    async def _accept(self) -> Connection:
        if self._closed:
            raise OSError("server is closed")

        rust_conn = await self._endpoint.accept()
        return Connection(rust_conn)

    def start(self, handler: ConnectionHandler) -> None:
        self.handler = handler
        self.server_task = asyncio.create_task(self._serve())

    async def _serve(self) -> None:
        """Accept connections in a loop, spawning handler(reader, writer) for each stream."""
        while not self._closed:
            try:
                conn = await self._accept()
            except OSError:
                break

            if self.handler is not None:
                asyncio.create_task(self.handler(conn))
            else:
                raise RuntimeError("server started without a handler")

    async def serve_forever(self):
        # https://github.com/python/cpython/blob/main/Lib/asyncio/base_events.py#L368-L387
        if self._serving_forever_fut is not None:
            raise RuntimeError(
                f"server {self!r} is already being awaited on serve_forever()"
            )

        if self.server_task is None:
            self.server_task = asyncio.create_task(self._serve())

        self._serving_forever_fut = asyncio.get_running_loop().create_future()

        try:
            await self._serving_forever_fut
        except asyncio.CancelledError:
            try:
                self.close()
            finally:
                raise
        finally:
            self._serving_forever_fut = None

    def close(self):
        """Stop accepting new connections. Does not close the underlying endpoint."""
        self._closed = True

    async def __aenter__(self) -> Server:
        return self

    async def __aexit__(self, *args):
        self.close()

    # just in case since we can
    def __enter__(self) -> Server:
        return self

    def __exit__(self, *args):
        self.close()


# TODO: overload?
async def _init_streams(
    rust_send: _iroh.IrohSendStream, rust_recv: _iroh.IrohRecvStream
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    loop = asyncio.get_event_loop()

    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)

    transport = IrohStreamTransport(loop, protocol, rust_send, rust_recv)
    protocol.connection_made(transport)
    transport.start()

    writer = asyncio.StreamWriter(transport, protocol, reader, loop)

    return reader, writer


def _init_send_stream(rust_send: _iroh.IrohSendStream) -> asyncio.StreamWriter:
    loop = asyncio.get_event_loop()

    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)

    transport = IrohSendTransport(loop, protocol, rust_send)
    protocol.connection_made(transport)

    return asyncio.StreamWriter(transport, protocol, reader, loop)


async def _init_recv_stream(rust_recv: _iroh.IrohRecvStream) -> asyncio.StreamReader:
    loop = asyncio.get_event_loop()

    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)

    transport = IrohRecvTransport(loop, protocol, rust_recv)
    protocol.connection_made(transport)
    transport.start()

    return reader
