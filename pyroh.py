"""pyroh: A friendly, minimalistic wrapper for iroh's Python bindings

Provides standard asyncio.StreamReader/StreamWriter interfaces for p2p connections
by wrapping iroh's QUIC streams.

Architecture:
    Pyroh wraps iroh's QUIC-based primitives (Endpoint, Connection, BiStream)
    and provides asyncio.StreamReader/StreamWriter interfaces through custom
    transport and protocol implementations.

Example:
    import asyncio
    import pyroh

    async def main():
        reader, writer = await pyroh.connect(node_addr, alpn=b"myapp")
        writer.write(b"Hello!")
        await writer.drain()
        response = await reader.read(1024)

    asyncio.run(main())
"""

import asyncio
from asyncio import StreamReader, StreamWriter
from collections.abc import Callable, Coroutine
from typing import Any

import iroh
import iroh.iroh_ffi

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "Connection",
    "Server",
    "connect",
    "serve",
    "Iroh",
    "NodeAddr",
    "PublicKey",
    "node_addr",
    "open_connection",
    "setup_event_loop",
]

# re exports
Iroh = iroh.Iroh
NodeAddr = iroh.NodeAddr
PublicKey = iroh.PublicKey
NodeOptions = iroh.NodeOptions

StreamHandler = Callable[[StreamReader, StreamWriter], Coroutine[Any, Any, None]]


def setup_event_loop(loop: asyncio.AbstractEventLoop | None = None) -> None:
    """Setup the event loop for iroh async callbacks. Call once at startup."""
    if loop is None:
        loop = asyncio.get_running_loop()
    iroh.iroh_ffi.uniffi_set_event_loop(loop)  # type: ignore[arg-type]


def node_addr(
    node_id: str | PublicKey,
    relay_url: str | None = None,
    addrs: list[str] | None = None,
) -> NodeAddr:
    """Create a NodeAddr from components."""
    if isinstance(node_id, str):
        node_id = PublicKey.from_string(node_id)
    return NodeAddr(node_id, relay_url, addrs or [])


class QuicTransport(asyncio.Transport):
    """asyncio Transport implementation wrapping iroh QUIC streams.

    This transport implements the asyncio transport interface but reads data
    on-demand rather than using a background read loop. This is necessary
    because iroh's QUIC read() can block indefinitely if called before data
    is available.
    """

    _DEFAULT_HIGH_WATER = 64 * 1024
    _DEFAULT_LOW_WATER = 16 * 1024

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        recv: iroh.RecvStream,
        send: iroh.SendStream,
        protocol: asyncio.Protocol,
        reader: StreamReader,
        extra: dict[str, Any] | None = None,
    ) -> None:
        self._loop = loop
        self._recv = recv
        self._send = send
        self._protocol = protocol
        self._reader = reader
        self._extra = extra or {}
        self._closing = False
        self._closed = False
        # Write state
        self._buffer: list[bytes] = []
        self._buffer_size = 0
        self._high_water = self._DEFAULT_HIGH_WATER
        self._low_water = self._DEFAULT_LOW_WATER
        self._paused = False
        self._write_task: asyncio.Task[None] | None = None
        self._eof_written = False

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        return self._extra.get(name, default)

    def is_closing(self) -> bool:
        return self._closing

    def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._schedule_write()

    def set_protocol(self, protocol: asyncio.Protocol) -> None:
        self._protocol = protocol

    def get_protocol(self) -> asyncio.Protocol:
        return self._protocol

    # ReadTransport methods - these are no-ops since we read on-demand
    def is_reading(self) -> bool:
        return not self._closing

    def pause_reading(self) -> None:
        pass

    def resume_reading(self) -> None:
        pass

    # WriteTransport methods
    def set_write_buffer_limits(
        self, high: int | None = None, low: int | None = None
    ) -> None:
        if high is None:
            high = self._DEFAULT_HIGH_WATER
        if low is None:
            low = high // 4
        self._high_water = high
        self._low_water = low

    def get_write_buffer_size(self) -> int:
        return self._buffer_size

    def get_write_buffer_limits(self) -> tuple[int, int]:
        return (self._low_water, self._high_water)

    def write(self, data: bytes) -> None:
        if self._closing or self._eof_written:
            return
        if not data:
            return
        self._buffer.append(data)
        self._buffer_size += len(data)
        self._schedule_write()
        self._maybe_pause_protocol()

    def writelines(self, list_of_data: list[bytes]) -> None:
        for data in list_of_data:
            self.write(data)

    def write_eof(self) -> None:
        if self._eof_written:
            return
        self._eof_written = True
        self._schedule_write()

    def can_write_eof(self) -> bool:
        return True

    def abort(self) -> None:
        self._closing = True
        self._closed = True
        self._buffer.clear()
        self._buffer_size = 0
        if self._write_task is not None:
            self._write_task.cancel()
        self._loop.create_task(self._abort_stream())

    async def _abort_stream(self) -> None:
        try:
            await self._send.reset(0)
        except Exception:
            pass

    def _schedule_write(self) -> None:
        if self._write_task is None or self._write_task.done():
            self._write_task = self._loop.create_task(self._do_write())

    async def _do_write(self) -> None:
        """Background task that flushes buffered data to QUIC."""
        try:
            # Yield to allow batching of multiple write() calls
            await asyncio.sleep(0)

            while self._buffer and not self._closed:
                data = b"".join(self._buffer)
                self._buffer.clear()
                self._buffer_size = 0

                try:
                    await self._send.write_all(data)
                except Exception:
                    self._closed = True
                    self._protocol.connection_lost(None)
                    return

                # Check if we should resume the protocol
                if self._paused and self._buffer_size <= self._low_water:
                    self._paused = False
                    try:
                        self._protocol.resume_writing()  # type: ignore[attr-defined]
                    except Exception:
                        pass

            if self._eof_written and not self._closed:
                try:
                    await self._send.finish()
                except Exception:
                    pass

            if self._closing and not self._buffer and not self._closed:
                self._closed = True

        except asyncio.CancelledError:
            pass

    def _maybe_pause_protocol(self) -> None:
        if self._buffer_size >= self._high_water and not self._paused:
            self._paused = True
            try:
                self._protocol.pause_writing()  # type: ignore[attr-defined]
            except Exception:
                pass

    async def _drain_helper(self) -> None:
        """Wait until the write buffer is flushed."""
        if self._closed:
            raise ConnectionResetError("Connection lost")
        # Wait for any pending write task to complete
        if self._write_task is not None and not self._write_task.done():
            await self._write_task

    async def _read_from_quic(self, n: int) -> bytes:
        """Read data from the QUIC recv stream."""
        if self._closing or self._closed:
            return b""
        try:
            data = await self._recv.read(n)
            return data if data else b""
        except Exception:
            return b""


class QuicStreamReader(StreamReader):
    """StreamReader that reads from a QUIC recv stream on-demand.

    This subclass overrides the standard StreamReader to pull data from
    the QUIC stream when needed, rather than relying on a transport to
    push data via feed_data().
    """

    def __init__(
        self,
        recv: iroh.RecvStream,
        limit: int = 2**16,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        super().__init__(limit=limit)
        self._quic_recv = recv
        self._quic_eof = False

    async def _fill_buffer(self, n: int = 65536) -> None:
        """Read data from QUIC and feed it to the buffer."""
        if self._quic_eof:
            return
        try:
            data = await self._quic_recv.read(n)
            if data:
                self.feed_data(data)
            else:
                self._quic_eof = True
                self.feed_eof()
        except Exception:
            self._quic_eof = True
            self.feed_eof()

    async def read(self, n: int = -1) -> bytes:
        """Read up to n bytes."""
        if n == 0:
            return b""

        # If we need more data, read from QUIC
        while not self._buffer and not self._eof:
            await self._fill_buffer()
            if self._eof:
                break

        if n < 0:
            # Read all available
            data = bytes(self._buffer)
            self._buffer.clear()
            return data

        # Read up to n bytes from buffer
        data = bytes(self._buffer[:n])
        del self._buffer[:n]
        return data

    async def readline(self) -> bytes:
        """Read a line (until newline or EOF)."""
        line = bytearray()
        while True:
            # Check if we have a newline in buffer
            if b"\n" in self._buffer:
                idx = self._buffer.index(b"\n")
                line.extend(self._buffer[: idx + 1])
                del self._buffer[: idx + 1]
                return bytes(line)

            # Add buffer to line and clear it
            line.extend(self._buffer)
            self._buffer.clear()

            if self._eof:
                return bytes(line)

            # Read more data
            await self._fill_buffer()

    async def readexactly(self, n: int) -> bytes:
        """Read exactly n bytes."""
        data = bytearray()
        while len(data) < n:
            needed = n - len(data)
            # Use buffer first
            if self._buffer:
                chunk = bytes(self._buffer[:needed])
                del self._buffer[:needed]
                data.extend(chunk)
            elif self._eof:
                raise asyncio.IncompleteReadError(bytes(data), n)
            else:
                await self._fill_buffer()

        return bytes(data)

    async def readuntil(self, separator: bytes = b"\n") -> bytes:
        """Read until separator is found."""
        data = bytearray()
        while True:
            # Check if separator is in buffer
            if separator in self._buffer:
                idx = self._buffer.index(separator)
                data.extend(self._buffer[: idx + len(separator)])
                del self._buffer[: idx + len(separator)]
                return bytes(data)

            # Add buffer to data
            data.extend(self._buffer)
            self._buffer.clear()

            if self._eof:
                raise asyncio.IncompleteReadError(bytes(data), None)  # type: ignore[arg-type]

            await self._fill_buffer()


class QuicStreamReaderProtocol(asyncio.StreamReaderProtocol):
    """Protocol for QUIC streams with drain support."""

    def __init__(
        self,
        stream_reader: StreamReader,
        client_connected_cb: Callable[..., Coroutine[Any, Any, None]] | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        super().__init__(stream_reader, client_connected_cb, loop)
        self._closed = self._loop.create_future()

    def connection_lost(self, exc: Exception | None) -> None:
        super().connection_lost(exc)
        if not self._closed.done():
            if exc is None:
                self._closed.set_result(None)
            else:
                self._closed.set_exception(exc)

    async def _drain_helper(self) -> None:
        """Drain helper that works with our QuicTransport."""
        if self._stream_reader is not None:
            exc = self._stream_reader.exception()
            if exc is not None:
                raise exc
        transport = self._transport
        if transport is not None and isinstance(transport, QuicTransport):
            await transport._drain_helper()

    async def _get_close_waiter(self, stream: StreamWriter) -> None:
        await self._closed


async def open_quic_stream(
    recv: iroh.RecvStream,
    send: iroh.SendStream,
    extra: dict[str, Any] | None = None,
) -> tuple[StreamReader, StreamWriter]:
    """Create asyncio StreamReader/StreamWriter pair from QUIC streams.

    This is the core function that wraps iroh's QUIC streams in standard
    asyncio stream interfaces.
    """
    loop = asyncio.get_running_loop()

    # Use our custom StreamReader that reads from QUIC on-demand
    reader = QuicStreamReader(recv, limit=2**16, loop=loop)
    protocol = QuicStreamReaderProtocol(reader, loop=loop)

    transport = QuicTransport(loop, recv, send, protocol, reader, extra)
    protocol.connection_made(transport)

    writer = StreamWriter(transport, protocol, reader, loop)
    return reader, writer


class Connection:
    """Wrapper around iroh.Connection providing stream pair creation."""

    __slots__ = ("_conn", "_remote_id", "_alpn")

    def __init__(self, conn: iroh.Connection) -> None:
        self._conn = conn
        self._remote_id = conn.remote_node_id()
        self._alpn = conn.alpn()

    @property
    def remote_node_id(self) -> str:
        return self._remote_id

    @property
    def alpn(self) -> bytes | None:
        return self._alpn

    async def open_stream(self) -> tuple[StreamReader, StreamWriter]:
        """Open a new bidirectional stream."""
        bi = await self._conn.open_bi()
        return await open_quic_stream(bi.recv(), bi.send())

    async def accept_stream(self) -> tuple[StreamReader, StreamWriter]:
        """Accept an incoming bidirectional stream."""
        bi = await self._conn.accept_bi()
        return await open_quic_stream(bi.recv(), bi.send())

    def close(self, code: int = 0, reason: bytes = b"") -> None:
        """Close the connection."""
        self._conn.close(code, reason)

    async def closed(self) -> str:
        """Wait for connection to close, returns close reason."""
        return await self._conn.closed()


class _ProtocolHandler:
    """Internal protocol handler that bridges iroh callbacks to asyncio handlers."""

    def __init__(
        self,
        handler: StreamHandler,
        on_connection: Callable[[iroh.Connection], None] | None = None,
    ) -> None:
        self._handler = handler
        self._on_connection = on_connection
        self._tasks: set[asyncio.Task[None]] = set()

    async def accept(self, conn: iroh.Connection) -> None:
        """Called by iroh when a new connection is accepted."""
        if self._on_connection:
            self._on_connection(conn)

        try:
            while True:
                bi = await conn.accept_bi()
                reader, writer = await open_quic_stream(bi.recv(), bi.send())
                task = asyncio.create_task(self._run_handler(reader, writer))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
        except Exception:
            pass  # Connection closed

    async def _run_handler(self, reader: StreamReader, writer: StreamWriter) -> None:
        try:
            await self._handler(reader, writer)
        except Exception:
            pass
        finally:
            if not writer.is_closing():
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass

    async def shutdown(self) -> None:
        """Shutdown the protocol handler."""
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)


class _ProtocolCreator:
    """Creates protocol handler instances for iroh.

    Implements iroh.ProtocolCreator protocol.
    """

    def __init__(self, handler: StreamHandler) -> None:
        self._handler = handler
        self._instance: _ProtocolHandler | None = None

    def create(self, endpoint: iroh.Endpoint) -> _ProtocolHandler:
        self._instance = _ProtocolHandler(self._handler)
        return self._instance


class Server:
    """Server that accepts connections for a given ALPN protocol."""

    def __init__(
        self,
        node: iroh.Iroh,
        alpn: bytes,
        handler: StreamHandler,
        creator: _ProtocolCreator,
    ) -> None:
        self._node = node
        self._alpn = alpn
        self._handler = handler
        self._creator = creator
        self._closed = False

    @property
    def node(self) -> iroh.Iroh:
        return self._node

    @property
    def alpn(self) -> bytes:
        return self._alpn

    @property
    def endpoint(self) -> iroh.Endpoint:
        return self._node.node().endpoint()

    @property
    def node_id(self) -> str:
        return self.endpoint.node_id()

    async def node_addr(self) -> NodeAddr:
        """Get the full node address for clients to connect to."""
        return await self._node.net().node_addr()

    async def serve_forever(self) -> None:
        """Run until cancelled."""
        try:
            while not self._closed:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass

    def close(self) -> None:
        """Stop accepting connections."""
        self._closed = True

    async def wait_closed(self) -> None:
        """Wait for server to fully close."""
        if self._creator._instance:
            await self._creator._instance.shutdown()

    async def __aenter__(self) -> "Server":
        return self

    async def __aexit__(self, *args: object) -> None:
        self.close()
        await self.wait_closed()


async def serve(
    handler: StreamHandler,
    alpn: bytes | str,
    *,
    node: iroh.Iroh | None = None,
) -> Server:
    """Create and start a server that accepts connections.

    Args:
        handler: Async function called for each stream: async def handler(reader, writer)
        alpn: Protocol identifier for ALPN negotiation
        node: Existing Iroh node (creates in-memory node if None)

    Returns:
        Server instance with node_id and node_addr for clients to connect
    """
    setup_event_loop()

    if isinstance(alpn, str):
        alpn = alpn.encode()

    creator = _ProtocolCreator(handler)
    protocols: dict[bytes, Any] = {alpn: creator}

    options = NodeOptions()
    options.protocols = protocols  # type: ignore[assignment]

    created_node = node if node is not None else await Iroh.memory_with_options(options)

    return Server(created_node, alpn, handler, creator)


async def connect(
    addr: NodeAddr | str,
    alpn: bytes | str = b"pyroh",
    *,
    node: iroh.Iroh | None = None,
) -> tuple[StreamReader, StreamWriter]:
    """Connect to a remote node and open a bidirectional stream.

    Args:
        addr: Target node address (NodeAddr or node ID string)
        alpn: Protocol identifier for ALPN negotiation
        node: Existing Iroh node (creates ephemeral node if None)

    Returns:
        Tuple of (asyncio.StreamReader, asyncio.StreamWriter)
    """
    setup_event_loop()

    if isinstance(alpn, str):
        alpn = alpn.encode()

    if isinstance(addr, str):
        addr = node_addr(addr)

    iroh_node = node if node is not None else await Iroh.memory()

    endpoint = iroh_node.node().endpoint()
    conn = await endpoint.connect(addr, alpn)
    bi = await conn.open_bi()

    return await open_quic_stream(bi.recv(), bi.send())


async def open_connection(
    addr: NodeAddr | str,
    alpn: bytes | str = b"pyroh",
    *,
    node: iroh.Iroh | None = None,
) -> Connection:
    """Connect to a remote node and return a Connection for multiple streams.

    Args:
        addr: Target node address
        alpn: Protocol identifier
        node: Existing Iroh node

    Returns:
        Connection object that can open/accept multiple streams
    """
    setup_event_loop()

    if isinstance(alpn, str):
        alpn = alpn.encode()

    if isinstance(addr, str):
        addr = node_addr(addr)

    iroh_node = node if node is not None else await Iroh.memory()

    endpoint = iroh_node.node().endpoint()
    conn = await endpoint.connect(addr, alpn)

    return Connection(conn)
