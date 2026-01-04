"""pyroh: A friendly, minimalistic wrapper for iroh's Python bindings

Provides asyncio-like StreamReader/StreamWriter interfaces for p2p connections.

Architecture:
    Pyroh wraps iroh's QUIC-based primitives (Endpoint, Connection, BiStream)
    in familiar asyncio interfaces. Each Connection can multiplex many BiStreams,
    which we expose as (StreamReader, StreamWriter) pairs.

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
from collections.abc import Callable, Coroutine
from typing import Any

import iroh
import iroh.iroh_ffi

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "StreamReader",
    "StreamWriter",
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

StreamHandler = Callable[["StreamReader", "StreamWriter"], Coroutine[Any, Any, None]]


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


class StreamReader:
    """asyncio.StreamReader-compatible interface for iroh receive streams."""

    __slots__ = ("_recv", "_buffer", "_eof")

    def __init__(self, recv: iroh.RecvStream) -> None:
        self._recv = recv
        self._buffer = bytearray()
        self._eof = False

    async def read(self, n: int = -1) -> bytes:
        """Read up to n bytes. If n is -1, read until EOF."""
        if n == -1:
            return await self.read_to_end()
        if n == 0:
            return b""

        # Drain buffer first
        if self._buffer:
            data = bytes(self._buffer[:n])
            del self._buffer[:n]
            return data

        if self._eof:
            return b""

        try:
            chunk = await self._recv.read(max(n, 8192))
            if not chunk:
                self._eof = True
                return b""
            if len(chunk) <= n:
                return chunk
            self._buffer.extend(chunk[n:])
            return chunk[:n]
        except Exception:
            self._eof = True
            return b""

    async def readexactly(self, n: int) -> bytes:
        """Read exactly n bytes. Raises EOFError if stream ends early."""
        data = await self._recv.read_exact(n)
        if len(data) != n:
            raise EOFError(f"Expected {n} bytes, got {len(data)}")
        return data

    async def readline(self) -> bytes:
        """Read until newline or EOF."""
        line = bytearray()
        while True:
            if b"\n" in self._buffer:
                idx = self._buffer.index(b"\n")
                line.extend(self._buffer[: idx + 1])
                del self._buffer[: idx + 1]
                return bytes(line)

            line.extend(self._buffer)
            self._buffer.clear()

            if self._eof:
                return bytes(line)

            try:
                chunk = await self._recv.read(8192)
                if not chunk:
                    self._eof = True
                    return bytes(line)
                self._buffer.extend(chunk)
            except Exception:
                self._eof = True
                return bytes(line)

    async def readuntil(self, separator: bytes = b"\n") -> bytes:
        """Read until separator is found. Raises EOFError if not found."""
        data = bytearray()
        while True:
            if separator in self._buffer:
                idx = self._buffer.index(separator)
                data.extend(self._buffer[: idx + len(separator)])
                del self._buffer[: idx + len(separator)]
                return bytes(data)

            data.extend(self._buffer)
            self._buffer.clear()

            if self._eof:
                raise EOFError(f"Separator {separator!r} not found")

            try:
                chunk = await self._recv.read(8192)
                if not chunk:
                    self._eof = True
                    raise EOFError(f"Separator {separator!r} not found")
                self._buffer.extend(chunk)
            except EOFError:
                raise
            except Exception:
                self._eof = True
                raise EOFError(f"Separator {separator!r} not found") from None

    async def read_to_end(self, limit: int = 10 * 1024 * 1024) -> bytes:
        """Read all remaining data until EOF."""
        chunks = [bytes(self._buffer)] if self._buffer else []
        self._buffer.clear()
        if not self._eof:
            try:
                chunk = await self._recv.read_to_end(limit)
                if chunk:
                    chunks.append(chunk)
            except Exception:
                pass
            self._eof = True
        return b"".join(chunks)

    def at_eof(self) -> bool:
        """Return True if EOF reached and buffer empty."""
        return self._eof and not self._buffer


class StreamWriter:
    """asyncio.StreamWriter-compatible interface for iroh send streams."""

    __slots__ = ("_send", "_pending", "_closing")

    def __init__(self, send: iroh.SendStream) -> None:
        self._send = send
        self._pending: list[bytes] = []
        self._closing = False

    def write(self, data: bytes) -> None:
        """Buffer data for sending."""
        if self._closing:
            raise RuntimeError("Stream is closing")
        self._pending.append(data)

    def writelines(self, lines: list[bytes]) -> None:
        """Buffer multiple byte strings."""
        self._pending.extend(lines)

    async def drain(self) -> None:
        """Flush buffered data to the stream."""
        if self._pending:
            await self._send.write_all(b"".join(self._pending))
            self._pending.clear()

    async def write_eof(self) -> None:
        """Signal end of stream (half-close)."""
        await self.drain()
        await self._send.finish()
        self._closing = True

    async def aclose(self) -> None:
        """Close the stream gracefully."""
        await self.write_eof()
        await self._send.stopped()

    def close(self) -> None:
        """Mark stream as closing."""
        self._closing = True

    async def wait_closed(self) -> None:
        """Wait for stream to fully close."""
        if not self._closing:
            await self.aclose()

    def is_closing(self) -> bool:
        return self._closing

    def can_write_eof(self) -> bool:
        return True

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        return default


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
        return StreamReader(bi.recv()), StreamWriter(bi.send())

    async def accept_stream(self) -> tuple[StreamReader, StreamWriter]:
        """Accept an incoming bidirectional stream."""
        bi = await self._conn.accept_bi()
        return StreamReader(bi.recv()), StreamWriter(bi.send())

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
                reader = StreamReader(bi.recv())
                writer = StreamWriter(bi.send())
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
                    await writer.aclose()
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

    async def __aenter__(self) -> Server:
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
        Tuple of (StreamReader, StreamWriter)
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

    return StreamReader(bi.recv()), StreamWriter(bi.send())


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
