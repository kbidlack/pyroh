import asyncio
import json
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any, Optional

from . import _iroh
from ._transport import IrohRecvTransport, IrohSendTransport, IrohStreamTransport

DEFAULT_ALPN = b"pyroh/1"

type ConnectionHandler = Callable[[Connection], Coroutine[Any, Any, Any]]


@dataclass(frozen=True)
class EndpointAddrEntry:
    """A single address entry inside an EndpointAddr."""

    kind: str
    value: str


@dataclass(frozen=True)
class EndpointAddr:
    """Dial information for an endpoint.

    Includes the endpoint's node ID plus optional relay and direct address
    details. When built from full address info, this is the most reliable
    way to connect without discovery.
    """

    id: str
    addrs: tuple[EndpointAddrEntry, ...]
    raw: str = field(repr=False)
    kind: str = field(default="json", repr=False)

    @classmethod
    def from_json(cls, raw: str) -> "EndpointAddr":
        data = json.loads(raw)
        node_id = data.get("id")
        if not isinstance(node_id, str):
            raise ValueError("EndpointAddr JSON is missing a string 'id' field")

        entries: list[EndpointAddrEntry] = []
        addrs = data.get("addrs", [])
        if isinstance(addrs, list):
            for addr in addrs:
                if isinstance(addr, dict) and len(addr) == 1:
                    kind, value = next(iter(addr.items()))
                    entries.append(EndpointAddrEntry(kind=str(kind), value=str(value)))
                else:
                    entries.append(
                        EndpointAddrEntry(kind="Unknown", value=json.dumps(addr))
                    )

        return cls(id=node_id, addrs=tuple(entries), raw=raw, kind="json")

    @classmethod
    def from_id(cls, node_id: str) -> "EndpointAddr":
        return cls(id=node_id, addrs=tuple(), raw=node_id, kind="id")

    def to_json(self) -> str:
        """Return the dial string to pass to connect().

        For full addresses this is the JSON form; for ID-only addresses this
        is just the node ID.
        """
        return self.raw

    def __str__(self) -> str:
        return self.raw


class SecretKey:
    """An Ed25519 secret key representing a node's identity.

    Use :meth:`generate` to create a new random key, or :meth:`from_bytes`
    to restore a previously saved key::

        # Generate a fresh key
        key = pyroh.SecretKey.generate()
        print(key.node_id)       # hex string node ID
        print(key.node_id_bytes) # raw 32-byte public key

        # Persist and restore
        saved = bytes(key)
        key = pyroh.SecretKey.from_bytes(saved)

    Pass to :meth:`Endpoint.bind` to use a stable node identity::

        endpoint = await pyroh.Endpoint.bind(key=bytes(key))
    """

    _key: _iroh.IrohSecretKey

    def __init__(self, inner: _iroh.IrohSecretKey) -> None:
        self._key = inner

    @classmethod
    def generate(cls) -> SecretKey:
        """Generate a new random secret key.

        Returns:
            A freshly generated :class:`SecretKey`.
        """
        return cls(_iroh.IrohSecretKey.generate())

    @classmethod
    def from_bytes(cls, data: bytes) -> SecretKey:
        """Restore a secret key from its 32-byte representation.

        Args:
            data: The 32-byte secret key, as previously returned by
                  ``bytes(key)`` or :meth:`to_bytes`.

        Returns:
            The corresponding :class:`SecretKey`.

        Raises:
            ValueError: if ``data`` is not exactly 32 bytes.
        """
        return cls(_iroh.IrohSecretKey.from_bytes(data))

    def to_bytes(self) -> bytes:
        """Return the secret key as 32 raw bytes.

        Store these to persist the key across process restarts and pass
        them back to :meth:`from_bytes` to restore it.
        """
        return bytes(self._key.to_bytes())

    @property
    def node_id(self) -> str:
        """The node ID (public key) derived from this secret key, as a hex string.

        This is a stable identifier for the endpoint. If address discovery
        is configured, peers can connect using just this value. Otherwise
        they will need a full :class:`EndpointAddr` or ticket.
        """
        return self._key.node_id

    @property
    def node_id_bytes(self) -> bytes:
        """The node ID (public key) derived from this secret key, as 32 raw bytes."""
        return bytes(self._key.node_id_bytes)

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def __repr__(self) -> str:
        return f"SecretKey(node_id={self.node_id})"


class Endpoint:
    """A local iroh QUIC endpoint with a stable node identity.

    Each endpoint has a keypair; the public half is the *node ID*, which
    is a stable identifier. If address discovery is configured, remote
    peers can connect using just the node ID. Otherwise share the full
    :attr:`addr` or :attr:`ticket`.

    Create with :meth:`bind`. Use as an async context manager to ensure
    the endpoint is closed on exit::

        async with await pyroh.Endpoint.bind(alpns=[b"myapp/1"]) as ep:
            conn = await ep.connect(remote_id, alpn=b"myapp/1")
    """

    _endpoint: _iroh.IrohEndpoint

    @classmethod
    async def bind(
        cls,
        *,
        alpns: list[bytes] = [DEFAULT_ALPN],
        key: Optional[SecretKey] = None,
        wait_online: bool = True,
    ) -> Endpoint:
        """Bind a new endpoint to the network.

        By default, blocks until the endpoint has contacted a relay and is
        reachable from the internet. Pass ``wait_online=False`` to return
        immediately and call :meth:`wait_online` yourself when ready.

        Args:
            alpns:        ALPN protocol labels to accept on the inbound side.
                          Defaults to ``[b"pyroh/1"]``. Only connections
                          advertising one of these labels will be accepted.
            key:          Optional 32-byte secret key. Pass the value of a
                          previous endpoint's :attr:`secret_key` to reuse the
                          same node ID across restarts. If ``None``, a fresh
                          keypair is generated.
            wait_online:  If ``True`` (the default), wait until the endpoint
                          has contacted a relay before returning. If ``False``,
                          return immediately and call :meth:`wait_online`
                          manually when needed.

        Returns:
            A :class:`Endpoint`. If ``wait_online=True``, it is already
            reachable; otherwise call :meth:`wait_online` before accepting
            or initiating connections.

        Raises:
            ValueError: if ``key`` is provided but is not exactly 32 bytes.
            OSError:    if the underlying endpoint cannot be bound.
        """
        key_bytes = key.to_bytes() if key is not None else None
        iendpoint = await _iroh.IrohEndpoint.bind(alpns=alpns, key=key_bytes)
        ep = cls(iendpoint)
        if wait_online:
            await ep.wait_online()
        return ep

    def __init__(self, iendpoint: _iroh.IrohEndpoint):
        self._endpoint = iendpoint

    async def wait_online(self) -> None:
        """Wait until the endpoint has contacted a relay and is reachable.

        Only needed when the endpoint was created with ``wait_online=False``.
        Has no built-in timeout — wrap with ``asyncio.wait_for`` if you need
        one::

            ep = await pyroh.Endpoint.bind(wait_online=False)
            await asyncio.wait_for(ep.wait_online(), timeout=10)

        """
        await self._endpoint.wait_online()

    @property
    def id(self) -> str:
        """The endpoint's node ID (public key) as a hex string.

        This is a stable identifier for the endpoint. If address discovery
        is enabled, peers can connect using just this value. Otherwise
        share :attr:`addr` or :attr:`ticket`.
        """
        return self._endpoint.addr

    @property
    def addr(self) -> EndpointAddr:
        """The endpoint address (node ID + optional relay/direct addresses).

        If the underlying iroh binding exposes full address info, this
        includes the relay URL and direct IP addresses. Otherwise it falls
        back to an ID-only address (discovery required).
        """
        addr_info = getattr(self._endpoint, "addr_info", None)
        if callable(addr_info):
            return EndpointAddr.from_json(addr_info())
        return EndpointAddr.from_id(self._endpoint.addr)

    @property
    def addr_json(self) -> Optional[str]:
        """Return the raw JSON address info if available, else ``None``."""
        addr_info = getattr(self._endpoint, "addr_info", None)
        if callable(addr_info):
            return addr_info()
        return None

    @property
    def ticket(self) -> str:
        """A serialized endpoint ticket suitable for copy/paste sharing.

        Tickets embed the endpoint address and can be handed to remote
        peers to connect without additional discovery.
        """
        ticket = getattr(self._endpoint, "ticket", None)
        if ticket is None:
            raise NotImplementedError(
                "endpoint tickets are not supported by this pyroh build"
            )
        return ticket

    @property
    def secret_key(self) -> SecretKey:
        """The endpoint's 32-byte secret key.

        Store this and pass it back to :meth:`bind` as ``key`` to reuse
        the same node ID across process restarts.
        """
        return SecretKey.from_bytes(self._endpoint.secret_key)

    async def connect(
        self, addr: str | EndpointAddr, *, alpn: bytes = DEFAULT_ALPN
    ) -> Connection:
        """Connect to a remote iroh peer.

        Args:
            addr: Remote address information. This can be:
                  - a node ID hex string (requires address discovery),
                  - an :class:`EndpointAddr` instance (full dial info), or
                  - a serialized endpoint ticket string.
            alpn: ALPN protocol label to use. The remote endpoint must have
                  registered this label. Defaults to ``b"pyroh/1"``.

        Returns:
            An established :class:`Connection`.

        Raises:
            ValueError: if ``addr`` cannot be parsed.
            OSError:    if the connection attempt fails (e.g. peer
                        unreachable, ALPN rejected).
        """
        addr_value = addr.to_json() if isinstance(addr, EndpointAddr) else addr
        rust_conn = await self._endpoint.connect(addr_value, alpn)
        return Connection(rust_conn)

    def set_alpns(self, alpns: list[bytes]) -> None:
        """Replace the set of accepted ALPNs at runtime.

        Useful for protocol version upgrades without rebinding the endpoint.
        Connections already in progress are not affected.

        Args:
            alpns: The new set of ALPN labels to accept.
        """
        self._endpoint.set_alpns(alpns)

    def start_server(self, handler: ConnectionHandler) -> Server:
        """Start accepting incoming connections in a background task.

        Each accepted connection is passed to ``handler`` as an independent
        :class:`asyncio.Task`. The server runs until :meth:`Server.close`
        is called.

        Args:
            handler: An async callable ``(conn: Connection) -> None`` that
                     will be called for each accepted connection.

        Returns:
            A :class:`Server` instance. Call :meth:`Server.serve_forever`
            to block until shutdown, or :meth:`Server.close` to stop
            accepting new connections.
        """
        server = Server(self._endpoint)
        server.start(handler)
        return server

    async def __aenter__(self) -> Endpoint:
        return self

    async def __aexit__(self, *args):
        await self._endpoint.close()


class Connection:
    """A QUIC connection to a remote iroh peer.

    A single connection multiplexes any number of independent streams,
    so there is usually no need to open multiple connections to the same
    peer. Use :meth:`open_bi` / :meth:`accept_bi` for bidirectional
    streams and :meth:`open_uni` / :meth:`accept_uni` for unidirectional
    streams.

    Streams are exposed as standard ``asyncio.StreamReader`` /
    ``asyncio.StreamWriter`` objects.

    Use as an async context manager to ensure streams are closed on exit::

        async with conn:
            reader, writer = await conn.open_bi()
            ...
    """

    _conn: _iroh.IrohConnection

    def __init__(self, rust_conn: _iroh.IrohConnection):
        self._conn = rust_conn

        # (for now?) does not clean up on individual stream close, only on self.close()
        self._transports: set[
            IrohStreamTransport | IrohSendTransport | IrohRecvTransport
        ] = set()

    @property
    def remote_node_id(self) -> str:
        """The node ID (public key) of the remote peer as a hex string."""
        return self._conn.remote_node_id

    async def open_bi(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Open a bidirectional stream initiated by this side.

        Returns:
            A ``(reader, writer)`` pair for the new stream.

        Raises:
            OSError: if the connection is closed or the peer's stream
                     concurrency limit has been reached.
        """
        rust_send, rust_recv = await self._conn.open_bi()
        return await _init_streams(rust_send, rust_recv, self)

    async def accept_bi(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Accept a bidirectional stream initiated by the remote peer.

        Blocks until the remote opens a new bidirectional stream.

        Returns:
            A ``(reader, writer)`` pair for the accepted stream.

        Raises:
            OSError: if the connection is closed before a stream arrives.
        """
        rust_send, rust_recv = await self._conn.accept_bi()
        return await _init_streams(rust_send, rust_recv, self)

    async def open_uni(self) -> asyncio.StreamWriter:
        """Open a send-only unidirectional stream initiated by this side.

        Returns:
            A ``StreamWriter`` for sending data. The remote peer receives
            a corresponding read-only stream.

        Raises:
            OSError: if the connection is closed or the peer's stream
                     concurrency limit has been reached.
        """
        rust_send = await self._conn.open_uni()
        return _init_send_stream(rust_send, self)

    async def accept_uni(self) -> asyncio.StreamReader:
        """Accept a receive-only unidirectional stream initiated by the remote peer.

        Blocks until the remote opens a new unidirectional stream.

        Returns:
            A ``StreamReader`` for receiving data.

        Raises:
            OSError: if the connection is closed before a stream arrives.
        """
        rust_recv = await self._conn.accept_uni()
        return await _init_recv_stream(rust_recv, self)

    async def abort(self, error_code: int = 0, reason: str = "") -> None:
        """Forcefully close the connection with a QUIC application error code.

        Immediately terminates all streams and sends the error code and
        reason to the remote peer.

        Args:
            error_code: A 32-bit application error code sent to the peer.
            reason:     A human-readable description of the reason for
                        closing (best-effort, may be truncated).
        """
        await self._conn.close(error_code, reason.encode())

    def close(self) -> None:
        """Close all open streams on this connection.

        Does not wait for streams to finish draining. Use
        :meth:`wait_closed` afterwards if you need to ensure all data
        has been flushed, or use ``async with conn`` which does both.
        """
        for transport in self._transports:
            transport.close()

    async def wait_closed(self) -> None:
        """Wait until all streams on this connection have fully closed."""
        await asyncio.gather(*(t.wait_closed() for t in self._transports))

    async def __aenter__(self) -> Connection:
        return self

    async def __aexit__(self, *args):
        self.close()
        await self.wait_closed()


class Server:
    """Accepts incoming QUIC connections on an endpoint.

    Obtained from :meth:`Endpoint.start_server`. Runs a background task
    that accepts connections and dispatches each one to the handler
    coroutine supplied at construction time.

    Use :meth:`serve_forever` to block until shutdown, or call
    :meth:`close` to stop accepting new connections without closing the
    underlying endpoint::

        server = endpoint.start_server(handler)
        await server.serve_forever()  # blocks until server.close() is called
    """

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
        """Node ID of the underlying endpoint (same as ``Endpoint.id``)."""
        return self._endpoint.addr

    @property
    def addr(self) -> EndpointAddr:
        """Address of the underlying endpoint (same as ``Endpoint.addr``)."""
        addr_info = getattr(self._endpoint, "addr_info", None)
        if callable(addr_info):
            return EndpointAddr.from_json(addr_info())
        return EndpointAddr.from_id(self._endpoint.addr)

    async def _accept(self) -> Connection:
        if self._closed:
            raise OSError("server is closed")

        rust_conn = await self._endpoint.accept()
        return Connection(rust_conn)

    def start(self, handler: ConnectionHandler) -> None:
        self.handler = handler
        self.server_task = asyncio.create_task(self._serve())

    async def _serve(self) -> None:
        """Accept connections in a loop, spawning handler(conn) for each one."""
        while not self._closed:
            try:
                conn = await self._accept()
            except OSError:
                break

            if self.handler is not None:
                asyncio.create_task(self.handler(conn))
            else:
                raise RuntimeError("server started without a handler")

    async def serve_forever(self) -> None:
        """Block until the server is closed.

        Useful as a top-level await in scripts or services::

            async with await pyroh.Endpoint.bind(alpns=[ALPN]) as ep:
                server = ep.start_server(handler)
                await server.serve_forever()

        Raises:
            RuntimeError: if ``serve_forever`` is already running for this server.
        """
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

    def close(self) -> None:
        """Stop accepting new connections.

        The background accept task is stopped. Connections that are
        already established are not affected, and the underlying endpoint
        remains open.
        """
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
    rust_send: _iroh.IrohSendStream,
    rust_recv: _iroh.IrohRecvStream,
    conn: Connection,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    loop = asyncio.get_event_loop()

    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)

    transport = IrohStreamTransport(loop, protocol, rust_send, rust_recv)
    protocol.connection_made(transport)
    transport.start()
    conn._transports.add(transport)

    writer = asyncio.StreamWriter(transport, protocol, reader, loop)

    return reader, writer


def _init_send_stream(
    rust_send: _iroh.IrohSendStream,
    conn: Connection,
) -> asyncio.StreamWriter:
    loop = asyncio.get_event_loop()

    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)

    transport = IrohSendTransport(loop, protocol, rust_send)
    protocol.connection_made(transport)
    conn._transports.add(transport)

    return asyncio.StreamWriter(transport, protocol, reader, loop)


async def _init_recv_stream(
    rust_recv: _iroh.IrohRecvStream,
    conn: Connection,
) -> asyncio.StreamReader:
    loop = asyncio.get_event_loop()

    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)

    transport = IrohRecvTransport(loop, protocol, rust_recv)
    protocol.connection_made(transport)
    transport.start()
    conn._transports.add(transport)

    return reader
