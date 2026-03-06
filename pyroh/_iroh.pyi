from __future__ import annotations

from typing import Optional, final

@final
class IrohSendStream:
    """Write side of a QUIC stream."""

    @property
    def id(self) -> int:
        """The QUIC stream ID."""
        ...

    async def write(self, data: bytes) -> None:
        """Write all of ``data`` to the stream.

        Completes only after all bytes have been handed to the transport.

        Args:
            data: The bytes to send.

        Raises:
            IOError: if the stream or connection is in an error state.
        """
        ...

    async def finish(self) -> None:
        """Send a FIN, signalling the end of the send side of the stream.

        After this call no further data may be written. The remote peer's
        ``StreamReader`` will return ``b""`` once it has consumed all
        preceding data.

        Raises:
            IOError: if the stream has already been finished or reset.
        """
        ...

@final
class IrohRecvStream:
    """Read side of a QUIC stream."""

    @property
    def id(self) -> int:
        """The QUIC stream ID."""
        ...

    async def read(self, max_bytes: int) -> bytes:
        """Read up to ``max_bytes`` bytes from the stream.

        Returns ``b""`` when the remote peer has sent a FIN and all data
        has been consumed.

        Args:
            max_bytes: Maximum number of bytes to return in one call.

        Raises:
            IOError: if the stream was reset by the peer or the connection
                was lost before EOF.
        """
        ...

@final
class IrohConnection:
    """An established QUIC connection to a remote iroh peer.

    A single connection multiplexes any number of independent streams.
    Use :meth:`open_bi`, :meth:`accept_bi`, :meth:`open_uni`, and
    :meth:`accept_uni` to create streams; use :meth:`close` or
    :meth:`abort` to tear the connection down.
    """

    @property
    def alpn(self) -> bytes:
        """The ALPN protocol label negotiated for this connection."""
        ...

    @property
    def remote_node_id(self) -> str:
        """The node ID (public key) of the remote peer as a hex string."""
        ...

    async def open_bi(self) -> tuple[IrohSendStream, IrohRecvStream]:
        """Open a bidirectional stream initiated by this side.

        Returns:
            A ``(send, recv)`` pair for the new stream.

        Raises:
            IOError: if the connection is closed or the peer's stream
                concurrency limit has been reached.
        """
        ...

    async def accept_bi(self) -> tuple[IrohSendStream, IrohRecvStream]:
        """Accept a bidirectional stream initiated by the remote peer.

        Blocks until the remote opens a new bidirectional stream.

        Returns:
            A ``(send, recv)`` pair for the accepted stream.

        Raises:
            IOError: if the connection is closed before a stream arrives.
        """
        ...

    async def open_uni(self) -> IrohSendStream:
        """Open a send-only unidirectional stream initiated by this side.

        Raises:
            IOError: if the connection is closed or the peer's stream
                concurrency limit has been reached.
        """
        ...

    async def accept_uni(self) -> IrohRecvStream:
        """Accept a receive-only unidirectional stream initiated by the remote peer.

        Blocks until the remote opens a new unidirectional stream.

        Raises:
            IOError: if the connection is closed before a stream arrives.
        """
        ...

    async def close(self, error_code: int = 0, reason: bytes = b"") -> None:
        """Close the connection with a QUIC application-level error code.

        Sends the close signal to the remote peer and waits until the
        connection is fully torn down.

        Args:
            error_code: A 32-bit application error code sent to the peer.
            reason:     An opaque byte string describing the reason for
                        closing (best-effort, may be truncated by the
                        transport).
        """
        ...

@final
class IrohEndpoint:
    """A bound iroh QUIC endpoint.

    Each endpoint has a stable identity — a keypair whose public half is
    the *node ID*. Remote peers connect to this endpoint by node ID; iroh
    handles NAT traversal and relay fallback automatically.

    Create an endpoint with :meth:`bind`.
    """

    @staticmethod
    async def bind(alpns: list[bytes], key: Optional[bytes] = None) -> IrohEndpoint:
        """Bind a new endpoint to the network and return it.

        Generates a fresh keypair unless ``key`` is provided. Returns as
        soon as the endpoint is bound — does **not** wait for relay contact.
        Use :meth:`wait_online` afterwards if you need the endpoint to be
        reachable before proceeding.

        Args:
            alpns: ALPN protocol labels to accept on the inbound side.
                   Only connections advertising one of these labels will be
                   accepted.
            key:   Optional 32-byte secret key. If provided, the endpoint
                   will use it as its identity (allowing a stable node ID
                   across restarts). If ``None``, a new key is generated.

        Returns:
            A bound :class:`IrohEndpoint`, not yet necessarily reachable.

        Raises:
            ValueError: if ``key`` is provided but is not exactly 32 bytes.
            IOError:    if the underlying endpoint cannot be bound.
        """
        ...

    async def wait_online(self) -> None:
        """Wait until the endpoint has contacted a relay and is reachable.

        Resolves as soon as the endpoint receives confirmation from a relay
        server. Has no built-in timeout — wrap with ``asyncio.wait_for`` if
        you need one.
        """
        ...

    @property
    def addr(self) -> str:
        """The endpoint's node ID (public key) as a hex string.

        This is the address remote peers need in order to connect.
        """
        ...

    @property
    def secret_key(self) -> bytes:
        """The endpoint's 32-byte secret key.

        Store this and pass it back to :meth:`bind` as ``key`` to reuse
        the same node ID across process restarts.
        """
        ...

    async def accept(self) -> IrohConnection:
        """Accept the next incoming connection.

        Accepts connections for any ALPN registered at :meth:`bind` time
        (or subsequently via :meth:`set_alpns`).

        Raises:
            IOError: if the endpoint has been closed.
        """
        ...

    async def connect(self, addr: str, alpn: bytes) -> IrohConnection:
        """Connect to a remote iroh peer.

        Args:
            addr: Node ID of the remote peer as a hex string (the value of
                  the remote endpoint's :attr:`addr` property).
            alpn: ALPN protocol label to use for this connection. The remote
                  endpoint must have registered this ALPN.

        Returns:
            An established :class:`IrohConnection`.

        Raises:
            ValueError: if ``addr`` cannot be parsed as a node ID.
            IOError:    if the connection attempt fails (e.g. peer
                        unreachable, ALPN rejected).
        """
        ...

    def set_alpns(self, alpns: list[bytes]) -> None:
        """Replace the set of accepted ALPNs at runtime.

        Useful for protocol version upgrades without rebinding the endpoint.
        Connections already in progress are not affected.

        Args:
            alpns: The new set of ALPN labels to accept.
        """
        ...

    async def close(self) -> None:
        """Shut down the endpoint and close all active connections."""
        ...
