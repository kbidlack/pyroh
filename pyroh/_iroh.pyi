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
        """Write all of `data` to the stream.

        Completes only after all bytes have been handed to the transport.

        Raises:
            IOError: if the stream or connection is in an error state.
        """
        ...

    async def finish(self) -> None:
        """Send a FIN, signalling the end of the stream.

        After this call no further data may be written.

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
        """Read up to `max_bytes` bytes from the stream.

        Returns `b""` when the remote peer has sent a FIN.

        Raises:
            IOError: if the stream was reset by the peer or the connection
                was lost.
        """
        ...

@final
class IrohConnection:
    """An established QUIC connection to a remote iroh peer."""

    @property
    def alpn(self) -> bytes:
        """The ALPN protocol negotiated for this connection."""
        ...

    async def open_bi(self) -> tuple[IrohSendStream, IrohRecvStream]:
        """Open a bidirectional stream initiated by this side.

        Returns:
            A `(send, recv)` pair for the new stream.

        Raises:
            IOError: if the connection is closed or the stream limit is reached.
        """
        ...

    async def accept_bi(self) -> tuple[IrohSendStream, IrohRecvStream]:
        """Accept a bidirectional stream initiated by the remote peer.

        Blocks until the remote opens a new stream.

        Raises:
            IOError: if the connection is closed before a stream arrives.
        """
        ...

    async def open_uni(self) -> IrohSendStream:
        """Open a send-only unidirectional stream initiated by this side.

        Raises:
            IOError: if the connection is closed or the stream limit is reached.
        """
        ...

    async def accept_uni(self) -> IrohRecvStream:
        """Accept a receive-only unidirectional stream initiated by the remote peer.

        Blocks until the remote opens a new stream.

        Raises:
            IOError: if the connection is closed before a stream arrives.
        """
        ...

    async def close(self, error_code: int = 0, reason: bytes = b"") -> None:
        """Close the connection with a QUIC application-level error code.

        Sends the close signal and waits until the connection is fully closed.

        Args:
            error_code: A 32-bit application error code sent to the peer.
            reason:     An opaque byte string describing the reason.
        """
        ...

@final
class IrohEndpoint:
    """A bound iroh QUIC endpoint.

    Use `bind` to create an instance.
    """

    @staticmethod
    async def bind(alpns: list[bytes], key: Optional[bytes]) -> IrohEndpoint:
        """Bind a new endpoint that accepts connections using any of the given ALPNs.

        Args:
            alpns: ALPN protocol labels to accept on the inbound side.
            key: An optional private key for the endpoint, as raw bytes.
                If provided, these bytes will be used as the node's private key / identity;
                if None, a new private key will be generated for the endpoint.

        Raises:
            IOError: if the underlying endpoint cannot be bound.
        """
        ...

    async def wait_online(self) -> None:
        """Wait until the endpoint has contacted a relay.

        Resolves once the endpoint is reachable from the internet via relay.
        Has no built-in timeout.
        """
        ...

    @property
    def addr(self) -> str:
        """The endpoint's node ID (public key) as a hex string."""
        ...

    @property
    def secret_key(self) -> bytes:
        """The endpoint's secret key as a length 32 byte string."""
        ...

    async def accept(self) -> IrohConnection:
        """Accept the next incoming connection.

        Accepts connections for any ALPN registered at `bind` time.

        Raises:
            IOError: if the endpoint has been closed.
        """
        ...

    async def connect(self, addr: str, alpn: bytes) -> IrohConnection:
        """Connect to a remote iroh peer.

        Args:
            addr: The node ID of the remote peer (hex string).
            alpn: The ALPN protocol label to use.

        Raises:
            ValueError: if `addr` cannot be parsed as a node ID.
            IOError:    if the connection attempt fails.
        """
        ...

    def set_alpns(self, alpns: list[bytes]) -> None:
        """Update the set of accepted ALPNs at runtime."""
        ...

    async def close(self) -> None:
        """Shut down the endpoint and close all connections."""
        ...
