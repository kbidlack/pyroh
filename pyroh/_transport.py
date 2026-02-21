import asyncio

from ._iroh import IrohRecvStream, IrohSendStream


class _RecvMixin:
    _loop: asyncio.AbstractEventLoop
    _protocol: asyncio.StreamReaderProtocol
    _recv: IrohRecvStream
    _closing: bool
    _closed: asyncio.Event
    _recv_task: asyncio.Task[None]

    def _start_recv(self):
        self._recv_task = self._loop.create_task(self._recv_loop())

    def _close_recv(self):
        if hasattr(self, "_recv_task"):
            self._recv_task.cancel()

    async def _recv_loop(self) -> None:
        try:
            while not self._closing:
                data = await self._recv.read(65536)
                if not data:
                    self._protocol.eof_received()
                    break
                self._protocol.data_received(data)
        except asyncio.CancelledError:
            self._mark_closed()
            raise
        except Exception as exc:
            self._protocol.connection_lost(exc)
            self._mark_closed()
            return
        self._on_recv_done()

    def _on_recv_done(self):
        self._protocol.connection_lost(None)
        self._mark_closed()

    def _mark_closed(self):
        self._closed.set()


class _SendMixin:
    _loop: asyncio.AbstractEventLoop
    _protocol: asyncio.BaseProtocol
    _send: IrohSendStream
    _closing: bool
    _closed: asyncio.Event

    def write(self, data: bytes | bytearray | memoryview):
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(
                f"Data argument must be a bytes, bytearray, or memoryview "
                f"object, not {type(data).__name__!r}"
            )
        asyncio.ensure_future(self._send.write(bytes(data)), loop=self._loop)

    def write_eof(self):
        asyncio.ensure_future(self._finish_send(), loop=self._loop)

    def can_write_eof(self):
        return True

    def _close_send(self):
        asyncio.ensure_future(self._finish_send(), loop=self._loop)

    async def _finish_send(self) -> None:
        await self._send.finish()
        self._on_send_done()

    def _on_send_done(self):
        self._protocol.connection_lost(None)
        self._mark_closed()

    def _mark_closed(self):
        self._closed.set()


class IrohRecvTransport(_RecvMixin, asyncio.ReadTransport):
    def __init__(self, loop, protocol, rust_recv: IrohRecvStream):
        super().__init__()
        self._loop = loop
        self._protocol = protocol
        self._recv = rust_recv
        self._closing = False
        self._closed = asyncio.Event()

    def start(self):
        self._start_recv()

    def set_protocol(self, protocol):
        self._protocol = protocol

    def get_protocol(self):
        return self._protocol

    def is_closing(self):
        return self._closing

    def close(self):
        if not self._closing:
            self._closing = True
            self._close_recv()

    async def wait_closed(self):
        await self._closed.wait()


class IrohSendTransport(_SendMixin, asyncio.WriteTransport):
    def __init__(self, loop, protocol, rust_send: IrohSendStream):
        super().__init__()
        self._loop = loop
        self._protocol = protocol
        self._send = rust_send
        self._closing = False
        self._closed = asyncio.Event()

    def set_protocol(self, protocol):
        self._protocol = protocol

    def get_protocol(self):
        return self._protocol

    def is_closing(self):
        return self._closing

    def close(self):
        if not self._closing:
            self._closing = True
            self._close_send()

    async def wait_closed(self):
        await self._closed.wait()


class IrohStreamTransport(_RecvMixin, _SendMixin, asyncio.Transport):
    def __init__(
        self, loop, protocol, rust_send: IrohSendStream, rust_recv: IrohRecvStream
    ):
        super().__init__()
        self._loop = loop
        self._protocol = protocol
        self._send = rust_send
        self._recv = rust_recv
        self._closing = False
        self._write_done = False
        self._closed = asyncio.Event()

    def start(self):
        self._start_recv()

    def set_protocol(self, protocol):
        self._protocol = protocol

    def get_protocol(self):
        return self._protocol

    def is_closing(self):
        return self._closing

    def close(self):
        if self._closing:
            return
        self._closing = True
        self._close_send()
        self._close_recv()

    def _on_send_done(self):
        self._write_done = True
        if self._closed.is_set():
            self._protocol.connection_lost(None)

    def _on_recv_done(self):
        if self._write_done:
            self._protocol.connection_lost(None)
        self._mark_closed()

    async def wait_closed(self):
        await self._closed.wait()
