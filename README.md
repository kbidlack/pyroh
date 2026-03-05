# pyroh

A friendly, asyncio-like Python wrapper for [iroh](https://iroh.computer).

Pyroh wraps iroh's rust bindings via [pyo3](https://pyo3.rs) and integrates with `asyncio` using the standard `StreamReader`/`StreamWriter` interface.

---

## Requirements

- Python ≥ 3.14

---

## Installation

```sh
pip install pyroh
```

To build from source:

```sh
maturin develop
```

---

## Quickstart

```python
import asyncio
import pyroh

ALPN = b"myapp/1"

async def handle_connection(conn: pyroh.Connection) -> None:
    reader, writer = await conn.accept_bi()
    data = await reader.read(1024)
    print(f"server got: {data!r}")
    writer.write(b"hello back")
    writer.close()
    await writer.wait_closed()

async def main():
    # Bind a server endpoint. A fresh keypair is generated automatically.
    server = await pyroh.Endpoint.bind(alpns=[ALPN])
    async with server:
        srv = server.start_server(handle_connection)

        # Bind a client endpoint and connect by node ID.
        # By default, this hangs until the endpoint is online.
        # You can disable this behavior by doing
        # `endpoint = await pyroh.Endpoint.bind(wait_online=False)`
        # and then awaiting `endpoint.wait_online()` later
        client = await pyroh.Endpoint.bind(alpns=[ALPN])
        async with client:
            conn = await client.connect(server.id, alpn=ALPN)
            async with conn:
                reader, writer = await conn.open_bi()
                writer.write(b"hello")
                writer.close()
                response = await reader.read(1024)
                print(f"client got: {response!r}")

        srv.close()

asyncio.run(main())
```

You can view more examples in the [examples](./examples) folder

---

## API

### `pyroh.Endpoint`

The main entry point. Represents a local QUIC endpoint.

```python
endpoint = await pyroh.Endpoint.bind(
    alpns=[b"myapp/1"],  # ALPNs to accept; default is [b"pyroh/1"]
    key=None,            # 32-byte secret key, or None to generate a fresh one
    wait_online=True,    # if False, return immediately and call wait_online() manually
)
```

#### Properties

| Property     | Type    | Description                                                                                              |
| ------------ | ------- | -------------------------------------------------------------------------------------------------------- |
| `id`         | `str`   | Node ID (public key) as a hex string. Pass this to remote peers so they can connect.                     |
| `secret_key` | `bytes` | The 32-byte secret key for this endpoint's identity. Store it to reuse the same node ID across restarts. |

#### Methods

| Method                                              | Description                                                                                                          |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `await endpoint.connect(addr, *, alpn=b"pyroh/1")`  | Connect to a remote peer by node ID string. Returns a `Connection`.                                                  |
| `endpoint.start_server(handler)`                    | Start accepting connections, calling `handler(conn)` for each one. Returns a `Server`.                               |
| `await endpoint.wait_online()`                      | Wait until the endpoint has contacted a relay and is reachable. Only needed when `bind(wait_online=False)` was used. |
| `endpoint.set_alpns(alpns)`                         | Update the set of accepted ALPNs at runtime (e.g. for protocol upgrades).                                            |
| `await endpoint.close()` (or `async with endpoint`) | Shut down the endpoint.                                                                                              |

---

### `pyroh.Connection`

A QUIC connection to a remote peer. Connections multiplex streams — open as many as you need without the overhead of new connections.

#### Methods

| Method                                      | Description                                                                                 |
| ------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `await conn.open_bi()`                      | Open a bidirectional stream. Returns `(StreamReader, StreamWriter)`.                        |
| `await conn.accept_bi()`                    | Accept a bidirectional stream opened by the remote. Returns `(StreamReader, StreamWriter)`. |
| `await conn.open_uni()`                     | Open a send-only stream. Returns `StreamWriter`.                                            |
| `await conn.accept_uni()`                   | Accept a receive-only stream opened by the remote. Returns `StreamReader`.                  |
| `await conn.abort(error_code=0, reason="")` | Forcefully close the connection with an application error code.                             |
| `conn.close()`                              | Close all open streams on this connection.                                                  |
| `async with conn`                           | Context manager — calls `close()` and `wait_closed()` on exit.                              |

---

### `pyroh.Server`

Returned by `endpoint.start_server(handler)`. Accepts connections in a background task.

| Method                         | Description                                                             |
| ------------------------------ | ----------------------------------------------------------------------- |
| `await server.serve_forever()` | Block until the server is closed (e.g. from a signal handler).          |
| `server.close()`               | Stop accepting new connections. Does not close the underlying endpoint. |
| `server.id`                    | Node ID of the underlying endpoint (same as `endpoint.id`).             |

---
