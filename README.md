# pyroh

A friendly, minimalistic Python wrapper for [iroh](https://iroh.computer) that provides asyncio-like StreamReader/StreamWriter interfaces for peer-to-peer connections.

Disclaimer: Most of this code was written by AI and so I will not claim to guarantee anything about it. I mostly just need it for another personal project


## Installation

```bash
pip install git+https://github.com/kbidlack/pyroh
```


## Quick Start

### Create a Server

```python
import asyncio
import pyroh

async def echo_handler(reader: pyroh.StreamReader, writer: pyroh.StreamWriter):
    data = await reader.read(1024)
    writer.write(data)
    await writer.drain()
    await writer.aclose()

async def main():
    server = await pyroh.serve(echo_handler, alpn=b"echo/1")
    
    # share this with clients
    print(f"Node ID: {server.node_id}")
    addr = await server.node_addr()
    print(f"Full address: {addr}")
    
    await server.serve_forever()

asyncio.run(main())
```

### Connect to a Peer

```python
import asyncio
import pyroh

async def main():
    # get address from the server (node_id, relay_url, direct_addrs)
    addr = pyroh.node_addr("server-node-id-here")
    
    # connect and get asyncio-like streams
    reader, writer = await pyroh.connect(addr, alpn=b"echo/1")
    
    # use like asyncio streams
    writer.write(b"Hello, peer!")
    await writer.drain()
    
    response = await reader.read(1024)
    print(f"Received: {response}")
    
    await writer.aclose()

asyncio.run(main())
```

### Multiple Streams per Connection

```python
async def main():
    addr = pyroh.node_addr("peer-node-id")
    
    # open a connection that supports multiple streams
    conn = await pyroh.open_connection(addr, alpn=b"myapp/1")
    
    # open multiple streams on the same connection
    reader1, writer1 = await conn.open_stream()
    reader2, writer2 = await conn.open_stream()
    
    # use streams independently
    writer1.write(b"stream 1 data")
    writer2.write(b"stream 2 data")
    await writer1.drain()
    await writer2.drain()
```

### Line-Based Protocol

```python
async def chat_handler(reader: pyroh.StreamReader, writer: pyroh.StreamWriter):
    writer.write(b"Welcome!\n")
    await writer.drain()
    
    while True:
        line = await reader.readline()
        if not line:
            break
        
        response = f"You said: {line.decode().strip()}\n".encode()
        writer.write(response)
        await writer.drain()
```

## API Reference

### Top-Level Functions

#### `serve(handler, alpn=b"pyroh", *, node=None) -> Server`

Create and start a server that accepts connections.

- `handler`: Async function `async def handler(reader, writer)`
- `alpn`: Protocol identifier for ALPN negotiation
- `node`: Optional existing Iroh node

#### `connect(addr, alpn=b"pyroh", *, node=None) -> (StreamReader, StreamWriter)`

Connect to a peer and open a bidirectional stream.

- `addr`: NodeAddr or node ID string
- `alpn`: Protocol identifier
- `node`: Optional existing Iroh node

#### `open_connection(addr, alpn=b"pyroh", *, node=None) -> Connection`

Connect and return a Connection for opening multiple streams.

#### `node_addr(node_id, relay_url=None, addrs=None) -> NodeAddr`

Create a NodeAddr from components.

#### `setup_event_loop(loop=None)`

Initialize iroh's async callbacks. Called automatically by `serve()` and `connect()`.

### StreamReader

Mirrors `asyncio.StreamReader`:

| Method | Description |
|--------|-------------|
| `read(n=-1)` | Read up to n bytes (-1 = until EOF) |
| `readexactly(n)` | Read exactly n bytes or raise EOFError |
| `readline()` | Read until `\n` or EOF |
| `readuntil(sep=b"\n")` | Read until separator or raise EOFError |
| `read_to_end(limit)` | Read all remaining data |
| `at_eof()` | True if EOF reached and buffer empty |

### StreamWriter

Mirrors `asyncio.StreamWriter`:

| Method | Description |
|--------|-------------|
| `write(data)` | Buffer data for sending |
| `writelines(lines)` | Buffer multiple byte strings |
| `drain()` | Flush buffered data to network |
| `write_eof()` | Signal end of stream (half-close) |
| `aclose()` | Close gracefully (drain + finish + wait) |
| `close()` | Mark as closing immediately |
| `wait_closed()` | Wait for stream to fully close |
| `is_closing()` | True if stream is closing |
| `can_write_eof()` | Always True |

### Connection

Wrapper for multiplexed streams over a single connection.

| Property/Method | Description |
|-----------------|-------------|
| `remote_node_id` | Remote peer's node ID |
| `alpn` | Negotiated ALPN protocol |
| `open_stream()` | Open new bidirectional stream |
| `accept_stream()` | Accept incoming stream |
| `close(code, reason)` | Close connection |
| `closed()` | Wait for connection close |

### Server

| Property/Method | Description |
|-----------------|-------------|
| `node` | Underlying Iroh node |
| `alpn` | Protocol being served |
| `endpoint` | Network endpoint |
| `node_id` | This server's node ID |
| `node_addr()` | Full address for clients |
| `serve_forever()` | Run until cancelled |
| `close()` | Stop accepting connections |
| `wait_closed()` | Wait for shutdown |

Supports async context manager:

```python
async with await pyroh.serve(handler, alpn=b"myapp") as server:
    print(f"Serving on {server.node_id}")
    await server.serve_forever()
```

### Re-exported Types

- `Iroh` - Main iroh node class
- `NodeAddr` - Peer address (node ID + connection hints)
- `PublicKey` - Node identifier
