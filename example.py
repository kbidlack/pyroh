import asyncio

import pyroh


async def echo_handler(reader: pyroh.StreamReader, writer: pyroh.StreamWriter) -> None:
    data = await reader.read(1024)
    if data:
        print(f"[server] Received: {data!r}")
        writer.write(data)
        await writer.drain()
    await writer.aclose()


async def main() -> None:
    print("pyroh Echo Demo")
    print("=" * 50)

    # Start server
    server = await pyroh.serve(echo_handler, alpn=b"echo/1")
    server_addr = await server.node_addr()

    print(f"Server started with node ID: {server.node_id[:32]}...")
    print()

    # Connect client
    print("Connecting client...")
    reader, writer = await pyroh.connect(server_addr, alpn=b"echo/1")

    # Send message
    message = b"Hello from pyroh!"
    print(f"[client] Sending: {message!r}")
    writer.write(message)
    await writer.drain()
    await writer.write_eof()

    # Receive echo
    response = await reader.read(1024)
    print(f"[client] Received: {response!r}")

    # Verify
    assert response == message, "Echo failed!"
    print()
    print("Echo successful!")
    print("=" * 50)

    # Cleanup
    server.close()
    await server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
