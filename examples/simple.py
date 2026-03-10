import asyncio

import pyroh

ALPN = b"pyroh/1"

MESSAGE = b"hello from client"
RESPONSE = b"hello from server"
NOTIFICATION = b"one-way notification"


async def handle_connection(conn: pyroh.Connection) -> None:
    # bidirectional stream
    reader, writer = await conn.accept_bi()
    data = await reader.read(1024)
    print(f"  server received {data!r} on bi")
    writer.write(RESPONSE)
    writer.close()
    await writer.wait_closed()

    # unidirectional stream (recv-only on server side)
    uni_reader = await conn.accept_uni()
    notif = await uni_reader.read(-1)
    assert notif == NOTIFICATION, f"server got unexpected notification: {notif!r}"
    print(f"  server received uni: {notif!r}")


async def run_client(addr: str):
    endpoint = await pyroh.Endpoint.bind(alpns=[ALPN])
    async with endpoint:
        conn = await endpoint.connect(addr, alpn=ALPN)
        async with conn:
            # bidirectional stream
            reader, writer = await conn.open_bi()
            writer.write(MESSAGE)
            data = await reader.read(1024)
            assert data == RESPONSE, f"client got unexpected data: {data!r}"
            print(f"  client received bi: {data!r}")
            writer.close()
            await writer.wait_closed()

            # unidirectional stream (send-only on client side)
            uni_writer = await conn.open_uni()
            uni_writer.write(NOTIFICATION)
            await uni_writer.drain()
            uni_writer.close()
            await uni_writer.wait_closed()


async def main():
    endpoint = await pyroh.Endpoint.bind(
        alpns=[ALPN],
        key=pyroh.SecretKey.from_bytes(b"thisisasecretkeythatis32bytes..."),
    )
    async with endpoint:
        server = endpoint.start_server(handle_connection)
        addr = server.id
        print(f"server listening at {addr}")

        await run_client(addr)
        server.close()

    print("done!")


if __name__ == "__main__":
    asyncio.run(main())
