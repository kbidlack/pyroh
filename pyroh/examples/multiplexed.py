import asyncio

import pyroh

NUM_STREAMS = 5


async def handle_bi(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    request = await reader.read(-1)
    stream_id, _, payload = request.partition(b":")
    print(f"  server bi stream {stream_id.decode()}: got {payload!r}")
    await asyncio.sleep(0.01)  # simulate work
    writer.write(b"response to " + payload)
    await writer.drain()
    writer.write_eof()
    writer.close()
    await writer.wait_closed()


async def handle_uni(reader: asyncio.StreamReader) -> None:
    data = await reader.read(-1)
    print(f"  server uni stream: got {data!r}")


async def handle_connection(conn: pyroh.Connection) -> None:
    tasks = []
    for _ in range(NUM_STREAMS):
        reader, writer = await conn.accept_bi()
        tasks.append(asyncio.create_task(handle_bi(reader, writer)))
    uni_reader = await conn.accept_uni()
    tasks.append(asyncio.create_task(handle_uni(uni_reader)))
    await asyncio.gather(*tasks)


async def open_bi_stream(conn: pyroh.Connection, stream_id: int) -> bytes:
    reader, writer = await conn.open_bi()
    writer.write(f"{stream_id}:".encode() + f"message {stream_id}".encode())
    await writer.drain()
    writer.write_eof()
    response = await reader.read(-1)
    writer.close()
    await writer.wait_closed()
    return response


async def open_uni_stream(conn: pyroh.Connection) -> None:
    writer = await conn.open_uni()
    writer.write(b"broadcast from client")
    await writer.drain()
    writer.write_eof()
    writer.close()
    await writer.wait_closed()


async def main():
    server_ep = await pyroh.Endpoint.bind()
    client_ep = await pyroh.Endpoint.bind()

    async with server_ep, client_ep:
        server = server_ep.start_server(handle_connection)
        print(f"server: {server_ep.id}")

        conn = await client_ep.connect(server_ep.id)
        async with conn:
            print(
                f"opening {NUM_STREAMS} bi streams + 1 uni stream over one connection..."
            )
            results = await asyncio.gather(
                *[open_bi_stream(conn, i) for i in range(NUM_STREAMS)],  # type: ignore
                open_uni_stream(conn),
            )

        responses = results[:NUM_STREAMS]
        for i, resp in enumerate(responses):
            print(f"  client bi stream {i}: got {resp!r}")

        server.close()


if __name__ == "__main__":
    asyncio.run(main())
