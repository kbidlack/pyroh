import asyncio

import pyroh


async def handle_connection(conn: pyroh.Connection, label: str) -> None:
    reader, writer = await conn.accept_bi()
    data = await reader.read(-1)
    print(f"  {label} received bi: {data!r}")
    writer.write(b"echo: " + data)
    await writer.drain()
    writer.write_eof()
    writer.close()
    await writer.wait_closed()

    uni_reader = await conn.accept_uni()
    ping = await uni_reader.read(-1)
    print(f"  {label} received uni: {ping!r}")


async def send_message(conn: pyroh.Connection, message: bytes) -> bytes:
    # bidirectional: send message, get echo back
    reader, writer = await conn.open_bi()
    writer.write(message)
    await writer.drain()
    writer.write_eof()
    response = await reader.read(-1)
    writer.close()
    await writer.wait_closed()

    # unidirectional: fire-and-forget ping
    uni_writer = await conn.open_uni()
    uni_writer.write(b"ping from " + message.split()[-1])
    await uni_writer.drain()
    uni_writer.write_eof()
    uni_writer.close()
    await uni_writer.wait_closed()

    return response


async def main():
    ep_a = await pyroh.Endpoint.bind()
    ep_b = await pyroh.Endpoint.bind()

    async with ep_a, ep_b:
        server_a = ep_a.start_server(lambda conn: handle_connection(conn, "A"))
        server_b = ep_b.start_server(lambda conn: handle_connection(conn, "B"))

        print(f"endpoint A id: {ep_a.id}")
        print(f"endpoint A addr: {ep_a.addr}")
        print(f"endpoint A ticket: {ep_a.ticket}")
        print(f"endpoint B id: {ep_b.id}")
        print(f"endpoint B addr: {ep_b.addr}")
        print(f"endpoint B ticket: {ep_b.ticket}")

        async def connect_and_send(ep, remote_addr, message):
            conn = await ep.connect(remote_addr)
            return await send_message(conn, message)

        resp_a, resp_b = await asyncio.gather(
            connect_and_send(ep_a, ep_b.ticket, b"hello from A"),
            connect_and_send(ep_b, ep_a.ticket, b"hello from B"),
        )

        print(f"A got: {resp_a!r}")
        print(f"B got: {resp_b!r}")

        server_a.close()
        server_b.close()


if __name__ == "__main__":
    asyncio.run(main())
