import argparse
import asyncio
import re


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    finally:
        writer.close()


async def _read_headers(reader: asyncio.StreamReader) -> bytes:
    return await reader.readuntil(b"\r\n\r\n")


def _rewrite_request_headers(headers: bytes, target_host: str, target_port: int) -> bytes:
    host_value = f"{target_host}:{target_port}".encode()
    return re.sub(
        rb"(?im)^Host:[^\r\n]*",
        b"Host: " + host_value,
        headers,
        count=1,
    )


def _rewrite_content_length(headers: bytes, body_length: int) -> bytes:
    content_length = f"Content-Length: {body_length}".encode()
    if re.search(rb"(?im)^Content-Length:", headers):
        return re.sub(
            rb"(?im)^Content-Length:[^\r\n]*",
            content_length,
            headers,
            count=1,
        )
    return headers[:-4] + b"\r\n" + content_length + b"\r\n\r\n"


async def _handle_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    target_host: str,
    target_port: int,
    advertised_host: str,
    advertised_port: int,
) -> None:
    try:
        request_headers = await _read_headers(client_reader)
        target_reader, target_writer = await asyncio.open_connection(
            target_host,
            target_port,
        )
        target_writer.write(
            _rewrite_request_headers(request_headers, target_host, target_port)
        )
        await target_writer.drain()

        response_headers = await _read_headers(target_reader)
    except (OSError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
        client_writer.close()
        return

    if response_headers.startswith(b"HTTP/1.1 101"):
        client_writer.write(response_headers)
        await client_writer.drain()
        await asyncio.gather(
            _pipe(client_reader, target_writer),
            _pipe(target_reader, client_writer),
            return_exceptions=True,
        )
        return

    content_length_match = re.search(
        rb"(?im)^Content-Length:\s*(\d+)\s*$",
        response_headers,
    )
    if content_length_match:
        body = await target_reader.readexactly(int(content_length_match.group(1)))
    else:
        body = await target_reader.read()

    advertised_endpoint = f"{advertised_host}:{advertised_port}".encode()
    body = body.replace(
        f"{target_host}:{target_port}".encode(),
        advertised_endpoint,
    ).replace(
        f"localhost:{target_port}".encode(),
        advertised_endpoint,
    )
    response_headers = _rewrite_content_length(response_headers, len(body))
    client_writer.write(response_headers + body)
    await client_writer.drain()
    client_writer.close()
    target_writer.close()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen-host", default="0.0.0.0")
    parser.add_argument("--listen-port", type=int, default=9223)
    parser.add_argument("--target-host", default="127.0.0.1")
    parser.add_argument("--target-port", type=int, default=9222)
    parser.add_argument("--advertised-host", default="host.docker.internal")
    parser.add_argument("--advertised-port", type=int)
    args = parser.parse_args()
    advertised_port = args.advertised_port or args.listen_port

    server = await asyncio.start_server(
        lambda reader, writer: _handle_client(
            reader,
            writer,
            args.target_host,
            args.target_port,
            args.advertised_host,
            advertised_port,
        ),
        args.listen_host,
        args.listen_port,
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
