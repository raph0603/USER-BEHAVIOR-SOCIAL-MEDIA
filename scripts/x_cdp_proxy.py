import argparse
import asyncio
import json
import os
import re
import subprocess
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


class EdgeController:
    def __init__(
        self,
        target_host: str,
        target_port: int,
        edge_path: Path,
        profile_path: Path,
        startup_timeout: int,
    ) -> None:
        self.target_host = target_host
        self.target_port = target_port
        self.edge_path = edge_path
        self.profile_path = profile_path
        self.startup_timeout = startup_timeout
        self.pid_path = profile_path.parent / "x-edge.pid"
        self.mode_path = profile_path.parent / "x-edge-mode.txt"
        self.lock = asyncio.Lock()

    async def _target_is_ready(self) -> bool:
        try:
            reader, writer = await asyncio.open_connection(
                self.target_host,
                self.target_port,
            )
            writer.write(
                b"GET /json/version HTTP/1.1\r\n"
                + f"Host: {self.target_host}:{self.target_port}\r\n".encode()
                + b"Connection: close\r\n\r\n"
            )
            await writer.drain()
            response_headers = await asyncio.wait_for(
                reader.readuntil(b"\r\n\r\n"),
                timeout=3,
            )
            writer.close()
            await writer.wait_closed()
            return response_headers.startswith(b"HTTP/1.1 200")
        except (
            OSError,
            asyncio.IncompleteReadError,
            asyncio.LimitOverrunError,
            asyncio.TimeoutError,
        ):
            return False

    def _current_mode(self) -> bool | None:
        try:
            return self.mode_path.read_text(encoding="utf-8").strip() == "headless"
        except OSError:
            return None

    def _stop_managed_edge(self) -> None:
        try:
            pid = int(self.pid_path.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            return

        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            check=False,
            capture_output=True,
        )
        self.pid_path.unlink(missing_ok=True)

    def _launch_edge(self, headless: bool) -> None:
        self.profile_path.mkdir(parents=True, exist_ok=True)
        arguments = [
            str(self.edge_path),
            "--remote-debugging-address=127.0.0.1",
            f"--remote-debugging-port={self.target_port}",
            "--remote-allow-origins=*",
            f"--user-data-dir={self.profile_path}",
            "--no-first-run",
            "--no-default-browser-check",
        ]
        if headless:
            arguments.append("--headless=new")
        arguments.append("https://x.com/home")

        process = subprocess.Popen(
            arguments,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
        self.pid_path.write_text(str(process.pid), encoding="utf-8")
        self.mode_path.write_text(
            "headless" if headless else "visible",
            encoding="utf-8",
        )

    async def ensure(self, headless: bool | None = None) -> None:
        async with self.lock:
            ready = await self._target_is_ready()
            current_mode = self._current_mode()
            requested_mode = current_mode if headless is None else headless
            requested_mode = requested_mode if requested_mode is not None else False

            if ready and (headless is None or current_mode == requested_mode):
                return
            if ready:
                self._stop_managed_edge()
                await asyncio.sleep(5)

            self._launch_edge(requested_mode)
            deadline = asyncio.get_running_loop().time() + self.startup_timeout
            while asyncio.get_running_loop().time() < deadline:
                if await self._target_is_ready():
                    return
                await asyncio.sleep(1)
            raise RuntimeError("Edge did not expose its CDP endpoint")

    async def open_connection(
        self,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        try:
            return await asyncio.open_connection(self.target_host, self.target_port)
        except OSError:
            await self.ensure()
            return await asyncio.open_connection(self.target_host, self.target_port)


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    finally:
        writer.close()


async def _read_headers(reader: asyncio.StreamReader) -> bytes:
    return await reader.readuntil(b"\r\n\r\n")


def _control_headless(headers: bytes) -> bool | None:
    try:
        request_target = headers.split(b"\r\n", 1)[0].split(b" ", 2)[1].decode()
    except (IndexError, UnicodeDecodeError):
        return None
    parsed = urlsplit(request_target)
    if parsed.path != "/__x_cdp__/ensure":
        return None
    value = parse_qs(parsed.query).get("headless", ["false"])[0].lower()
    return value in {"1", "true", "yes", "on"}


async def _send_control_response(
    writer: asyncio.StreamWriter,
    status: int,
    payload: dict,
) -> None:
    body = json.dumps(payload).encode()
    reason = "OK" if status == 200 else "Service Unavailable"
    writer.write(
        f"HTTP/1.1 {status} {reason}\r\n".encode()
        + b"Content-Type: application/json\r\n"
        + f"Content-Length: {len(body)}\r\n".encode()
        + b"Connection: close\r\n\r\n"
        + body
    )
    await writer.drain()
    writer.close()
    await writer.wait_closed()


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
    edge_controller: EdgeController,
) -> None:
    try:
        request_headers = await _read_headers(client_reader)
        headless = _control_headless(request_headers)
        if headless is not None:
            try:
                await edge_controller.ensure(headless)
                await _send_control_response(
                    client_writer,
                    200,
                    {"ready": True, "headless": headless},
                )
            except (OSError, RuntimeError) as exc:
                await _send_control_response(
                    client_writer,
                    503,
                    {"ready": False, "error": str(exc)},
                )
            return

        target_reader, target_writer = await edge_controller.open_connection()
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
    parser.add_argument("--edge-path")
    parser.add_argument("--profile-path")
    parser.add_argument("--target-startup-timeout", type=int, default=45)
    args = parser.parse_args()
    advertised_port = args.advertised_port or args.listen_port
    project_root = Path(__file__).resolve().parent.parent
    edge_candidates = [
        Path(os.environ.get("ProgramFiles(x86)", ""))
        / "Microsoft/Edge/Application/msedge.exe",
        Path(os.environ.get("ProgramFiles", ""))
        / "Microsoft/Edge/Application/msedge.exe",
    ]
    edge_path = (
        Path(args.edge_path)
        if args.edge_path
        else next((path for path in edge_candidates if path.is_file()), None)
    )
    if edge_path is None or not edge_path.is_file():
        raise FileNotFoundError("Microsoft Edge was not found")
    profile_path = (
        Path(args.profile_path)
        if args.profile_path
        else project_root / "data/x-edge-profile"
    )
    edge_controller = EdgeController(
        args.target_host,
        args.target_port,
        edge_path,
        profile_path,
        args.target_startup_timeout,
    )

    server = await asyncio.start_server(
        lambda reader, writer: _handle_client(
            reader,
            writer,
            args.target_host,
            args.target_port,
            args.advertised_host,
            advertised_port,
            edge_controller,
        ),
        args.listen_host,
        args.listen_port,
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
