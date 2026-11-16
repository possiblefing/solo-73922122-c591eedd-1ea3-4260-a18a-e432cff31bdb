"""Download responses that are safe against concurrent reclamation.

The failure this module fixes
-----------------------------
Starlette's :class:`~starlette.responses.FileResponse` sends the ``200``
status line and headers *before* it opens the file.  A batch can be retired
and its original garbage-collected in that window: the client sees ``200``
with a ``content-length``, then the server's ``open()`` raises
``FileNotFoundError`` and the client receives an empty (truncated) ZIP.

The fix
-------
The endpoint opens the archive with :func:`os.open` *before* constructing
this response, so the original is pinned by a file descriptor before any
byte of the status line is committed.  Once the response starts, garbage
collection may rename the file into ``quarantine/`` or unlink it (on the
same instance or a sibling sharing the volume); POSIX keeps the inode
readable through the already-open descriptor, so the declared
``Content-Length``/``Range`` is always served in full.

The response also watches for ``http.disconnect``.  A client that goes
away stops the transfer, the descriptor is closed, and an original that
was reclaimed meanwhile is released rather than held for the lifetime of
a dead connection.

Range handling (single, suffix, multiple, ``If-Range``, ``416``,
``400``) preserves the previous ``FileResponse`` semantics byte for byte;
the parsers are local so this module does not depend on Starlette
private helpers.
"""
from __future__ import annotations

import hashlib
import os
import re
from email.utils import formatdate
from secrets import token_hex
from urllib.parse import quote

import anyio
from starlette.datastructures import Headers
from starlette.responses import PlainTextResponse, Response
from starlette.types import Receive, Scope, Send

# Same acceptance grammar as Starlette's FileResponse (``bytes=`` unit over
# comma separated ``start-end`` / ``-suffix`` specs).
_RANGE_PATTERN = re.compile(r"(\d*)-(\d*)")


#: Seam for tests that need to park a transfer deterministically.  Kept as
#: a module global (not patched onto ``os``) so it is trivial to override.
_pread = os.pread


class _MalformedRangeHeader(Exception):
    def __init__(self, content: str = "Malformed range header.") -> None:
        self.content = content


class _RangeNotSatisfiable(Exception):
    def __init__(self, max_size: int) -> None:
        self.max_size = max_size


def _parse_range_header(http_range: str, file_size: int) -> list[tuple[int, int]]:
    """Parse a ``Range`` header exactly like Starlette's FileResponse.

    Returns half-open ``(start, end)`` pairs.  Raises
    :class:`_MalformedRangeHeader` (-> 400) and
    :class:`_RangeNotSatisfiable` (-> 416) as the previous implementation
    did.
    """
    try:
        units, range_ = http_range.split("=", 1)
    except ValueError:
        raise _MalformedRangeHeader() from None

    if units.strip().lower() != "bytes":
        raise _MalformedRangeHeader("Only support bytes range")

    ranges = [
        (
            int(_[0]) if _[0] else file_size - int(_[1]),
            int(_[1]) + 1
            if _[0] and _[1] and int(_[1]) < file_size
            else file_size,
        )
        for _ in _RANGE_PATTERN.findall(range_)
        if _ != ("", "")
    ]

    if len(ranges) == 0:
        raise _MalformedRangeHeader("Range header: range must be requested")

    if any(not (0 <= start < file_size) for start, _ in ranges):
        raise _RangeNotSatisfiable(file_size)

    if any(start > end for start, end in ranges):
        raise _MalformedRangeHeader("Range header: start must be less than end")

    if len(ranges) == 1:
        return ranges

    # Merge overlapping/adjacent ranges (also Starlette's behavior).
    result: list[tuple[int, int]] = []
    for start, end in ranges:
        for position in range(len(result)):
            p_start, p_end = result[position]
            if start > p_end:
                continue
            if end < p_start:  # pragma: no cover - unreachable, kept for parity
                result.insert(position, (start, end))
                break
            result[position] = (min(start, p_start), max(end, p_end))
            break
        else:
            result.append((start, end))
    return result


def _etag(stat_result: os.stat_result) -> str:
    """Same weak-ish ETag scheme Starlette's FileResponse uses."""
    etag_base = str(stat_result.st_mtime) + "-" + str(stat_result.st_size)
    digest = hashlib.md5(
        etag_base.encode(), usedforsecurity=False
    ).hexdigest()
    return f'"{digest}"'


def _content_disposition(filename: str) -> str:
    quoted = quote(filename)
    if quoted != filename:
        return f"attachment; filename*=utf-8''{quoted}"
    return f'attachment; filename="{filename}"'


class ArchiveDownloadResponse(Response):
    """Stream an already-open archive fd with Range support.

    Parameters
    ----------
    fd:
        Open read-only descriptor for the original bytes.  Ownership
        transfers to the response, which always closes it (on completion,
        on client disconnect, or on error).
    stat_result:
        ``os.fstat(fd)`` taken by the caller.  Used for ``Content-Length``,
        validators and range math so headers describe the exact inode that
        will be streamed.
    filename:
        Suggested download name for ``Content-Disposition``.
    """

    chunk_size = 64 * 1024
    media_type = "application/zip"

    def __init__(
        self,
        fd: int,
        stat_result: os.stat_result,
        filename: str,
    ) -> None:
        self._fd = fd
        self._fd_closed = False
        self.stat_result = stat_result
        self._file_size = stat_result.st_size
        self.status_code = 200
        self.background = None
        headers = {
            "content-disposition": _content_disposition(filename),
            "accept-ranges": "bytes",
            "content-length": str(stat_result.st_size),
            "last-modified": formatdate(stat_result.st_mtime, usegmt=True),
            "etag": _etag(stat_result),
        }
        super().__init__(headers=headers, media_type=self.media_type)

    # -- lifecycle --------------------------------------------------------- #

    @property
    def closed(self) -> bool:
        return self._fd_closed

    def close(self) -> None:
        """Release the pinned original (idempotent)."""
        if self._fd_closed:
            return
        self._fd_closed = True
        try:
            os.close(self._fd)
        except OSError:  # pragma: no cover - defensive double-close guard
            pass

    def __del__(self) -> None:  # pragma: no cover - safety net
        try:
            self.close()
        except Exception:
            pass

    # -- ASGI -------------------------------------------------------------- #

    async def __call__(
        self, scope: Scope, receive: Receive, send: Send
    ) -> None:
        try:
            async with anyio.create_task_group() as task_group:
                # A disconnect cancels the streaming task; the ``finally``
                # then closes the descriptor and, with it, any inode that
                # GC already unlinked from the directory.
                task_group.start_soon(
                    self._watch_disconnect, receive, task_group.cancel_scope
                )
                try:
                    await self._respond(scope, receive, send)
                finally:
                    task_group.cancel_scope.cancel()
        finally:
            self.close()

    @staticmethod
    async def _watch_disconnect(
        receive: Receive, cancel_scope: anyio.CancelScope
    ) -> None:
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                cancel_scope.cancel()
                return

    async def _respond(
        self, scope: Scope, receive: Receive, send: Send
    ) -> None:
        send_header_only = scope["method"].upper() == "HEAD"
        request_headers = Headers(scope=scope)
        http_range = request_headers.get("range")
        http_if_range = request_headers.get("if-range")

        etag = self.headers.get("etag")
        last_modified = self.headers.get("last-modified")
        if http_range is None or (
            http_if_range is not None
            and http_if_range not in (etag, last_modified)
        ):
            await self._send_headers(send, 200)
            if send_header_only:
                await self._send_body(send, b"", final=True)
                return
            await self._stream(send, 0, self._file_size)
            await self._send_body(send, b"", final=True)
            return

        try:
            ranges = _parse_range_header(http_range, self._file_size)
        except _MalformedRangeHeader as exc:
            await PlainTextResponse(exc.content, status_code=400)(
                scope, receive, send
            )
            return
        except _RangeNotSatisfiable as exc:
            await PlainTextResponse(
                status_code=416,
                headers={"Content-Range": f"*/{exc.max_size}"},
            )(scope, receive, send)
            return

        if len(ranges) == 1:
            start, end = ranges[0]
            self.headers["content-range"] = (
                f"bytes {start}-{end - 1}/{self._file_size}"
            )
            self.headers["content-length"] = str(end - start)
            await self._send_headers(send, 206)
            if send_header_only:
                await self._send_body(send, b"", final=True)
                return
            await self._stream(send, start, end)
            await self._send_body(send, b"", final=True)
            return

        # Multiple ranges: same wire format as Starlette's FileResponse
        # (multipart/byteranges with per-part headers), but the declared
        # Content-Length is summed from the exact bytes we emit.  Reusing
        # Starlette's helper would inherit its one-byte-short accounting,
        # which would break "body matches the advertised length".
        boundary = token_hex(13)
        part_header = self._multipart_part_header
        content_length = (
            sum(
                len(part_header(boundary, start, end, self._file_size))
                + (end - start)
                + 1  # trailing "\n" after each part's data
                for start, end in ranges
            )
            + len(self._multipart_closing(boundary))
        )
        self.headers["content-range"] = (
            f"multipart/byteranges; boundary={boundary}"
        )
        self.headers["content-length"] = str(content_length)
        await self._send_headers(send, 206)
        if send_header_only:
            await self._send_body(send, b"", final=True)
            return
        for start, end in ranges:
            await self._send_body(
                send,
                part_header(boundary, start, end, self._file_size),
                final=False,
            )
            await self._stream(send, start, end)
            await self._send_body(send, b"\n", final=False)
        await self._send_body(
            send, self._multipart_closing(boundary), final=True
        )

    @staticmethod
    def _multipart_part_header(
        boundary: str, start: int, end: int, file_size: int
    ) -> bytes:
        return (
            f"--{boundary}\n"
            f"Content-Type: application/zip\n"
            f"Content-Range: bytes {start}-{end - 1}/{file_size}\n"
            "\n"
        ).encode("latin-1")

    @staticmethod
    def _multipart_closing(boundary: str) -> bytes:
        return f"\n--{boundary}--\n".encode("latin-1")

    # -- byte plumbing ----------------------------------------------------- #

    async def _send_headers(self, send: Send, status: int) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": self.raw_headers,
            }
        )

    async def _send_body(self, send: Send, body: bytes, final: bool) -> None:
        await send(
            {
                "type": "http.response.body",
                "body": body,
                "more_body": not final,
            }
        )

    async def _stream(self, send: Send, start: int, end: int) -> None:
        """Send exactly ``[start, end)`` bytes read from the pinned fd."""
        offset = start
        while offset < end:
            chunk = await anyio.to_thread.run_sync(
                _pread, self._fd, min(self.chunk_size, end - offset), offset
            )
            if not chunk:
                # The blob is content-addressed and immutable while its row
                # is live; an early EOF means external truncation.  Let the
                # ASGI server surface the short body instead of lying.
                return
            offset += len(chunk)
            await send(
                {
                    "type": "http.response.body",
                    "body": chunk,
                    "more_body": True,
                }
            )
