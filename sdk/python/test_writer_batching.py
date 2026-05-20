"""Unit tests for SDK write() byte batching.

Phase 2a of the data-gateway memory/speed work. These tests verify the
batching contract without spinning up a real gateway: we mock the
Writer._write_chunk_immediate seam, then assert on call count and per-call
payload bytes.
"""

import sys
import os
import unittest
from unittest.mock import MagicMock

# Add SDK to path so we can import the Writer directly.
sys.path.insert(0, os.path.dirname(__file__))

from client import Writer, DEFAULT_BATCH_SIZE, WriteResult


def make_writer(batch_size: int = DEFAULT_BATCH_SIZE) -> Writer:
    """Construct a Writer with all transport dependencies mocked out.

    The batching logic lives entirely in write()/write_chunk()/flush()/close()
    above the _write_chunk_immediate seam, so we can substitute that seam
    with a recording MagicMock and observe exactly what would have hit the
    network.
    """
    w = Writer(
        stub=MagicMock(),
        http_session=MagicMock(),
        writer_id="w1",
        http_endpoint="",  # we never reach the actual HTTP/gRPC path
        bucket="raw",
        table="events",
        input_format=0,
        inferred_schema=None,
        batch_size=batch_size,
    )
    # Replace the seam: every successful flush returns a synthetic result.
    w._write_chunk_immediate = MagicMock(  # type: ignore[method-assign]
        return_value=WriteResult(rows_accepted=0, buffer_size=0, inferred_schema=None)
    )
    return w


class WriteBatchingTest(unittest.TestCase):
    """Mirror of sdk/go/writer_batching_test.go — same contract, both languages."""

    def test_small_writes_accumulate(self):
        """Writes below the threshold do not trigger the gateway."""
        w = make_writer(batch_size=1024)
        for _ in range(10):
            w.write(b"x" * 50)  # 500 bytes total, well under 1024
        self.assertEqual(w._write_chunk_immediate.call_count, 0)
        self.assertEqual(len(w._batch_buffer), 500)

    def test_threshold_flush(self):
        """Crossing the threshold flushes exactly the accumulated bytes."""
        w = make_writer(batch_size=100)
        for _ in range(3):  # 50 + 50 + 50 = 150
            w.write(b"x" * 50)
        # First two writes cross the threshold (50+50 = 100 >= 100).
        # Third write leaves 50 bytes pending.
        self.assertEqual(w._write_chunk_immediate.call_count, 1)
        flushed_payload = w._write_chunk_immediate.call_args[0][0]
        self.assertEqual(len(flushed_payload), 100)
        self.assertEqual(len(w._batch_buffer), 50)

    def test_flush_drains_and_is_idempotent(self):
        """Explicit flush() empties the buffer; second flush is a no-op."""
        w = make_writer(batch_size=10 * 1024)
        w.write(b"hello")
        w.flush()
        w.flush()  # idempotent — no extra call
        self.assertEqual(w._write_chunk_immediate.call_count, 1)
        self.assertEqual(w._write_chunk_immediate.call_args[0][0], b"hello")

    def test_close_drains(self):
        """close() must drain pending batched data before CloseWriter."""
        w = make_writer(batch_size=10 * 1024)
        w.write(b"row1\n")
        w.write(b"row2\n")
        # Synthetic CloseWriter response so close() returns cleanly.
        w._stub.CloseWriter.return_value = MagicMock(
            total_rows=2, total_bytes=10, files_written=1
        )
        w.close()
        # One flush call carrying the concatenated bytes.
        self.assertEqual(w._write_chunk_immediate.call_count, 1)
        self.assertEqual(
            w._write_chunk_immediate.call_args[0][0], b"row1\nrow2\n"
        )

    def test_write_chunk_preserves_order(self):
        """write_chunk after pending write() flushes batch first, then explicit chunk."""
        w = make_writer(batch_size=10 * 1024)
        w.write(b"first\n")
        w.write(b"second\n")
        w.write_chunk(b"third-now\n")
        # Two underlying calls: the batch flush, then the explicit chunk.
        self.assertEqual(w._write_chunk_immediate.call_count, 2)
        first_call = w._write_chunk_immediate.call_args_list[0][0][0]
        second_call = w._write_chunk_immediate.call_args_list[1][0][0]
        self.assertEqual(first_call, b"first\nsecond\n")
        self.assertEqual(second_call, b"third-now\n")

    def test_disabled_by_negative_batch_size(self):
        """batch_size < 0 disables batching: every write is one chunk."""
        w = make_writer(batch_size=-1)
        for _ in range(5):
            w.write(b"row\n")
        self.assertEqual(w._write_chunk_immediate.call_count, 5)

    def test_batch_size_zero_uses_default(self):
        """batch_size=0 is shorthand for DEFAULT_BATCH_SIZE (1 MiB)."""
        w = make_writer(batch_size=0)
        self.assertEqual(w._batch_threshold, DEFAULT_BATCH_SIZE)
        # 100 small writes well under 1 MiB → no flush yet.
        for _ in range(100):
            w.write(b"x" * 100)
        self.assertEqual(w._write_chunk_immediate.call_count, 0)

    def test_roundtrip_reduction_headline(self):
        """At 1 MiB threshold, 10K row-at-a-time writes should compress
        to ~1 underlying call (well under the no-batch baseline of 10K)."""
        w = make_writer(batch_size=1024 * 1024)
        N = 10_000
        row = (
            b'{"id":1,"v":"padding-padding-padding-padding-padding-padding"}\n'
        )
        for _ in range(N):
            w.write(row)
        w.flush()
        self.assertLess(
            w._write_chunk_immediate.call_count,
            N // 100,
            "expected at least 100x fewer underlying calls than write() calls",
        )
        total_bytes = sum(
            len(call.args[0]) for call in w._write_chunk_immediate.call_args_list
        )
        self.assertEqual(total_bytes, N * len(row))


if __name__ == "__main__":
    unittest.main()
