#!/usr/bin/env python3
"""Integration tests for Python SDK with Data Gateway v2."""

import os
import sys
import time
import unittest
from contextlib import contextmanager

# Add SDK to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../sdk/python"))

from client import Client
from gateway.v2 import gateway_pb2 as pb


class TestPythonSDKV2(unittest.TestCase):
    """Test Python SDK integration with Gateway v2."""

    def setUp(self):
        """Set up test fixtures."""
        if not os.getenv("INTEGRATION_TEST"):
            self.skipTest("Skipping integration test. Set INTEGRATION_TEST=1 to run.")

        # Set gateway address
        os.environ["DATUPLET_GATEWAY_ADDR"] = "localhost:50051"

    def test_csv_write(self):
        """Test basic CSV write operation."""
        client = Client()

        # Open writer for CSV format (uses defaultBucket from config)
        writer = client.open_writer("test_output", input_format=pb.DataFormat.FORMAT_CSV)

        # Write CSV data
        csv_data = b"id,name,price\n1,Widget,9.99\n2,Gadget,19.99\n"
        result = writer.write_chunk(csv_data)
        self.assertEqual(result.rows_accepted, 2)

        # Close writer
        close_result = writer.close()
        self.assertEqual(close_result.total_rows, 2)

        # Commit
        commit_result = client.commit()
        self.assertTrue(commit_result.success)

        client.close()

    def test_json_write_explicit_bucket(self):
        """Test JSON write with explicit bucket and format conversion."""
        client = Client()

        # Open writer with explicit bucket and table
        writer = client.open_writer(
            "test_json",
            bucket="raw",
            input_format=pb.DataFormat.FORMAT_JSON,
        )

        # Write JSON array
        json_data = b'[{"id":1,"name":"Widget","price":9.99},{"id":2,"name":"Gadget","price":19.99}]'
        result = writer.write_chunk(json_data)
        self.assertEqual(result.rows_accepted, 2)

        # Verify bucket and table are set
        self.assertIsNotNone(writer.bucket)
        self.assertEqual(writer.table, "test_json")

        # Close writer
        close_result = writer.close()
        self.assertEqual(close_result.total_rows, 2)

        # Commit
        commit_result = client.commit()
        self.assertTrue(commit_result.success)

        client.close()

    def test_config_access(self):
        """Test accessing gateway configuration."""
        client = Client()

        config = client.config
        self.assertIsNotNone(config)
        self.assertIsNotNone(config.execution_id)
        # Check bucket-based config fields
        self.assertIsInstance(config.input_buckets, list)
        self.assertIsInstance(config.output_buckets, list)
        self.assertIsInstance(config.input_tables, list)

        client.close()

    def test_logging(self):
        """Test logging functionality."""
        client = Client()

        # Log messages should not raise errors
        client.log("INFO", "Test info message")
        client.log("WARN", "Test warning message")

        client.close()


if __name__ == "__main__":
    unittest.main()
