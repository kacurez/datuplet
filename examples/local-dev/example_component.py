#!/usr/bin/env python3
"""Example Python component using the Datuplet SDK.

Filters products with price > threshold from config.
"""

import csv
import io
import sys

# Add SDK to path for local testing
sys.path.insert(0, "../../sdk/python")

from client import Client


def main():
    # Connect to gateway
    client = Client()

    # Get config
    cfg = client.config
    print(f"Execution: {cfg.execution_id}")
    print(f"Inputs: {cfg.inputs}")
    print(f"Outputs: {cfg.outputs}")

    # Parse component config
    comp_cfg = client.parse_config()
    print(f"Operations: {comp_cfg.get('operations', [])}")

    # Open reader and writer
    reader = client.open_reader("products")
    writer = client.open_writer("filtered")

    # Process data
    total_rows = 0
    filtered_rows = 0
    header = None

    for chunk in reader:
        # Parse CSV
        csv_reader = csv.reader(io.StringIO(chunk.data.decode("utf-8")))
        rows = list(csv_reader)

        # Get header from first chunk
        if header is None and rows:
            header = rows[0]
            rows = rows[1:]

        # Find price column
        price_idx = header.index("price") if "price" in header else -1

        # Filter rows
        filtered = []
        for row in rows:
            total_rows += 1
            keep = True

            for op in comp_cfg.get("operations", []):
                if op.get("type") == "filter" and op.get("column") == "price" and price_idx >= 0:
                    price = float(row[price_idx])
                    threshold = op.get("value", 0)
                    op_type = op.get("op", ">")

                    if op_type == ">":
                        keep = keep and price > threshold
                    elif op_type == ">=":
                        keep = keep and price >= threshold
                    elif op_type == "<":
                        keep = keep and price < threshold
                    elif op_type == "<=":
                        keep = keep and price <= threshold

            if keep:
                filtered.append(row)
                filtered_rows += 1

        # Write filtered data
        if filtered:
            output = io.StringIO()
            csv_writer = csv.writer(output)

            # Include header on first write
            if total_rows == len(rows):
                csv_writer.writerow(header)

            for row in filtered:
                csv_writer.writerow(row)

            writer.write(output.getvalue().encode("utf-8"), len(filtered))

    reader.close()
    writer.close()

    # Commit
    result = client.commit()

    print(f"\nProcessed {total_rows} rows, filtered to {filtered_rows} rows")
    print(f"Commit success: {result.success}")
    for t in result.tables:
        print(f"  {t.name}: success={t.success}, files={t.files_added}, rows={t.rows_added}")

    client.close()


if __name__ == "__main__":
    main()
