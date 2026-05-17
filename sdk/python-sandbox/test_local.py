import datuplet_sandbox
from datuplet_sandbox.client import resolve_tables

# Step 1: Test resolve API
print("=== Resolving tables ===")
resp = resolve_tables(["raw.products"], gateway_url="http://localhost:30080")
for name, table in resp.tables.items():
    print(f"Table: {name}")
    print(f"  Rows: {table.total_rows}")
    print(f"  Files: {len(table.access.data_files)}")
    for url in table.access.data_files:
        print(f"  URL: {url[:80]}...")

# Step 2: Test DuckDB connection
print("\n=== Connecting via DuckDB ===")
try:
    con = datuplet_sandbox.connect(["raw.products"], gateway_url="http://localhost:30080")
    print(con.sql("SELECT count(*) FROM raw.products").df())
    print(con.sql("SELECT * FROM raw.products LIMIT 5").df())
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
