import os
import sys
from dotenv import load_dotenv, find_dotenv

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv(find_dotenv(usecwd=True))
host = os.getenv("DATABRICKS_HOST", "").strip().rstrip("/")
token = os.getenv("DATABRICKS_TOKEN", "").strip()

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(host=host, token=token)
    
    # Try listing schemas in samples
    print("[INFO] Attempting to inspect catalog 'samples'...")
    try:
        schemas = list(w.schemas.list(catalog_name="samples"))
        print(f"[SUCCESS] Found {len(schemas)} schemas in 'samples':")
        for s in schemas:
            print(f"  - {s.name}")
    except Exception as e:
        print(f"[ERROR listing schemas in 'samples']: {e}")

    # Try listing tables in samples.wanderbricks
    print("\n[INFO] Attempting to inspect tables in 'samples.wanderbricks'...")
    try:
        tables = list(w.tables.list(catalog_name="samples", schema_name="wanderbricks"))
        print(f"[SUCCESS] Found {len(tables)} tables in 'samples.wanderbricks':")
        for t in tables:
            print(f"\n================ TABLE: {t.name} ================")
            print(f"Type: {t.table_type}, Comment: {t.comment}")
            if t.columns:
                print("Columns:")
                for col in t.columns:
                    print(f"  - {col.name} ({col.type_name}): {col.comment or ''}")
    except Exception as e:
        print(f"[ERROR listing tables in 'samples.wanderbricks']: {e}")

except Exception as e:
    print(f"[ERROR]: {e}")
