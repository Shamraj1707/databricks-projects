import os
import sys
from dotenv import load_dotenv, find_dotenv

# Ensure UTF-8 output on Windows consoles
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Load credentials from .env in workspace root
load_dotenv(find_dotenv(usecwd=True))

host = os.getenv("DATABRICKS_HOST", "").strip().rstrip("/")
token = os.getenv("DATABRICKS_TOKEN", "").strip()
warehouse_id = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "").strip()

if not host or not token:
    print("[ERROR] Missing DATABRICKS_HOST or DATABRICKS_TOKEN in .env file!")
    print("Please fill in your credentials in the .env file in your workspace.")
    exit(1)

print(f"Connecting to Databricks workspace: {host} ...")

try:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(host=host, token=token)
    
    # 1. Test Authentication & Current User
    current_user = w.current_user.me()
    print("[SUCCESS] Authentication successful!")
    print(f"   Logged in as: {current_user.user_name} (ID: {current_user.id})")

    # 2. Check Unity Catalog Catalogs
    print("\n[INFO] Checking Unity Catalog Catalogs:")
    try:
        catalogs = list(w.catalogs.list())
        for cat in catalogs[:5]:
            print(f"   - Catalog: {cat.name} ({cat.catalog_type or 'standard'})")
        if len(catalogs) > 5:
            print(f"   ... and {len(catalogs) - 5} more.")
    except Exception as e:
        print(f"   [WARNING] Could not list catalogs: {e}")

    # 3. Check Clusters
    print("\n[INFO] Checking Available Clusters:")
    try:
        clusters = list(w.clusters.list())
        if clusters:
            for cl in clusters[:5]:
                print(f"   - Cluster: {cl.cluster_name} (Status: {cl.state})")
        else:
            print("   - No active or stopped clusters found.")
    except Exception as e:
        print(f"   [WARNING] Could not list clusters: {e}")

    # 4. Check SQL Warehouse (if provided)
    if warehouse_id:
        print(f"\n[INFO] Testing SQL Warehouse ({warehouse_id}):")
        try:
            from databricks import sql
            http_path = warehouse_id if warehouse_id.startswith("/") else f"/sql/1.0/warehouses/{warehouse_id}"
            server_hostname = host.replace("https://", "").replace("http://", "").rstrip("/")
            
            with sql.connect(server_hostname=server_hostname, http_path=http_path, access_token=token) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT current_date(), current_version()")
                    result = cursor.fetchall()
                    print(f"   [SUCCESS] SQL Query output: {result}")
        except Exception as e:
            print(f"   [WARNING] SQL Warehouse test failed: {e}")

    print("\n[DONE] Databricks connection verified successfully!")

except Exception as e:
    print(f"[ERROR] Connection failed: {e}")
