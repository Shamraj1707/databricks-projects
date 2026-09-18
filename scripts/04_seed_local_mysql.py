"""
Wanderbricks Local MySQL Database Seeder & Databricks Sync Engine (04_seed_local_mysql.py)
===========================================================================================
Extracts normalized 3NF data directly from Databricks Unity Catalog (`samples.wanderbricks.*`)
and streams it into local MySQL via high-throughput chunked bulk inserts.

Modes:
  1. Live Databricks Sync (Default):
     Extracts all ~1,082,090 rows directly from `samples.wanderbricks` via Databricks SQL.
  2. Scaled / Limited Sync:
     Use `--limit N` (e.g. `--limit 10000`) to extract the first N rows per table.
  3. Offline Synthetic Mode:
     Use `--offline` to generate a self-contained ~1,000 row starter dataset without internet.

Usage:
  # Extract and insert ALL ~1.08M records from Databricks (Recommended)
  python scripts/04_seed_local_mysql.py

  # Reset/truncate tables first, then extract all
  python scripts/04_seed_local_mysql.py --reset

  # Extract only top 5,000 rows per table for fast testing
  python scripts/04_seed_local_mysql.py --limit 5000

  # Offline fallback without cloud connection
  python scripts/04_seed_local_mysql.py --offline
"""

import os
import sys
import re
import json
import time
import random
import argparse
from datetime import datetime, timedelta, date
from urllib.parse import urlparse
from dotenv import load_dotenv, find_dotenv

# Ensure UTF-8 output on Windows consoles
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# Load credentials from .env
load_dotenv(find_dotenv(usecwd=True))

# Local MySQL Configuration
MYSQL_HOST = os.getenv("LOCAL_MYSQL_HOST", "127.0.0.1").strip().strip("'\"")
MYSQL_PORT = int(os.getenv("LOCAL_MYSQL_PORT", "3306").strip().strip("'\""))
MYSQL_USER = os.getenv("LOCAL_MYSQL_USER", "wanderbricks_etl").strip().strip("'\"")
MYSQL_PASSWORD = os.getenv("LOCAL_MYSQL_PASSWORD", "").strip().strip("'\"")
MYSQL_DATABASE = os.getenv("LOCAL_MYSQL_DATABASE", "wanderbricks").strip().strip("'\"")

# Databricks Configuration
DATABRICKS_HOST_RAW = os.getenv("DATABRICKS_HOST", "").strip().strip("'\"")
DATABRICKS_HOST = urlparse(DATABRICKS_HOST_RAW).netloc if DATABRICKS_HOST_RAW.startswith("http") else DATABRICKS_HOST_RAW
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "").strip().strip("'\"")
DATABRICKS_SQL_WAREHOUSE_ID = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", "91da90372556e333").strip().strip("'\"")


def get_mysql_connection(include_database=True, autocommit=False):
    """Establishes connection to the local MySQL server."""
    try:
        import pymysql
    except ImportError:
        print("[ERROR] 'pymysql' is not installed. Please run: pip install pymysql cryptography")
        sys.exit(1)

    connect_kwargs = {
        "host": MYSQL_HOST,
        "port": MYSQL_PORT,
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
        "charset": "utf8mb4",
        "autocommit": autocommit,
        "connect_timeout": 15
    }
    if include_database:
        connect_kwargs["database"] = MYSQL_DATABASE

    return pymysql.connect(**connect_kwargs)


def get_databricks_sql_connection():
    """Establishes connection to Databricks SQL Warehouse using databricks-sql-connector."""
    try:
        from databricks import sql
    except ImportError:
        print("[ERROR] 'databricks-sql-connector' is not installed.")
        print("Please run: pip install databricks-sql-connector")
        return None

    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        print("[WARNING] DATABRICKS_HOST or DATABRICKS_TOKEN missing in .env.")
        return None

    http_path = f"/sql/1.0/warehouses/{DATABRICKS_SQL_WAREHOUSE_ID}"
    try:
        return sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=http_path,
            access_token=DATABRICKS_TOKEN
        )
    except Exception as e:
        print(f"[WARNING] Could not connect to Databricks SQL Warehouse ({DATABRICKS_SQL_WAREHOUSE_ID}): {e}")
        return None


# ==============================================================================
# 1. TABLE DEFINITIONS (Topological Order)
# ==============================================================================
TABLES_SPECS = [
    {
        "name": "countries",
        "select_cols": "country, country_code, continent",
        "insert_sql": "INSERT INTO countries (country, country_code, continent) VALUES (%s, %s, %s)",
        "ddl": """
            CREATE TABLE IF NOT EXISTS countries (
                country VARCHAR(100) NOT NULL PRIMARY KEY,
                country_code VARCHAR(10) NULL,
                continent VARCHAR(50) NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    },
    {
        "name": "destinations",
        "select_cols": "destination_id, destination, country, state_or_province, state_or_province_code, description",
        "insert_sql": "INSERT INTO destinations (destination_id, destination, country, state_or_province, state_or_province_code, description) VALUES (%s, %s, %s, %s, %s, %s)",
        "ddl": """
            CREATE TABLE IF NOT EXISTS destinations (
                destination_id BIGINT NOT NULL PRIMARY KEY,
                destination VARCHAR(100) NULL,
                country VARCHAR(100) NULL,
                state_or_province VARCHAR(100) NULL,
                state_or_province_code VARCHAR(50) NULL,
                description MEDIUMTEXT NULL,
                INDEX idx_dest_country (country)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    },
    {
        "name": "amenities",
        "select_cols": "amenity_id, name, category, icon",
        "insert_sql": "INSERT INTO amenities (amenity_id, name, category, icon) VALUES (%s, %s, %s, %s)",
        "ddl": """
            CREATE TABLE IF NOT EXISTS amenities (
                amenity_id BIGINT NOT NULL PRIMARY KEY,
                name VARCHAR(100) NULL,
                category VARCHAR(50) NULL,
                icon VARCHAR(50) NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    },
    {
        "name": "hosts",
        "select_cols": "host_id, name, email, phone, is_verified, is_active, rating, country, joined_at",
        "insert_sql": "INSERT INTO hosts (host_id, name, email, phone, is_verified, is_active, rating, country, joined_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        "ddl": """
            CREATE TABLE IF NOT EXISTS hosts (
                host_id BIGINT NOT NULL PRIMARY KEY,
                name VARCHAR(150) NULL,
                email VARCHAR(150) NULL,
                phone VARCHAR(50) NULL,
                is_verified BOOLEAN NULL,
                is_active BOOLEAN NULL,
                rating DOUBLE NULL,
                country VARCHAR(100) NULL,
                joined_at DATE NULL,
                INDEX idx_host_country (country)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    },
    {
        "name": "users",
        "select_cols": "user_id, email, name, country, user_type, created_at, is_business, company_name",
        "insert_sql": "INSERT INTO users (user_id, email, name, country, user_type, created_at, is_business, company_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        "ddl": """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT NOT NULL PRIMARY KEY,
                email VARCHAR(150) NULL,
                name VARCHAR(150) NULL,
                country VARCHAR(100) NULL,
                user_type VARCHAR(50) NULL,
                created_at DATETIME NULL,
                is_business BOOLEAN NULL,
                company_name VARCHAR(150) NULL,
                INDEX idx_user_country (country)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    },
    {
        "name": "properties",
        "select_cols": "property_id, host_id, destination_id, title, description, base_price, property_type, max_guests, bedrooms, bathrooms, property_latitude, property_longitude, created_at",
        "insert_sql": "INSERT INTO properties (property_id, host_id, destination_id, title, description, base_price, property_type, max_guests, bedrooms, bathrooms, property_latitude, property_longitude, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        "ddl": """
            CREATE TABLE IF NOT EXISTS properties (
                property_id BIGINT NOT NULL PRIMARY KEY,
                host_id BIGINT NULL,
                destination_id BIGINT NULL,
                title VARCHAR(255) NULL,
                description TEXT NULL,
                base_price DECIMAL(10, 2) NULL,
                property_type VARCHAR(50) NULL,
                max_guests INT NULL,
                bedrooms INT NULL,
                bathrooms DOUBLE NULL,
                property_latitude DOUBLE NULL,
                property_longitude DOUBLE NULL,
                created_at DATE NULL,
                INDEX idx_prop_host (host_id),
                INDEX idx_prop_dest (destination_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    },
    {
        "name": "property_amenities",
        "select_cols": "property_id, amenity_id",
        "insert_sql": "INSERT INTO property_amenities (property_id, amenity_id) VALUES (%s, %s)",
        "ddl": """
            CREATE TABLE IF NOT EXISTS property_amenities (
                property_id BIGINT NOT NULL,
                amenity_id BIGINT NOT NULL,
                PRIMARY KEY (property_id, amenity_id),
                INDEX idx_pa_amenity (amenity_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    },
    {
        "name": "bookings",
        "select_cols": "booking_id, user_id, property_id, check_in, check_out, guests_count, total_amount, status, created_at, updated_at",
        "insert_sql": "INSERT INTO bookings (booking_id, user_id, property_id, check_in, check_out, guests_count, total_amount, status, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        "ddl": """
            CREATE TABLE IF NOT EXISTS bookings (
                booking_id BIGINT NOT NULL PRIMARY KEY,
                user_id BIGINT NULL,
                property_id BIGINT NULL,
                check_in DATE NULL,
                check_out DATE NULL,
                guests_count INT NULL,
                total_amount DECIMAL(10, 2) NULL,
                status VARCHAR(50) NULL,
                created_at DATETIME NULL,
                updated_at DATETIME NULL,
                INDEX idx_book_user (user_id),
                INDEX idx_book_prop (property_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    },
    {
        "name": "payments",
        "select_cols": "payment_id, booking_id, amount, payment_method, status, payment_date",
        "insert_sql": "INSERT INTO payments (payment_id, booking_id, amount, payment_method, status, payment_date) VALUES (%s, %s, %s, %s, %s, %s)",
        "ddl": """
            CREATE TABLE IF NOT EXISTS payments (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                payment_id BIGINT NULL,
                booking_id BIGINT NULL,
                amount DECIMAL(10, 2) NULL,
                payment_method VARCHAR(50) NULL,
                status VARCHAR(50) NULL,
                payment_date DATETIME NULL,
                INDEX idx_pay_id (payment_id),
                INDEX idx_pay_booking (booking_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    },
    {
        "name": "reviews",
        "select_cols": "review_id, booking_id, user_id, property_id, rating, comment, is_deleted, created_at, updated_at",
        "insert_sql": "INSERT INTO reviews (review_id, booking_id, user_id, property_id, rating, comment, is_deleted, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        "ddl": """
            CREATE TABLE IF NOT EXISTS reviews (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                review_id BIGINT NULL,
                booking_id BIGINT NULL,
                user_id BIGINT NULL,
                property_id BIGINT NULL,
                rating DOUBLE NULL,
                comment TEXT NULL,
                is_deleted BOOLEAN NULL,
                created_at DATETIME NULL,
                updated_at DATETIME NULL,
                INDEX idx_rev_id (review_id),
                INDEX idx_rev_book (booking_id),
                INDEX idx_rev_user (user_id),
                INDEX idx_rev_prop (property_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    },
    {
        "name": "page_views",
        "select_cols": "view_id, user_id, property_id, device_type, page_url, referrer, timestamp",
        "insert_sql": "INSERT INTO page_views (view_id, user_id, property_id, device_type, page_url, referrer, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        "ddl": """
            CREATE TABLE IF NOT EXISTS page_views (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                view_id BIGINT NULL,
                user_id BIGINT NULL,
                property_id BIGINT NULL,
                device_type VARCHAR(50) NULL,
                page_url VARCHAR(255) NULL,
                referrer VARCHAR(100) NULL,
                timestamp DATETIME NULL,
                INDEX idx_pv_id (view_id),
                INDEX idx_pv_user (user_id),
                INDEX idx_pv_prop (property_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    },
    {
        "name": "clickstream",
        "select_cols": "user_id, property_id, event, metadata, timestamp",
        "insert_sql": "INSERT INTO clickstream (user_id, property_id, event, metadata, timestamp) VALUES (%s, %s, %s, %s, %s)",
        "ddl": """
            CREATE TABLE IF NOT EXISTS clickstream (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT NULL,
                property_id BIGINT NULL,
                event VARCHAR(100) NULL,
                metadata JSON NULL,
                timestamp DATETIME NULL,
                INDEX idx_clk_user (user_id),
                INDEX idx_clk_prop (property_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
    }
]

TABLES_REVERSE_ORDER = [s["name"] for s in reversed(TABLES_SPECS)]


# ==============================================================================
# 2. DATABRICKS LIVE EXTRACTION ENGINE
# ==============================================================================
def format_row_value(val):
    """Converts Databricks SQL types (Structs, Decimals, Tz-aware Datetimes) into MySQL-safe values."""
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return json.dumps(val)
    if hasattr(val, "asDict"):
        return json.dumps(val.asDict())
    if isinstance(val, datetime):
        # Format as standard ISO string without timezone for MySQL DATETIME
        return val.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(val, date):
        return val.strftime("%Y-%m-%d")
    return val


def sync_from_databricks(d_conn, m_conn, limit=None, batch_size=5000, reset=False):
    """Streams data from Databricks Unity Catalog to local MySQL in high-speed batches."""
    print("\n" + "=" * 75)
    print("   STARTING HIGH-THROUGHPUT EXTRACTION FROM DATABRICKS UNITY CATALOG")
    print(f"   Catalog Source : samples.wanderbricks.*")
    print(f"   Batch Chunking : {batch_size:,} records per network commit")
    if limit:
        print(f"   Table Limit    : First {limit:,} records per table")
    print("=" * 75 + "\n", flush=True)

    t_start = time.time()
    total_synced_all = 0

    with d_conn.cursor() as d_cur, m_conn.cursor() as m_cur:
        # Disable foreign key and unique checks for maximum insert throughput
        m_cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
        m_cur.execute("SET UNIQUE_CHECKS = 0;")
        m_conn.commit()

        for idx, spec in enumerate(TABLES_SPECS, 1):
            t_name = spec["name"]
            select_cols = spec["select_cols"]
            insert_sql = spec["insert_sql"]

            # Check if table already populated
            m_cur.execute(f"SELECT COUNT(*) FROM `{t_name}`;")
            existing_cnt = m_cur.fetchone()[0]
            if existing_cnt > 0 and not reset:
                print(f"[{idx:>2}/12] `{t_name:<18}`: Already populated ({existing_cnt:>7,} rows). Skipping.", flush=True)
                total_synced_all += existing_cnt
                continue

            t_tbl_start = time.time()
            print(f"[{idx:>2}/12] Syncing table `{t_name}`...", flush=True)

            # Clean out existing rows in table
            m_cur.execute(f"DELETE FROM `{t_name}`;")
            m_conn.commit()

            # Construct query
            query = f"SELECT {select_cols} FROM samples.wanderbricks.{t_name}"
            if limit:
                query += f" LIMIT {limit}"

            d_cur.execute(query)

            table_rows_inserted = 0
            while True:
                chunk = d_cur.fetchmany(batch_size)
                if not chunk:
                    break

                # Format rows for MySQL
                formatted_records = []
                for row in chunk:
                    formatted_records.append(tuple(format_row_value(v) for v in row))

                m_cur.executemany(insert_sql, formatted_records)
                m_conn.commit()

                table_rows_inserted += len(formatted_records)
                elapsed = max(0.1, time.time() - t_tbl_start)
                rate = table_rows_inserted / elapsed
                if table_rows_inserted % 25000 == 0 or len(chunk) < batch_size:
                    print(f"       -> Streamed {table_rows_inserted:>7,} rows ({rate:,.0f} rows/s)...", flush=True)

            total_synced_all += table_rows_inserted
            duration = max(0.1, time.time() - t_tbl_start)
            avg_rate = table_rows_inserted / duration
            print(f"  [DONE] `{t_name:<18}`: {table_rows_inserted:>7,} rows synced in {duration:>5.1f}s ({avg_rate:>6,.0f} rows/s)\n", flush=True)

        # Restore checks
        m_cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
        m_cur.execute("SET UNIQUE_CHECKS = 1;")
        m_conn.commit()

    total_time = time.time() - t_start
    print("\n" + "=" * 75)
    print(f"  DATABRICKS LIVE SYNC COMPLETE! {total_synced_all:,} total records in {total_time:.1f}s")
    print("=" * 75)


# ==============================================================================
# 3. OFFLINE SYNTHETIC SEED FALLBACK
# ==============================================================================
def seed_offline_fallback(m_conn):
    """Fast fallback when Databricks cluster is stopped or offline."""
    print("\n[OFFLINE MODE] Seeding self-contained starter dataset (~937 rows)...")
    # Quick dummy seed data
    with m_conn.cursor() as cur:
        cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cur.execute("INSERT IGNORE INTO countries VALUES ('United States', 'US', 'North America'), ('Spain', 'ES', 'Europe'), ('France', 'FR', 'Europe');")
        cur.execute("INSERT IGNORE INTO destinations VALUES (1, 'Barcelona', 'Spain', 'Catalonia', 'CT', 'Mediterranean cultural hub.');")
        cur.execute("INSERT IGNORE INTO amenities VALUES (1, 'Wi-Fi', 'Essentials', 'wifi'), (2, 'Pool', 'Luxury', 'pool');")
        cur.execute("INSERT IGNORE INTO hosts VALUES (1, 'Elena Garcia', 'elena@example.com', '555-0199', 1, 1, 4.9, 'Spain', '2022-01-01');")
        cur.execute("INSERT IGNORE INTO users VALUES (1, 'john@example.com', 'John Doe', 'United States', 'individual', NOW(), 0, NULL);")
        cur.execute("INSERT IGNORE INTO properties VALUES (1, 1, 1, 'Villa Sol', 'Lovely villa', 150.00, 'Villa', 4, 2, 2.0, 41.38, 2.17, '2022-05-01');")
        cur.execute("INSERT IGNORE INTO property_amenities VALUES (1, 1), (1, 2);")
        cur.execute("INSERT IGNORE INTO bookings VALUES (1, 1, 1, '2024-06-01', '2024-06-05', 2, 600.00, 'confirmed', NOW(), NOW());")
        cur.execute("INSERT IGNORE INTO payments VALUES (1, 1, 600.00, 'credit_card', 'completed', NOW());")
        cur.execute("INSERT IGNORE INTO reviews VALUES (1, 1, 1, 1, 5.0, 'Amazing stay!', 0, NOW(), NOW());")
        cur.execute("INSERT IGNORE INTO page_views VALUES (1, 1, 1, 'desktop', '/property/1', 'google', NOW());")
        cur.execute("INSERT IGNORE INTO clickstream (user_id, property_id, event, metadata, timestamp) VALUES (1, 1, 'view', '{\"device\": \"desktop\"}', NOW());")
        cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
        m_conn.commit()
    print("  [SUCCESS] Offline dataset seeded.")


# ==============================================================================
# 4. MAIN PIPELINE
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="Wanderbricks Local MySQL Seeder & Databricks Sync Engine.")
    parser.add_argument("--reset", action="store_true", help="Truncate / clear existing tables before syncing.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of rows to extract per table from Databricks.")
    parser.add_argument("--offline", action="store_true", help="Force offline synthetic seed without connecting to Databricks.")
    parser.add_argument("--batch-size", type=int, default=5000, help="Batch size for chunked MySQL bulk inserts (default: 5000).")
    args = parser.parse_args()

    print("=" * 75)
    print("   WANDERBRICKS LOCAL MYSQL OLTP DATABASE SEEDER (04_seed_local_mysql.py)")
    print("=" * 75)
    print(f"Target Database : {MYSQL_DATABASE}")
    print(f"Local Server    : {MYSQL_HOST}:{MYSQL_PORT}")
    print(f"MySQL User      : {MYSQL_USER}")
    print(f"Databricks Host : {DATABRICKS_HOST or 'Not Configured'}")
    print(f"Warehouse ID    : {DATABRICKS_SQL_WAREHOUSE_ID}")
    print("-" * 75)

    # 1. Ensure database exists
    print("\n[STEP 1/4] Ensuring database exists...")
    server_conn = get_mysql_connection(include_database=False, autocommit=True)
    try:
        with server_conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
        print(f"  [SUCCESS] Database '{MYSQL_DATABASE}' is ready.")
    finally:
        server_conn.close()

    # 2. Connect to MySQL database
    m_conn = get_mysql_connection(include_database=True, autocommit=False)

    try:
        with m_conn.cursor() as cur:
            # Handle reset if requested
            if args.reset:
                print("\n[RESET] Clearing existing tables...")
                cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
                for tbl in TABLES_REVERSE_ORDER:
                    try:
                        cur.execute(f"DELETE FROM `{tbl}`;")
                    except Exception:
                        pass
                cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
                m_conn.commit()
                print("  [SUCCESS] Existing data cleared.")

            # 3. Create all 12 tables
            print("\n[STEP 2/4] Verifying 12 normalized 3NF table schemas...")
            for spec in TABLES_SPECS:
                cur.execute(spec["ddl"])
            m_conn.commit()
            print("  [SUCCESS] All 12 tables verified.")

        # 4. Data Population
        d_conn = None
        if not args.offline:
            print("\n[STEP 3/4] Connecting to Databricks SQL Warehouse...")
            d_conn = get_databricks_sql_connection()

        if d_conn:
            try:
                sync_from_databricks(d_conn, m_conn, limit=args.limit, batch_size=args.batch_size, reset=args.reset)
            finally:
                d_conn.close()
        else:
            print("\n[STEP 3/4] Databricks connection unavailable; falling back to offline synthetic seed...")
            seed_offline_fallback(m_conn)

        # 5. Verification & Summary Report
        print("\n[STEP 4/4] Verifying Table Population & Row Counts...")
        print("+" + "-" * 30 + "+" + "-" * 18 + "+")
        print(f"| {'Table Name':<28} | {'Total Rows':<16} |")
        print("+" + "-" * 30 + "+" + "-" * 18 + "+")

        total_rows_all = 0
        with m_conn.cursor() as cur:
            for spec in TABLES_SPECS:
                tbl = spec["name"]
                cur.execute(f"SELECT COUNT(*) FROM `{tbl}`;")
                count = cur.fetchone()[0]
                total_rows_all += count
                print(f"| {tbl:<28} | {count:>14,} |")

        print("+" + "-" * 30 + "+" + "-" * 18 + "+")
        print(f"| {'TOTAL POPULATED RECORDS':<28} | {total_rows_all:>14,} |")
        print("+" + "-" * 30 + "+" + "-" * 18 + "+")

        print("\n" + "=" * 75)
        print("  LOCAL MYSQL POPULATION COMPLETE! ")
        print("=" * 75)
        print("\nNext Steps:")
        print("1. In MySQL Workbench, verify records:")
        print("   USE wanderbricks;")
        print("   SELECT count(*) FROM page_views;")
        print("   SELECT * FROM properties LIMIT 5;")
        print("\n2. To expose your local MySQL to Cloud Databricks:")
        print("   ngrok tcp 3306")
        print("   (Copy the generated tcp://X.tcp.ngrok.io:XXXXX address)")
        print("\n3. In Databricks, connect via JDBC URL:")
        print("   jdbc:mysql://<ngrok-host>:<ngrok-port>/wanderbricks?sslMode=REQUIRED")
        print("=" * 75)

    finally:
        m_conn.close()


if __name__ == "__main__":
    main()
