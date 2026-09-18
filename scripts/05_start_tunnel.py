"""
Wanderbricks TCP Tunnel Launcher (05_start_tunnel.py)
======================================================
Starts a high-speed, zero-signup, zero-credit-card TCP tunnel using Bore.
Forwards traffic from public cloud relay (bore.pub) directly to local MySQL (127.0.0.1:3306).

Usage:
  python scripts/05_start_tunnel.py
"""

import os
import sys
import subprocess

# Ensure UTF-8 output on Windows consoles
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BORE_EXE = os.path.join(ROOT_DIR, "scripts", "bore.exe")


def check_or_download_bore():
    """Ensures bore.exe exists in scripts directory."""
    if os.path.exists(BORE_EXE):
        return True

    print("[INFO] bore.exe not found. Downloading modern TCP tunnel client...")
    url = "https://github.com/ekzhang/bore/releases/download/v0.5.2/bore-v0.5.2-x86_64-pc-windows-msvc.zip"
    zip_path = os.path.join(ROOT_DIR, "scripts", "bore.zip")
    
    import urllib.request
    import zipfile
    urllib.request.urlretrieve(url, zip_path)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(os.path.join(ROOT_DIR, "scripts"))
    if os.path.exists(zip_path):
        os.remove(zip_path)
    print("  [SUCCESS] bore.exe installed.")
    return True


def main():
    check_or_download_bore()

    print("=" * 75)
    print("   WANDERBRICKS TCP TUNNEL LAUNCHER (bore.pub -> localhost:3306)")
    print("=" * 75)
    print("  ✓ Zero credit card required")
    print("  ✓ Zero account signup required")
    print("  ✓ Forwarding traffic to local MySQL (127.0.0.1:3306)")
    print("-" * 75)
    print("Starting tunnel... Look for 'listening at bore.pub:PORT' below:\n")

    try:
        subprocess.run([BORE_EXE, "local", "3306", "--to", "bore.pub"], check=True)
    except KeyboardInterrupt:
        print("\n[INFO] Tunnel closed by user.")


if __name__ == "__main__":
    main()
