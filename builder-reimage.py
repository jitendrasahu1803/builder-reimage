#!.venv/bin/python3

import asyncio
import argparse
import json
import os
import sys
import time
import configparser
from cryptography.fernet import Fernet
from datetime import datetime
from maas.client import connect
from aiohttp.client_exceptions import ClientConnectorError, ServerDisconnectedError, ClientResponseError, ClientOSError

LOG_FILE_DEFAULT = "maas_redeploy.log"


# -------------------- Logging -------------------- #
def log(msg, log_file=None):
    """Print and optionally log to a file."""
    print(msg)
    if log_file:
        with open(log_file, "a") as f:
            f.write(f"{datetime.now().isoformat()} - {msg}\n")

# -------------------- Load Configuration from maas.conf file  -------------------- #
config = configparser.ConfigParser()
config.read('maas.conf')

MAAS_URL = config.get('maas', 'maas_url')

# --------------------  Read encrypted files and decrypt API key -------------------- #
with open('maas_api.key', 'rb') as key_file:
    encryption_key = key_file.read()

fernet = Fernet(encryption_key)

with open('maas_api_key.encrypted', 'rb') as enc_file:
    encrypted_api_key = enc_file.read()

MAAS_API_KEY = fernet.decrypt(encrypted_api_key).decode().strip()

# -------------------- Core Functions -------------------- #
async def connect_maas(maas_url, api_key, retries=3):
    """Connect to MAAS with retries and clear error handling."""
    for attempt in range(1, retries + 1):
        try:
            client = await connect(maas_url, apikey=api_key)
            return client
        except (ClientConnectorError, ClientOSError) as e:
            print(f"Connection attempt {attempt} failed: {e}")
        except ClientResponseError as e:
            print(f"MAAS server responded with error: {e.status} {e.message}")
        except asyncio.TimeoutError:
            print(f"Connection timed out on attempt {attempt}")
        except ServerDisconnectedError:
            print(f"Server disconnected unexpectedly (attempt {attempt})")
        if attempt < retries:
            print("Retrying in 2 seconds...")
            await asyncio.sleep(2)
    print("Error: Unable to connect to MAAS after several attempts.")
    print("Please check:")
    print("  • Network or VPN connection")
    print("  • MAAS server hostname and port")
    print("  • MAAS service availability")
    sys.exit(1)


async def list_machines(client, log_file=None):
    """List all MAAS machines."""
    machines = await client.machines.list()
    log(f"{'Hostname':20} | {'System ID':10} | {'Status':10} | {'OS':10}", log_file)
    log("-" * 65, log_file)
    for m in machines:
        os_name = m.distro_series if getattr(m, "distro_series", None) else "-"
        log(f"{m.hostname:20} | {m.system_id:10} | {m.status_name:10} | {os_name:10}", log_file)
    return machines


async def query_machine(client, hostname, log_file=None, quiet=False):
    """Query a single machine and display detailed information."""
    machines = await client.machines.list()
    for m in machines:
        if m.hostname == hostname:
            # Fetch latest machine data to ensure up-to-date power info
            machine = await client.machines.get(system_id=m.system_id)

            os_name = getattr(machine, "distro_series", None) or "-"
            os_type = getattr(machine, "osystem", None) or "-"
            owner = getattr(machine, "owner", None)
            owner_display = owner.get("username") if isinstance(owner, dict) and "username" in owner else str(owner or "-")
            power_state = getattr(machine, "power_state", "Unknown")
            power_type = getattr(machine, "power_type", "Unknown")
#            cpu_count = getattr(machine, "cpu_count", "N/A")

            if not quiet:
                log(f"\nMachine Details\n{'-'*60}", log_file)
                log(
                    f"Name: {machine.hostname}\n"
                    f"System ID: {machine.system_id}\n"
                    f"Status: {machine.status_name}\n"
                    f"OS Distro: {os_name}\n"
                    f"OS Type: {os_type}\n"
                    f"Owner: {owner_display}\n"
                    f"Power Type: {power_type}\n"
                    f"Power Status: {power_state}",
                    log_file
                )
            return machine

    log(f"Machine '{hostname}' not found.", log_file)
    return None

async def list_distros(client, log_file=None):
    """List all available OS distributions and their release versions."""
    resources = await client.boot_resources.list()
    log(f"{'ID':<5} | {'OS Type':<20} | {'Release':<20} | {'Architecture':<15}", log_file)
    log("-" * 70, log_file)
    
    for res in resources:
        # Many MAAS versions store distro info in 'name' like 'ubuntu/focal' or 'centos/stream9'
        name = getattr(res, "name", "-")
        os_type, release = "-", "-"
        if "/" in name:
            parts = name.split("/")
            os_type = parts[0]
            if len(parts) > 1:
                release = parts[1]
        elif getattr(res, "osystem", None):
            os_type = res.osystem
        elif getattr(res, "distro_series", None):
            release = res.distro_series

        arch = getattr(res, "architecture", "-")
        log(f"{res.id:<5} | {os_type:<20} | {release:<20} | {arch:<15}", log_file)

async def get_status(client, hostname, log_file=None):
    """Check current status of a machine."""
    m = await query_machine(client, hostname, log_file, quiet=True)
    if m:
        log(f"{hostname} → Current Status: {m.status_name}", log_file)
        return m.status_name

# -------------------- find last deployed machine -------------------- #
async def find_last_deployed_machine(client):
    """Return the machine object that was most recently deployed.

    Heuristics used:
    - Try to read several common timestamp attributes on a machine object.
    - Parse ISO timestamps where available and pick the most recent.
    - If no timestamps are available, fall back to the latest machine with status 'Deployed'.
    """
    machines = await client.machines.list()
    candidates = []

    timestamp_attrs = [
        "deployment_started_at", "deployed_at", "deployment_finished_at",
        "commissioning_finished_at", "last_updated", "updated_at", "created_at", "created"
    ]

    for m in machines:
        # Only consider machines that are in Deployed state
        if getattr(m, "status_name", "").lower() != "deployed":
            continue

        ts = None
        for attr in timestamp_attrs:
            val = getattr(m, attr, None)
            if not val:
                continue
            # val may be a datetime already or a string
            try:
                if isinstance(val, str):
                    # Support ISO format and fallback gracefully
                    ts = datetime.fromisoformat(val)
                elif isinstance(val, datetime):
                    ts = val
                else:
                    # Try converting numeric timestamps
                    ts = datetime.fromtimestamp(float(val))
                break
            except Exception:
                # ignore parse errors and try next attr
                ts = None

        candidates.append((ts, m))

    # Select the machine with the latest timestamp (None values will be sorted last)
    candidates = [c for c in candidates if c[0] is not None]
    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

    # Fallback: return any machine with status 'Deployed' (choose the first)
    for m in machines:
        if getattr(m, "status_name", "").lower() == "deployed":
            return m

    return None

async def release_machine(client, machine):
    """Release a machine if deployed."""
    if machine.status_name.lower() == "deployed":
        print(f"Releasing machine {machine.hostname}...")
        await machine.release()


async def wait_for_status(client, system_id, expected_status, timeout=900):
    """Wait for a machine to reach a specific status."""
    start = time.time()
    while time.time() - start < timeout:
        m = await client.machines.get(system_id=system_id)
        if m.status_name.lower() == expected_status.lower():
            print(f"{m.hostname} reached status '{expected_status}'")
            return True
        await asyncio.sleep(10)
    print(f"Timeout waiting for {system_id} to reach {expected_status}")
    return False


#async def deploy_machine(client, machine, os_release):
#    """Deploy a machine with given OS."""
#    print(f"Deploying {machine.hostname} with OS {os_release}...")
#    await machine.deploy(distro_series=os_release)

async def deploy_machine(client, machine_name, os_distro, log_file=None):
    """
    Deploy the machine with the given OS only if not already deployed.
    If already deployed → return details.
    """

    try:
        machines = await client.machines.list()
        machine = next((m for m in machines if m.hostname == machine_name), None)

        if not machine:
            msg = f"[ERROR] Machine '{machine_name}' not found."
            log(msg, log_file)
            return

        status = machine.status_name.lower()

        # Already deployed
        if status == "deployed":
            msg = (
                f"[INFO] Machine '{machine_name}' is already deployed.\n"
                f"System ID: {machine.system_id}\n"
                f"Status: {machine.status_name}\n"
                f"Owner: {getattr(machine, 'owner_data', {}).get('username', 'N/A')}\n"
                f"Power Type: {machine.power_type}"
            )
            log(msg, log_file)
            return

        # Non-deployable states
        if status in ["failed", "broken", "error", "unknown"]:
            msg = f"[ERROR] Machine '{machine_name}' is in non-deployable state: {machine.status_name}"
            log(msg, log_file)
            return

        # Trigger deployment
        msg = (
            f"[ACTION] Deploying '{machine_name}' "
            f"with OS: {os_distro} (System ID: {machine.system_id})..."
        )
        log(msg, log_file)

        await machine.deploy(osystem=os_distro)

        msg = f"[SUCCESS] Deployment triggered for '{machine_name}' with OS {os_distro}"
        log(msg, log_file)

    except Exception as e:
        msg = f"[ERROR] Deploy failed for '{machine_name}': {str(e)}"
        log(msg, log_file)
        return

async def redeploy_machine(client, hostname, os_release=None, log_file=None):
    """Redeploy a single machine."""
    machine = await query_machine(client, hostname, log_file)
    if not machine:
        return

    current_os = getattr(machine, "distro_series", None) or "focal"
    os_to_use = os_release or current_os

    if machine.status_name.lower() == "deployed":
        await release_machine(client, machine)
        await wait_for_status(client, machine.system_id, "Ready")

    await deploy_machine(client, machine, os_to_use)
    await wait_for_status(client, machine.system_id, "Deployed")

    log(f"Machine {hostname} successfully redeployed with {os_to_use}", log_file)


async def redeploy_all(client, os_release=None, log_file=None):
    """Redeploy all machines."""
    machines = await client.machines.list()
    for m in machines:
        os_to_use = os_release or getattr(m, "distro_series", "focal")
        await redeploy_machine(client, m.hostname, os_to_use, log_file)


# -------------------- Main Entry -------------------- #
async def main():
    parser = argparse.ArgumentParser(description="MAAS Reimage Automation Script")
    parser.add_argument("--action", required=True, choices=[
        "list", "list-distros", "query", "status", "deploy", "redeploy", "redeploy-all", "last-deployed"
    ], help="Action to perform on MAAS machines")
    parser.add_argument("--machine", help="Target hostname (use with --action query/status/redeploy)")
    parser.add_argument("--os", help="Specify OS release (e.g. focal, jammy, centos-9-stream)")
    parser.add_argument("--log-file", help="Optional log file path")

    args = parser.parse_args()
    client = await connect_maas(MAAS_URL, MAAS_API_KEY)
    log_file = args.log_file or LOG_FILE_DEFAULT

    if args.action == "list":
        await list_machines(client, log_file)
    elif args.action == "list-distros":
        await list_distros(client, log_file)
    elif args.action == "query":
        if not args.machine:
            print("Please specify --machine for query action.")
        else:
            await query_machine(client, args.machine, log_file)
    elif args.action == "status":
        if not args.machine:
            print("Please specify --machine for status action.")
        else:
            await get_status(client, args.machine, log_file)
    elif args.action == "deploy":
        if not args.machine:
            print("Please specify --machine for deploy action")
        else:
            await deploy_machine(client, args.machine, args.os, log_file)
    elif args.action == "redeploy":
        if not args.machine:
            print("Please specify --machine for redeploy action.")
        else:
            await redeploy_machine(client, args.machine, args.os, log_file)
    elif args.action == "redeploy-all":
        await redeploy_all(client, args.os, log_file)
    elif args.action == "last-deployed":
        m = await find_last_deployed_machine(client)
        if m:
            log(f"Last deployed machine: {m.hostname} ({m.system_id})", log_file)
        else:
            log("No deployed machines found.", log_file)
    else:
        print("Invalid action selected.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nScript interrupted by user.")
    except Exception as e:
        print(f"Error: {e}")
