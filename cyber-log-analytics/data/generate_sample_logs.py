"""
Generates a synthetic cybersecurity log dataset (CSV) for use as:
  - historical training data for the ML anomaly model
  - a replay source for the log producer

Simulates normal traffic plus a minority of well-known attack *patterns*
(brute-force login bursts, port-scan-like port fan-out, and abnormally
large transfers) purely as labeled numeric/categorical features -- no
real exploit code or working attack logic is generated here.
"""
import csv
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

fake = Faker()
random.seed(42)

EVENT_TYPES = ["auth", "network", "file_access", "dns", "http"]
ACTIONS = ["allow", "deny", "login_success", "login_failed", "logout"]
PROTOCOLS = ["TCP", "UDP", "ICMP"]
USERS = [fake.user_name() for _ in range(40)]
NORMAL_PORTS = [22, 80, 443, 3306, 5432, 8080, 53]


def random_ip(private=False):
    if private:
        return f"10.0.{random.randint(0, 255)}.{random.randint(1, 254)}"
    return fake.ipv4_public()


def base_row(ts, label="normal"):
    return {
        "log_id": str(uuid.uuid4()),
        "timestamp": ts.isoformat(),
        "src_ip": random_ip(private=True),
        "dst_ip": random_ip(),
        "src_port": random.randint(1024, 65535),
        "dst_port": random.choice(NORMAL_PORTS),
        "protocol": random.choice(PROTOCOLS),
        "event_type": random.choice(EVENT_TYPES),
        "action": random.choice(["allow", "login_success", "logout"]),
        "user": random.choice(USERS),
        "bytes_sent": random.randint(200, 20000),
        "bytes_received": random.randint(200, 20000),
        "duration_ms": random.randint(5, 4000),
        "failed_login_count_5m": 0,
        "unique_ports_contacted_1m": random.randint(1, 3),
        "label": label,
    }


def brute_force_burst(ts, attacker_ip, user):
    rows = []
    for i in range(random.randint(6, 15)):
        row = base_row(ts + timedelta(seconds=i * 2), label="brute_force")
        row["src_ip"] = attacker_ip
        row["dst_port"] = 22
        row["event_type"] = "auth"
        row["action"] = "login_failed"
        row["user"] = user
        row["failed_login_count_5m"] = i + 1
        row["bytes_sent"] = random.randint(50, 300)
        row["bytes_received"] = random.randint(50, 300)
        rows.append(row)
    return rows


def port_scan_burst(ts, attacker_ip, target_ip):
    rows = []
    ports = random.sample(range(1, 65535), random.randint(15, 40))
    for i, port in enumerate(ports):
        row = base_row(ts + timedelta(milliseconds=i * 200), label="port_scan")
        row["src_ip"] = attacker_ip
        row["dst_ip"] = target_ip
        row["dst_port"] = port
        row["event_type"] = "network"
        row["action"] = "deny"
        row["bytes_sent"] = random.randint(40, 80)
        row["bytes_received"] = 0
        row["unique_ports_contacted_1m"] = i + 1
        rows.append(row)
    return rows


def large_transfer_event(ts):
    row = base_row(ts, label="large_transfer")
    row["event_type"] = "file_access"
    row["action"] = "allow"
    row["bytes_sent"] = random.randint(60_000_000, 300_000_000)
    row["duration_ms"] = random.randint(5000, 60000)
    return row


def blacklisted_ip_event(ts, blacklisted_ip):
    row = base_row(ts, label="blacklisted_ip")
    row["src_ip"] = blacklisted_ip
    row["event_type"] = "network"
    row["action"] = "deny"
    return row


def generate_dataset(num_normal=4000, num_incidents=60, out_path="sample_security_logs.csv"):
    start = datetime.utcnow() - timedelta(days=2)
    rows = []

    for i in range(num_normal):
        rows.append(base_row(start + timedelta(seconds=i * 3)))

    blacklisted = ["203.0.113.66", "198.51.100.23", "192.0.2.187"]

    for _ in range(num_incidents):
        ts = start + timedelta(seconds=random.randint(0, num_normal * 3))
        kind = random.choice(["brute_force", "port_scan", "large_transfer", "blacklisted_ip"])
        if kind == "brute_force":
            rows.extend(brute_force_burst(ts, random_ip(), random.choice(USERS)))
        elif kind == "port_scan":
            rows.extend(port_scan_burst(ts, random_ip(), random_ip(private=True)))
        elif kind == "large_transfer":
            rows.append(large_transfer_event(ts))
        else:
            rows.append(blacklisted_ip_event(ts, random.choice(blacklisted)))

    rows.sort(key=lambda r: r["timestamp"])

    fieldnames = list(rows[0].keys())
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {out_path}")


if __name__ == "__main__":
    generate_dataset()
