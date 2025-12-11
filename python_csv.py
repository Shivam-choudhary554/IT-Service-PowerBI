import csv
import random
from datetime import datetime, timedelta

OUTPUT_FILE = "incidents_10M.csv"
TOTAL_ROWS = 10_000_000
CHUNK_SIZE = 100_000  # writes 100k rows at a time

# Predefine some lookup lists to avoid recomputing
PRIORITIES = ["P1", "P2", "P3", "P4"]
CATEGORIES = ["Hardware", "Software", "Network", "Access", "MDM", "Security"]
SUBCATEGORIES = [
    "Enrollment", "Policy sync", "App install", "Password reset",
    "WiFi", "Display", "MDM profile", "Antivirus"
]
STATUSES = ["Open", "In progress", "On hold", "Resolved", "Closed"]
GROUPS = ["Service Desk", "MDM", "Endpoint", "Network"]
CHANNELS = ["Email", "Portal", "Phone", "Chat"]

START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 12, 31)

def random_datetime(start, end):
    delta = end - start
    seconds = random.randrange(int(delta.total_seconds()))
    return start + timedelta(seconds=seconds)

def generate_row(i: int):
    ticket_id = f"INC{str(i).zfill(8)}"
    created = random_datetime(START_DATE, END_DATE)
    # resolution time between 10 minutes and 5 days
    resolve_delta = timedelta(minutes=random.randint(10, 5 * 24 * 60))
    resolved = created + resolve_delta

    status = random.choices(
        STATUSES,
        weights=[5, 10, 5, 50, 30],  # more Resolved/Closed
        k=1
    )[0]

    priority = random.choices(PRIORITIES, weights=[5, 15, 50, 30], k=1)[0]
    category = random.choice(CATEGORIES)
    subcategory = random.choice(SUBCATEGORIES)
    asset_id = f"AT-{random.randint(1, 200_000):06d}"
    assigned_group = random.choice(GROUPS)
    assignee = f"user_{random.randint(1, 2000)}"
    channel = random.choice(CHANNELS)

    sla_target_hours = {
        "P1": 4,
        "P2": 8,
        "P3": 24,
        "P4": 72
    }[priority]

    time_to_respond_minutes = random.randint(1, 240)
    time_to_resolve_minutes = int(resolve_delta.total_seconds() // 60)
    breached_sla = "Yes" if time_to_resolve_minutes > sla_target_hours * 60 else "No"
    customer_sat_score = random.randint(1, 5)

    short_description = f"{category} issue - {subcategory}"

    return [
        ticket_id,
        created.isoformat(sep=" "),
        resolved.isoformat(sep=" "),
        status,
        priority,
        category,
        subcategory,
        short_description,
        asset_id,
        assigned_group,
        assignee,
        channel,
        sla_target_hours,
        time_to_respond_minutes,
        time_to_resolve_minutes,
        breached_sla,
        customer_sat_score,
    ]

def main():
    with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # header
        writer.writerow([
            "ticket_id",
            "created_datetime",
            "resolved_datetime",
            "status",
            "priority",
            "category",
            "subcategory",
            "short_description",
            "asset_id",
            "assigned_group",
            "assignee",
            "channel",
            "sla_target_hours",
            "time_to_respond_minutes",
            "time_to_resolve_minutes",
            "breached_sla",
            "customer_sat_score",
        ])

        rows_written = 0
        while rows_written < TOTAL_ROWS:
            rows_to_write = min(CHUNK_SIZE, TOTAL_ROWS - rows_written)
            batch = [generate_row(rows_written + i + 1) for i in range(rows_to_write)]
            writer.writerows(batch)
            rows_written += rows_to_write
            print(f"Wrote {rows_written:,} / {TOTAL_ROWS:,} rows")

if __name__ == "__main__":
    main()
