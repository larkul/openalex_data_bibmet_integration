import requests
import json
import time
from datetime import datetime

# Parameters
mailto = "lars.kullman@ub.gu.se"
ror_id = "01tm6cn81"
start_year = 2015
end_year = 2024

# Corrected filter and base URL
filter = f"authorships.institutions.ror:{ror_id},publication_year:{start_year}-{end_year}"
base_url = "https://api.openalex.org/works"
params = {
    "filter": filter,
    "per_page": 200,
    "mailto": mailto,
    "cursor": "*"
}

works = []
page_count = 0

print("Starting data collection...")

while True:
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        print(f"\nError fetching data: {e}")
        break

    results = data.get("results", [])
    meta = data.get("meta", {})
    total = meta.get("count", 0)
    next_cursor = meta.get("next_cursor")

    works.extend(results)
    page_count += 1

    print(f"\rPage {page_count}: {len(works)}/{total} works collected ({(len(works)/total*100):.1f}%)", end="")

    if not next_cursor:
        break

    params["cursor"] = next_cursor
    time.sleep(0.5)

print(f"\nTotal works collected: {len(works)}")

# Save all data to JSON file
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"openalex_ror_{ror_id}_{start_year}-{end_year}_{timestamp}.json"

with open(filename, 'w', encoding='utf-8') as f:
    json.dump(works, f, ensure_ascii=False, indent=2)

print(f"Data saved to {filename}")

# Optional: Print a sample of the first work
if works:
    print("\nSample of first work (first few fields):")
    first_work = works[0]
    sample = {k: first_work[k] for k in list(first_work.keys())[:5]}
    print(json.dumps(sample, indent=2))
