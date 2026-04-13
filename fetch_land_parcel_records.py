import requests
import json
import time
from typing import List

BASE_URL = "https://gisportal.horrycounty.org/server/rest/services/ClosedData/ParcelLandRecords/FeatureServer/0/query"

BATCH_SIZE = 500
SLEEP_SECONDS = 0.2
OUTPUT_FILE = "parcel_land_records.json"


def get_all_object_ids() -> List[int]:
    params = {
        "where": "1=1",
        "returnIdsOnly": "true",
        "f": "json",
    }

    response = requests.get(BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    if "objectIds" not in data:
        raise ValueError(f"Did not receive objectIds. Response was: {data}")

    object_ids = data["objectIds"]
    object_ids.sort()
    return object_ids


def fetch_batch(id_batch: List[int]) -> List[dict]:
    params = {
        "objectIds": ",".join(map(str, id_batch)),
        "outFields": "*",
        "returnGeometry": "false",
        "f": "json",
    }

    response = requests.get(BASE_URL, params=params, timeout=120)
    response.raise_for_status()
    data = response.json()

    if "error" in data:
        raise ValueError(f"Server returned error: {data['error']}")

    return data.get("features", [])


def chunk_list(items: List[int], chunk_size: int):
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def main():
    print("Fetching object IDs...")
    object_ids = get_all_object_ids()
    print(f"Found {len(object_ids)} object IDs.")

    all_features = []

    for idx, batch in enumerate(chunk_list(object_ids, BATCH_SIZE), start=1):
        first_id = batch[0]
        last_id = batch[-1]
        print(f"Batch {idx}: fetching IDs {first_id} to {last_id} ({len(batch)} IDs)")

        try:
            features = fetch_batch(batch)
            all_features.extend(features)
            print(f"  Retrieved {len(features)} features. Running total: {len(all_features)}")
        except Exception as e:
            print(f"  Error fetching batch {idx}: {e}")

        time.sleep(SLEEP_SECONDS)

    output = {
        "features": all_features
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f)

    print(f"Done. Saved {len(all_features)} features to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()