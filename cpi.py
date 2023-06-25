import json
import csv
from functools import reduce
from bls_client import BLSClient
import os


def main():
    with open("data_ids/cpi_series.txt") as file:
        ids = file.read().strip().split("\n")
    #
    client = BLSClient(os.getenv("BLS_API_KEY"))
    results = client.bulk_load(ids, start_year=2005, end_year=2023)
    with open("cpi_items.json", "w") as output:
        output.write(json.dumps(results))

    csvs = [ client.convert_to_csv(bulk, ["item"]) for bulk in results ]
    data = reduce(lambda a, b: a + b[1:], csvs)
    with open("cpi_items.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(data)


if __name__ == "__main__":
    main()
