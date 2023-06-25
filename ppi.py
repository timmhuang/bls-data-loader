#
# with open("ppi_commodities.txt") as file:
#     lines = [line.replace(" .", "") for line in file.read().split("\n")]
#
# codes = []
# for line in lines:
#     tokens = line.split(". ")
#     if len(tokens) < 2:
#         continue
#     codes.append("WPU" + ''.join(tokens[1].split(' ')[:2]))
#     print(codes[-1])
# with open("ppi_commodities_codes.txt", "w") as out:
#     out.write("\n".join(codes))
import json
import csv
from functools import reduce
from bls_client import BLSClient
import os


def main():
    with open("data_ids/ppi_commodities_codes.txt") as file:
        ids = file.read().split("\n")

    client = BLSClient(os.getenv("BLS_API_KEY"))
    results = client.bulk_load(ids, start_year=2005, end_year=2023)
    with open("ppi_commodities_data.json", "w") as output:
        output.write(json.dumps(results))

    with open("ppi_commodities_data.json") as file:
        results = json.loads(file.read())

    csvs = [ client.convert_to_csv(bulk, ["commerce_sector", "item"]) for bulk in results ]
    data = reduce(lambda a, b: a + b[1:], csvs)
    with open("ppi_commodities.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(data)


if __name__ == "__main__":
    main()
