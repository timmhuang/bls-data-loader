import requests
import json
import csv
from collections import defaultdict
import os


def fetch(series):
    data = {"seriesid": series, "startyear": "2005",
            "endyear": "2023", "catalog": True,
            "registrationkey": os.getenv("BLS_API_KEY")}
    res = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/",
                        json=data, headers={ 'Content-Type': 'application/json'})
    return res.text


def process(input_name, output_name):
    with open(input_name) as input_file:
        json_string = input_file.read()

    # Parse the JSON string into a Python object
    data = json.loads(json_string)

    # Prepare the data
    # We use a defaultdict which allows us to easily append to non-existent keys
    prepared_data = defaultdict(lambda: defaultdict(dict))

    # Iterate over each series object
    for series in data['Results']['series']:
        # Store the commerce_sector and commerce_industry values
        prepared_data[series['seriesID']]['metadata'] = {
            'commerce_sector': series['catalog']['commerce_sector'],
            'commerce_industry': series['catalog']['commerce_industry']
        }
        # Iterate over each data object in the series
        for data_obj in series['data']:
            # We use year and period as a key in the dictionary
            key = f'{data_obj["year"]}-{data_obj["period"][1:]}'
            prepared_data[series['seriesID']]['data'][key] = data_obj['value']

    # Create a list of all unique keys for our header row
    headers = set()
    for series_data in prepared_data.values():
        for key in series_data['data'].keys():
            headers.add(key)

    # The header row starts with 'seriesId', 'commerce_sector', 'commerce_industry', followed by all unique headers
    header_row = ['seriesId', 'commerce_sector', 'commerce_industry'] + sorted(list(headers), reverse=True)

    # Open a CSV file for writing
    with open(output_name, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # Write the header row
        writer.writerow(header_row)

        # Write the data rows
        for seriesID in sorted(prepared_data.keys()):
            metadata = prepared_data[seriesID]['metadata']
            row = [seriesID, metadata['commerce_sector'], metadata['commerce_industry']] + [
                prepared_data[seriesID]['data'].get(header, '') for header in sorted(list(headers), reverse=True)
            ]
            writer.writerow(row)


def main():
    ids = ["CES0000000001","CES0500000001","CES0600000001","CES1000000001","CES1011330001","CES1021000001","CES1021100001","CES1021200001","CES1021210001","CES1021220001","CES1021230001","CES1021300001","CES2000000001","CES2023600001","CES2023610001","CES2023620001","CES2023700001","CES2023800001","CES2023800101","CES2023800201","CES3000000001","CES3100000001","CES3132100001","CES3132700001","CES3133100001","CES3133200001","CES3133300001","CES3133400001","CES3133410001","CES3133420001","CES3133440001","CES3133450001","CES3133460001","CES3133500001","CES3133600001","CES3133600101","CES3133700001","CES3133900001","CES3200000001","CES3231100001","CES3231300001","CES3231400001","CES3231500001","CES3232200001","CES3232300001","CES3232400001","CES3232500001","CES3232600001","CES3232900001","CES0800000001","CES4000000001","CES4142000001","CES4142300001","CES4142400001","CES4142500001","CES4200000001","CES4244100001","CES4244110001","CES4244120001","CES4244130001","CES4244400001","CES4244500001","CES4244900001","CES4244910001","CES4244920001","CES4245500001","CES4245510001","CES4245520001","CES4245600001","CES4245700001","CES4245800001","CES4245900001","CES4300000001","CES4348100001","CES4348200001","CES4348300001","CES4348400001","CES4348500001","CES4348600001","CES4348700001","CES4348800001","CES4349200001","CES4349300001","CES4422000001","CES5000000001","CES5051200001","CES5051300001","CES5051600001","CES5051700001","CES5051800001","CES5051900001","CES5500000001","CES5552000001","CES5552100001","CES5552200001","CES5552210001","CES5552211001","CES5552220001","CES5552230001","CES5552300001","CES5552400001","CES5553000001","CES5553100001","CES5553200001","CES5553300001","CES6000000001","CES6054000001","CES6054110001","CES6054120001","CES6054130001","CES6054140001","CES6054150001","CES6054160001","CES6054170001","CES6054180001","CES6054190001","CES6055000001","CES6056000001","CES6056100001","CES6056110001","CES6056120001","CES6056130001","CES6056132001","CES6056140001","CES6056150001","CES6056160001","CES6056170001","CES6056190001","CES6056200001","CES6500000001","CES6561000001","CES6562000001","CES6562000101","CES6562100001","CES6562110001","CES6562120001","CES6562130001","CES6562140001","CES6562150001","CES6562160001","CES6562190001","CES6562200001","CES6562300001","CES6562310001","CES6562320001","CES6562330001","CES6562390001","CES6562400001","CES6562410001","CES6562420001","CES6562430001","CES6562440001","CES7000000001","CES7071000001","CES7071100001","CES7071200001","CES7071300001","CES7072000001","CES7072100001","CES7072200001","CES8000000001","CES8081100001","CES8081200001","CES8081300001","CES9000000001","CES9091000001","CES9091100001","CES9091912001","CES9092000001","CES9092161101","CES9092200001","CES9093000001","CES9093161101","CES9093200001"]
    # for i in range(0, len(ids), 50):
    #     batch = ids[i: i+50]
    #     print(f"Fetching series starting with {batch[0]} and ending with {batch[-1]}")
    #     res = fetch(batch)
    #     with open(f"nfp_his_1995-2023_{i}.json", "w") as file:
    #         file.write(res)
    for id in ids:
        print(id)


if __name__ == "__main__":
    main()