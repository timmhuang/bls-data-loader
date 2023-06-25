from collections import defaultdict

import requests
import json
from typing import List
from datetime import date


class BLSClient:
    def __init__(self, token):
        self._token = token
        self._bulk_size = 50  # default bulk size from BLS API

    def fetch(self, series, start_year=2005, end_year=None):
        if not end_year:
            end_year = date.today().year
        data = {"seriesid": series, "startyear": f"{start_year}",
                "endyear": f"{end_year}", "catalog": True,
                "registrationkey": self._token}
        res = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/",
                            json=data, headers={'Content-Type': 'application/json'})
        return res

    def bulk_load(self, series: List[str], start_year, end_year) -> List[dict]:
        bulk = []
        for i in range(0, len(series), self._bulk_size):
            batch = series[i: i + self._bulk_size]
            print(f"Fetching series starting with {batch[0]} and ending with {batch[-1]}")
            bulk.append(self.fetch(batch, start_year, end_year).json())
        return bulk

    def convert_to_csv(self, data: dict, categories: List[str], sort_by_id=False):
        # Prepare the data
        # We use a defaultdict which allows us to easily append to non-existent keys
        prepared_data = defaultdict(lambda: defaultdict(dict))

        # Iterate over each series object
        for series in data['Results']['series']:
            if "catalog" not in series:
                continue    # empty series

            prepared_data[series['seriesID']]['metadata'] = {
                cat: series['catalog'][cat] for cat in categories
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
        header_row = ['seriesId'] + categories + sorted(list(headers), reverse=True)
        csv = [header_row]

        keys = prepared_data.keys()
        if sort_by_id:
            keys = sorted(keys)
        for seriesID in keys:
            metadata = prepared_data[seriesID]['metadata']
            row = [seriesID] + [metadata[cat] for cat in categories] + [
                prepared_data[seriesID]['data'].get(header, '') for header in sorted(list(headers), reverse=True)
            ]
            csv.append(row)
        return csv
