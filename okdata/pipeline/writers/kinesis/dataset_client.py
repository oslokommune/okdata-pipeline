import os

import requests
from requests.exceptions import HTTPError, Timeout


class DatasetClient:
    metadata_api_url = os.environ["METADATA_API_URL"]

    def get_dataset(self, dataset_id, retries=3):
        get_url = f"{self.metadata_api_url}/datasets/{dataset_id}"
        try:
            response = requests.get(get_url)
            response.raise_for_status()
            return response.json()
        except (HTTPError, Timeout) as e:
            if retries > 0:
                return self.get_dataset(dataset_id, retries - 1)
            else:
                raise e
