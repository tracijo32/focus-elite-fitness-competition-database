from google.cloud import storage
from parameters import GoogleCloudParameters
import api
import json, re, math
from tqdm import tqdm

gcp_params = GoogleCloudParameters()
storage_client = storage.Client(project=gcp_params.project_id)
BUCKET = storage_client.bucket(gcp_params.bucket_name)

class SourceInventoryManager:
    def __init__(
        self, 
        api_client: api.APIRequestClient,
        source: str,
        api_data_path: str = 'api'
):
        self.bucket = BUCKET
        self._index = None
        self.source = source
        self.prefix = f'{source}/{api_data_path}'
        self.api_client = api_client

    def file_exists(self, blob_name):
        blob = self.bucket.blob(blob_name)
        return blob.exists()

    @staticmethod
    def int_after(key: str, text: str) -> int | None:
        m = re.search(rf'"{re.escape(key)}"\s*:\s*(\d+)', text)
        return int(m.group(1)) if m else None

    def upload_as_string(self, data, blob_name):
        blob = self.bucket.blob(blob_name)
        blob.upload_from_string(data)

    def download_as_string(self, blob_name):
        blob = self.bucket.blob(blob_name)
        return blob.download_as_string().decode('utf-8')

    def upload_as_json(self, data, blob_name):
        blob = self.bucket.blob(blob_name)
        blob.upload_from_string(json.dumps(data,indent=4))

    def download_as_json(self, blob_name):
        blob = self.bucket.blob(blob_name)
        return json.loads(blob.download_as_string().decode('utf-8'))

    def download_snippet(self,blob_name, start, end):
        blob = BUCKET.get_blob(blob_name)
        if blob is None:
            return None
        total_size = blob.size
        if start < 0:
            start = total_size + start
        if end <= 0:
            end = total_size + end
        if start > total_size:
            return bytes('')
        if end > total_size:
            end = total_size

        return blob.download_as_bytes(start=start, end=end).decode('utf-8')

    @property
    def index(self):
        if self._index is None:
            f = f'{self.source}/index.json'
            self._index = self.download_as_json(f)
        return self._index

    def load_metadata(self, comp_id, refresh: bool = False):
        path = f'{self.prefix}/{comp_id}/metadata.json'
        if not self.file_exists(path) or refresh:
            data = self.api_client.fetch_metadata(comp_id)
            self.upload_as_json(data, path)
        else:
            data = self.download_as_json(path)
        return data

    def load_divisions(self, comp_id, refresh: bool = False):
        path = f'{self.prefix}/{comp_id}/divisions.json'
        if not self.file_exists(path) or refresh:
            data = self.api_client.fetch_divisions(comp_id)
            self.upload_as_json(data, path)
        else:
            data = self.download_as_json(path)
        return data
        
    def load_leaderboard_page(self, comp_id, div_id, page: int = 1, refresh: bool = False):
        path = f'{self.prefix}/{comp_id}/leaderboard/{div_id}_{page}.json'
        if not self.file_exists(path) or refresh:
            data = self.api_client.fetch_leaderboard_page(comp_id, div_id, page)
            self.upload_as_json(data, path)
        else:
            data = self.download_as_json(path)
        return data

    def inventory_metadata(self):
        comp_ids = [
            c['source_comp_id']
            for c in self.index
        ]

        expected_files = {
            f'{self.prefix}/{c}/metadata.json'
            for c in comp_ids
        }

        existing_files = {
            blob.name for blob in 
            self.bucket.list_blobs(match_glob=f'{self.prefix}/*/metadata.json')
        }

        missing_files = expected_files - existing_files
        if len(missing_files) == 0:
            print("Metadata inventory complete")
            return

        print(f"Downloading {len(missing_files)} missing files")
        failed = []
        for file in tqdm(missing_files):
            comp_id = file.split('/')[-2]
            try:
                self.load_metadata(comp_id)
            except Exception as e:
                failed.append(comp_id)
        if len(failed) > 0:
            print(f"Failed to download {len(failed)} files")
            print(failed)
        print("Metadata inventory complete")
        return

    @staticmethod
    def get_leaderboard_page_count(comp_id, div_id):
        return 1

    def inventory_leaderboard_page_one(self):
        comp_divs = [
            (c['source_comp_id'], d)
            for c in self.index
            for d in [c['division_male'], c['division_female']]
        ]

        expected_files = {
            f'{self.prefix}/{c}/leaderboard/{d}_1.json'
            for c, d in comp_divs
        }

        existing_files = {
            blob.name for blob in 
            self.bucket.list_blobs(match_glob=f'{self.prefix}/*/leaderboard/*_1.json')
        }

        missing_files = expected_files - existing_files
        if len(missing_files) == 0:
            print(f"Leaderboard page 1 inventory complete")
            return

        print(f"Downloading {len(missing_files)} missing files")
        failed = []
        for file in tqdm(missing_files):
            comp_id = file.split('/')[-3]
            div_id = file.split('/')[-1].split('_')[0]
            try:
                self.load_leaderboard_page(comp_id, div_id, 1)
            except Exception as e:
                s = f"comp_id={comp_id},div_id={div_id}"
                failed.append(s)
        if len(failed) > 0:
            print(f"Failed to download {len(failed)} files")
            print(failed)
        print("Leaderboard page 1 inventory complete")
        return
        
    def inventory_leaderboard_pages(self):
        self.inventory_leaderboard_page_one()
        comp_divs = [
            (c['source_comp_id'], d)
            for c in self.index
            for d in [c['division_male'], c['division_female']]
        ]

        expected_files = {
            f'{self.prefix}/{c}/leaderboard/{d}_{p}.json'
            for c, d in comp_divs
            for p in range(1, self.get_leaderboard_page_count(c, d) + 1)
        }
        existing_files = {
            blob.name for blob in 
            self.bucket.list_blobs(match_glob=f'{self.prefix}/*/leaderboard/*_*.json')
        }

        missing_files = expected_files - existing_files
        if len(missing_files) == 0:
            print("Multi-page leaderboard inventory complete")
            return

        print(f"Downloading {len(missing_files)} missing files")
        failed = []
        for file in tqdm(missing_files):
            comp_id = file.split('/')[-3]
            div_id = file.split('/')[-1].split('_')[0]
            page = file.split('/')[-1].split('_')[1].split('.')[0]
            try:
                self.load_leaderboard_page(comp_id, div_id, page)
            except Exception as e:
                s = f"comp_id={comp_id},div_id={div_id},page={page}"
                failed.append(s)
        if len(failed) > 0:
            print(f"Failed to download {len(failed)} files")
            print(failed)
        print("Multi-page leaderboard inventory complete")
        return

class StrongestInventoryManager(SourceInventoryManager):
    def __init__(self, api_data_path: str = 'api'):
        super().__init__(
            api_client=api.StrongestAPIRequestClient(),
            source='strongest',
            api_data_path=api_data_path
        )

    def get_leaderboard_page_count(self, comp_id, div_id):
        f = f'{self.source}/api/{comp_id}/leaderboard/{div_id}_1.json'
        if not self.file_exists(f):
            data = self.api_client.fetch_leaderboard_page(comp_id, div_id, 1)
            results = data['results']
            page_size = data['page_size']
        else:
            snip = self.download_snippet(f, -128, 0)
            results = self.int_after('results', snip)
            page_size = self.int_after('page_size', snip)
        return math.ceil(results / page_size)
            