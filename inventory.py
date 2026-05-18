from google.cloud import storage
from parameters import GoogleCloudParameters
import api
import json, re, math
from tqdm import tqdm
from typing import Callable

gcp_params = GoogleCloudParameters()
storage_client = storage.Client(project=gcp_params.project_id)
BUCKET = storage_client.bucket(gcp_params.bucket_name)

class InventoryManager:
    def __init__(
        self, 
        api_client: api.APIRequestClient | None = None,
        source: str | None = None,
        api_data_path: str = 'api',
        max_consecutive_failures: int | None = None
):
        self.bucket = BUCKET
        self._index = None
        self.source = source
        self.prefix = f'{source}/{api_data_path}'
        self.api_client = api_client
        self.max_consecutive_failures = max_consecutive_failures

    @property
    def index(self):
        if self._index is None:
            f = f'{self.source}/index.json'
            self._index = self.download_as_json(f)
            if self.max_consecutive_failures is None:
                n = len(self._index)
                self.max_consecutive_failures = n // 4
        return self._index

    @staticmethod
    def int_after(key: str, text: str) -> int | None:
        m = re.search(rf'"{re.escape(key)}"\s*:\s*(\d+)', text)
        return int(m.group(1)) if m else None

    def _build_md_blob(self, **kwargs):
        return f'{self.prefix}/{kwargs["comp_id"]}/metadata.json'

    def _parse_md_blob(self, blob_name):
        return {'comp_id': blob_name.split('/')[-2]}

    def _build_divs_blob(self, **kwargs):
        return f'{self.prefix}/{kwargs["comp_id"]}/divisions.json'

    def _parse_divs_blob(self, blob_name):
        return {'comp_id': blob_name.split('/')[-2]}
    
    def _build_lb_pg_blob(self, **kwargs):
        comp_id = kwargs['comp_id']
        div_id = kwargs['div_id']
        page = kwargs['page']

        return '/'.join([
            self.prefix, 
            str(comp_id), 
            'leaderboard', 
            f'{div_id}_{page}.json'
        ])

    def _parse_lb_pg_blob(self, blob_name):
        comp_id = blob_name.split('/')[-3]
        base = blob_name.split('/')[-1].split('_')
        div_id = '_'.join(base[:-1])
        page = int(base[-1].split('.')[0])

        return {
            'comp_id': comp_id,
            'div_id': div_id,
            'page': page
        }


    def _blob_exists(self, blob_name):
        blob = self.bucket.blob(blob_name)
        return blob.exists()

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

    def _get_lb_pg_cnt(self, **kwargs):
        return 1

    def _load_or_fetch(
        self, 
        build_method: Callable,
        fetch_method: Callable,
        refresh: bool = False, 
        **kwargs
    ): 
        path = build_method(**kwargs)
        if not self._blob_exists(path) or refresh:
            data = fetch_method(**kwargs)
            self.upload_as_json(data, path)
        else:
            data = self.download_as_json(path)
        return data

    def _list_blobs_matching(self, match_glob: str):
        return {
            blob.name for blob in 
            self.bucket.list_blobs(match_glob=match_glob)
        }

    def _load_progress(
        self, kwargs_list: list[dict],
        load_method: Callable,
        refresh: bool = False
    ):
        failed_kwargs_list = []
        consecutive_failures = 0
        for kwargs in tqdm(kwargs_list):
            try:
                load_method(**kwargs, refresh=refresh)
                consecutive_failures = 0
            except Exception as e:
                consecutive_failures += 1
                failed_kwargs_list.append(kwargs)
                if consecutive_failures >= self.max_consecutive_failures:
                    print(f"Max consecutive failures reached: {consecutive_failures}")
                    return failed_kwargs_list
        return

    def load_metadata(
        self,
        refresh: bool = False,
        **kwargs
    ):
        return self._load_or_fetch(
            self._build_md_blob,
            self.api_client.fetch_metadata,
            refresh,
            **kwargs
        )
    
    def load_divisions(
        self,
        refresh: bool = False,
        **kwargs
    ):
        return self._load_or_fetch(
            self._build_divs_blob,
            self.api_client.fetch_divisions,
            refresh,
            **kwargs
        )

    def load_leaderboard_page(
        self,
        refresh: bool = False,
        **kwargs
    ):
        return self._load_or_fetch(
            self._build_lb_pg_blob,
            self.api_client.fetch_leaderboard_page,
            refresh,
            **kwargs
        )

    def _get_missing_kwargs_list(
        self, kwargs_list: list[dict],
        build_method: Callable,
        refresh: bool = False
    ):
        expected_files = {
            build_method(**kwargs)
            for kwargs in kwargs_list
        }

        match_glob = build_method(**{k: '*' for k in kwargs_list[0].keys()})
        existing_files = self._list_blobs_matching(match_glob)

        if refresh:
            existing_files = set()

        missing_files = expected_files - existing_files
        missing_kwargs_list = [
            kwargs for kwargs in kwargs_list
            if build_method(**kwargs) in missing_files
        ]
        return missing_kwargs_list
        
    def run_metadata_inventory(self, refresh: bool = False):
        kwargs_list = [
            {'comp_id': c['source_comp_id']}
            for c in self.index
        ]

        missing_kwargs_list = self._get_missing_kwargs_list(
            kwargs_list,
            self._build_md_blob,
            refresh=refresh
        )

        if len(missing_kwargs_list) == 0:
            return

        self._load_progress(
            missing_kwargs_list,
            self.load_metadata,
            refresh=refresh
        )

    def run_divisions_inventory(self, refresh: bool = False):
        kwargs_list = [
            {'comp_id': c['source_comp_id']}
            for c in self.index
        ]
        missing_kwargs_list = self._get_missing_kwargs_list(
            kwargs_list,
            self._build_divs_blob,
            refresh=refresh
        )
        if len(missing_kwargs_list) == 0:
            return
        self._load_progress(
            missing_kwargs_list,
            self.load_divisions,
            refresh=refresh
        )

    def _run_lb_inv_pg_1(
        self, refresh: bool = False
    ):
        kwargs_list = [
            {
                'comp_id': c['source_comp_id'],
                'div_id': d,
                'page': 1
            }
            for c in self.index
            for d in [c['division_male'], c['division_female']]
        ]
        missing_kwargs_list = self._get_missing_kwargs_list(
            kwargs_list,
            self._build_lb_pg_blob,
            refresh=refresh
        )
        if len(missing_kwargs_list) == 0:
            return
        self._load_progress(
            missing_kwargs_list,
            self.load_leaderboard_page,
            refresh=refresh
        )

    def run_leaderboard_inventory(
        self, refresh: bool = False
    ):
        self._run_lb_inv_pg_1(refresh)
        kwargs_list = [
            {
                'comp_id': c['source_comp_id'],
                'div_id': d,
            }
            for c in self.index
            for d in [c['division_male'], c['division_female']]
        ]
        kwargs_list = [
            {
                **kwargs,
                'page': p
            }
            for kwargs in kwargs_list
            for p in range(2, self._get_lb_pg_cnt(**kwargs) + 1)
        ]
        if len(kwargs_list) == 0:
            return
        
        missing_kwargs_list = self._get_missing_kwargs_list(
            kwargs_list,
            self._build_lb_pg_blob,
            refresh=refresh
        )
        if len(missing_kwargs_list) == 0:
            return
        self._load_progress(
            missing_kwargs_list,
            self.load_leaderboard_page,
            refresh=refresh
        )

class CompetitionCornerInventoryManager(InventoryManager):
    def __init__(self, api_data_path: str = 'api'):
        super().__init__(
            api_client = api.CompetitionCornerAPIRequestClient(),
            source='competition-corner',
            api_data_path=api_data_path
        )

class StrongestInventoryManager(InventoryManager):
    def __init__(self, api_data_path: str = 'api'):
        super().__init__(
            api_client = api.StrongestAPIRequestClient(),
            source='strongest',
            api_data_path=api_data_path
        )
        
    def _get_lb_pg_cnt(self, **kwargs):
        kwargs['page'] = 1
        f = self._build_lb_pg_blob(**kwargs)
        if not self._blob_exists(f):
            data = self.api_client.fetch_leaderboard_page(**kwargs)
            results = data['results']
            page_size = data['page_size']
        else:
            snip = self.download_snippet(f, -128, 0)
            results = self.int_after('results', snip)
            page_size = self.int_after('page_size', snip)
        return math.ceil(results / page_size)

class ScoreItInventoryManager(InventoryManager):
    def __init__(self, api_data_path: str = 'api'):
        super().__init__(
            api_client = api.ScoreItAPIRequestClient(),
            source='score-it',
            api_data_path=api_data_path
        )

    def load_divisions(self, **kwargs):
        data = self.load_metadata(**kwargs)
        return data['eventDivisions']

    def run_divisions_inventory(self, **kwargs):
        return