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
        api_data_path: str = 'api'
    ):
        self.bucket = BUCKET
        self._index = None
        self.source = source
        self.prefix = f'{source}/{api_data_path}'
        self.api_client = api_client

    @property
    def index(self):
        if self._index is None:
            f = f'{self.source}/index.json'
            self._index = self.download_as_json(f)
        return self._index

    def _build_md_blob(self, **kwargs):
        return f'{self.prefix}/{kwargs["comp_id"]}/metadata.json'

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

    def _blob_exists(self, blob_name):
        blob = self.bucket.blob(blob_name)
        return blob.exists()

    def upload_as_json(self, data, blob_name):
        blob = self.bucket.blob(blob_name)
        blob.upload_from_string(json.dumps(data,indent=4))

    def download_as_json(self, blob_name):
        blob = self.bucket.blob(blob_name)
        return json.loads(blob.download_as_string().decode('utf-8'))

    @staticmethod
    def _get_lb_pg_cnt(data):
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

    def load_leaderboard(
        self,
        refresh: bool = False,
        **kwargs
    ):
        out = []
        kwargs['page'] = 1
        data = self.load_leaderboard_page(
            refresh=refresh,
            **kwargs
        )
        out.append({**kwargs, 'data': data})

        for page in range(2, self._get_lb_pg_cnt(data) + 1):
            kwargs['page'] = page
            data = self.load_leaderboard_page(
                refresh=refresh,
                **kwargs
            )
            out.append({**kwargs, 'data': data})
        return out

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

    @staticmethod
    def _get_lb_pg_cnt(data):
        results = data['data']['results']
        page_size = data['data']['page_size']
        return math.ceil(results / page_size)

    def _build_divisions_blob(self, **kwargs):
        return f'{self.prefix}/{kwargs["comp_id"]}/divisions.json'

    def _build_workouts_blob(self, **kwargs):
        return f'{self.prefix}/{kwargs["comp_id"]}/workouts.json'

    def _build_scoring_blob(self, **kwargs):
        return f'{self.prefix}/{kwargs["comp_id"]}/scoring_policies.json'

    def load_divisions(self, refresh: bool = False, **kwargs):
        return self._load_or_fetch(
            self._build_divisions_blob,
            self.api_client.fetch_divisions,
            refresh,
            **kwargs
        )

    def load_workouts(
        self, refresh: bool = False, **kwargs):
        return self._load_or_fetch(
            self._build_workouts_blob,
            self.api_client.fetch_workouts,
            refresh,
            **kwargs
        )
    
    def load_scoring_policies(self, refresh: bool = False, **kwargs):
        return self._load_or_fetch(
            self._build_scoring_blob,
            self.api_client.fetch_scoring_policies,
            refresh,
            **kwargs
        )

class ScoreItInventoryManager(InventoryManager):
    def __init__(self, api_data_path: str = 'api'):
        super().__init__(
            api_client = api.ScoreItAPIRequestClient(),
            source='score-it',
            api_data_path=api_data_path
        )

class CrossFitInventoryManager(InventoryManager):
    def __init__(self, api_data_path: str = 'api'):
        super().__init__(
            api_client = api.CrossFitAPIRequestClient(),
            source='crossfit',
            api_data_path=api_data_path
        )

    @staticmethod
    def _get_lb_pg_cnt(data):
        return int(data['pagination']['totalPages'])

    def _build_lb_pg_blob(self, **kwargs):
        comp_id = kwargs['comp_id']
        comp_type = kwargs['comp_type']
        division = kwargs['div_id']
        page = kwargs['page']
        
        if comp_type == 'open':
            scaled_str = '/scaled=0/'
        else:
            scaled_str = '/'
        
        return f'{self.prefix}/comp={comp_id}/division={division}{scaled_str}page={page}.json'
