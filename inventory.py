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
    
    def _build_workout_blob(self, **kwargs):
        return f'{self.prefix}/{kwargs["comp_id"]}/workouts/{kwargs["div_id"]}.json'

    def _build_workout_description_blob(self, **kwargs):
        return f'{self.prefix}/{kwargs["comp_id"]}/workout_description/{kwargs["workout_id"]}.json'

    def _build_workout_schedule_blob(self, **kwargs):
        return f'{self.prefix}/{kwargs["comp_id"]}/workout_schedule/{kwargs["workout_id"]}_{kwargs["div_id"]}.json'

    def load_workouts(self, refresh: bool = False, **kwargs):
        return self._load_or_fetch(
            self._build_workout_blob,
            self.api_client.fetch_workouts,
            refresh,
            **kwargs
        )
    
    def load_workout_description(self, refresh: bool = False, **kwargs):
        return self._load_or_fetch(
            self._build_workout_description_blob,
            self.api_client.fetch_workout_description,
            refresh,
            **kwargs
        )

    def load_workout_schedule(self, refresh: bool = False, **kwargs):
        return self._load_or_fetch(
            self._build_workout_schedule_blob,
            self.api_client.fetch_workout_schedule,
            refresh,
            **kwargs
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

    def _build_event_configs_blob(self, **kwargs):
        return f'{self.prefix}/{kwargs["comp_id"]}/event-configs.json'

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

    def load_event_configs(self, refresh: bool = False, **kwargs):
        return self._load_or_fetch(
            self._build_event_configs_blob,
            self.api_client.fetch_event_configs,
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

class LocalCompInventoryManager(InventoryManager):
    def __init__(self, api_data_path: str = 'api'):
        super().__init__(
            api_client = api.LocalCompAPIRequestClient(),
            source='local-comp',
            api_data_path=api_data_path
        )

    def _build_divisions_blob(self, **kwargs):
        return f'{self.prefix}/{kwargs["comp_id"]}/divisions.json'

    def load_divisions(self, refresh: bool = False, **kwargs):
        return self._load_or_fetch(
            self._build_divisions_blob,
            self.api_client.fetch_divisions,
            refresh,
            **kwargs
        )

class Circle21InventoryManager(InventoryManager):
    def __init__(self, api_data_path: str = 'api'):
        super().__init__(
            api_client = api.Circle21APIRequestClient(),
            source='circle-21',
            api_data_path=api_data_path
        )
    
    def _build_workout_blob(self, **kwargs):
        return f'{self.prefix}/{kwargs["comp_id"]}/workouts.json'

    def load_workouts(self, refresh: bool = False, **kwargs):
        return self._load_or_fetch(
            self._build_workout_blob,
            self.api_client.fetch_workouts,
            refresh,
            **kwargs
        )

class CaptureFitInventoryManager(InventoryManager):
    def __init__(self, api_data_path: str = 'api'):
        super().__init__(
            api_client = api.CaptureFitAPIRequestClient(),
            source='capturefit',
            api_data_path=api_data_path
        )

    def _build_divisions_blob(self, **kwargs):
        return f'{self.prefix}/{kwargs["comp_id"]}/divisions.json'

    def _build_md_blob(self, **kwargs):
        return f'{self.prefix}/metadata-all.json'

    def load_metadata(self,refresh: bool = False,**kwargs):
        return self._load_or_fetch(
            self._build_md_blob,
            self.api_client.fetch_competitions,
            refresh=refresh,
            **kwargs
        )

    def load_divisions(
        self,
        comp_id: str,
        refresh: bool = False,
        **kwargs
    ):
        return self._load_or_fetch(
            self._build_divisions_blob,
            self.api_client.fetch_divisions,
            refresh,
            comp_id=comp_id
        )

    def load_leaderboard_page(
        self,
        comp_id: str,
        div_id: str,
        refresh: bool = False,
        page: int = 1,
        **kwargs
    ):
        divs = self.load_divisions(
            comp_id=comp_id,
            refresh=refresh
        )
        for div in divs:
            if div['_id'] == div_id:
                kwargs = {
                    'comp_id': comp_id,
                    'div_id': div_id,
                    'page': page,
                    'entrytype': div['entrytype'],
                    'category': div['category'],
                    'gender': div['gender'],
                }
                return self._load_or_fetch(
                    self._build_lb_pg_blob,
                    self.api_client.fetch_leaderboard_page,
                    **kwargs
                )

class BTWBWireInventoryManager(InventoryManager):
    def __init__(self, api_data_path: str = 'api'):
        super().__init__(
            api_client = api.BTWBWireAPIRequestClient(),
            source='btwb-thewire',
            api_data_path=api_data_path
        )

    def _build_config_blob(self, **kwargs):
        return f'{self.prefix}/{kwargs["comp_id"]}/config/{kwargs["div_id"]}.json'

    @staticmethod
    def _get_lb_pg_cnt(data):
        return data['Pages']

    def load_config(
        self,
        refresh: bool = False,
        **kwargs
    ):
        return self._load_or_fetch(
            self._build_config_blob,
            self.api_client.fetch_config,
            refresh,
            **kwargs)

    def load_leaderboard_page(
        self,
        refresh: bool = False,
        **kwargs
    ):
        config = self.load_config(
            refresh,
            **kwargs
        )
        kwargs['leaderboard_id'] = config['LeaderboardId']
        data = self._load_or_fetch(
            self._build_lb_pg_blob,
            self.api_client.fetch_leaderboard_page,
            refresh,
            **kwargs
        )
        return {**config, **data}

