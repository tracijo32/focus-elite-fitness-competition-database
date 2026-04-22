from google.cloud import storage
from api import APIRequestClient, CrossFitAPIRequestClient
from models import CFCompetition, CFEntrant, CFScore
from parameters import GoogleCloudParameters
import json, re
from datetime import datetime, timezone, timedelta
from typing import Callable
from tqdm import tqdm

gcp_params = GoogleCloudParameters()

class StorageManager:
    def __init__(
        self,  
        api_client: APIRequestClient,
        cache_refresh_days: int = 1
    ):
        storage_client = storage.Client(project=gcp_params.project_id)
        bucket_name = gcp_params.bucket_name
        self.bucket = storage_client.bucket(bucket_name)
        self.api_client = api_client
        self._refresh = cache_refresh_days

    def _get_blob_created(self, blob_name: str):
        blob = self.bucket.blob(blob_name)
        if not blob.exists():
            return None
        blob = self.bucket.get_blob(blob_name)
        return blob.time_created

    def _get_blob_last_modified(self, blob_name: str):
        blob = self.bucket.blob(blob_name)
        if not blob.exists():
            return None
        blob = self.bucket.get_blob(blob_name)
        return blob.updated

    def _download_json_blob(self, blob_name: str):
        blob = self.bucket.blob(blob_name)
        return json.loads(blob.download_as_string())

    def _load_cache_json(
        self, 
        blob_name: str, 
        cache_attribute_name: str,
        fetch_function: Callable,
        fetch_function_args: dict = {},
        fetch_function_kwargs: dict = {},
        ):
        if hasattr(self, cache_attribute_name):
            return getattr(self, cache_attribute_name)
        
        blob = self.bucket.blob(blob_name)
        if not blob.exists():
            needs_refresh = True
        else:
            last_modified = self._get_blob_last_modified(blob_name)
            if last_modified is None:
                last_modified = self._get_blob_created(blob_name)
            refresh_deadline = datetime.now(timezone.utc) - timedelta(days=self._refresh)
            needs_refresh = last_modified < refresh_deadline
        
        if needs_refresh:
            json_data = fetch_function(**fetch_function_args, **fetch_function_kwargs)
            blob.upload_from_string(json.dumps(json_data))
            setattr(self, cache_attribute_name, json_data)
        else:
            json_data = self._download_json_blob(blob_name)
            setattr(self, cache_attribute_name, json_data)

        return json_data

class CFStorageManager(StorageManager):
    def __init__(self):
        super().__init__(CrossFitAPIRequestClient())

    @property
    def competitions_json(self):
        return self._load_cache_json(
            blob_name='crossfit/competitions.json',
            cache_attribute_name='_competitions_json_cache',
            fetch_function=self.api_client.get_events,
        )

    def hard_refresh_manifest(self):
        manifest = self.inventory_page_one()
        manifest = self.inventory_multiple_pages(manifest)
        blob = self.bucket.blob('crossfit/manifest.json')
        blob.upload_from_string(json.dumps(manifest))
        setattr(self, '_manifest', manifest)
        return manifest

    @property
    def manifest(self):
        if hasattr(self, '_manifest'):
            return getattr(self, '_manifest')   

        return self.hard_refresh_manifest()

    def soft_refresh_manifest(self):
        manifest = self.manifest
        missing_comps = [c for c in self.competitions_json if c['id'] not in manifest]
        additions = self.inventory_page_one(missing_comps)
        manifest = {**manifest, **additions}
        manifest = self.inventory_multiple_pages(manifest)
        blob = self.bucket.blob('crossfit/manifest.json')
        blob.upload_from_string(json.dumps(manifest))
        setattr(self, '_manifest', manifest)
        return manifest

    @property
    def elite_competitions(self):
        if hasattr(self, '_elite_competition_cache'):
            return self._elite_competition_cache

        comps_dict = {c['id']: c for c in self.all_competitions_json}

        relations = {}
        for comp in comps_dict.values():
            if comp['parent_competition_id'] is not None:
                relations.setdefault(comp['parent_competition_id'], []).append(comp['id'])

        parents = set(relations.keys())
        children = set(c for cs in relations.values() for c in cs)

        elite_comp_types = [
            'open','games','regional','semifinal',
            'quarterfinalsindividual'
        ]
        slug_blacklist = [
            'all','age','adaptive','team'
        ]

        elite_comps = {}
        for parent, children in relations.items():
            for child in children:
                child_type = comps_dict[child]['type']
                if child_type not in elite_comp_types:
                    continue
                child_slug = comps_dict[child]['slug']
                if any(s in child_slug for s in slug_blacklist):
                    continue
                parent_type = comps_dict[parent]['type']
                params = {**comps_dict[child], 'parent_type': parent_type}
                elite_comps[child] = CFCompetition(**params)

        for comp in comps_dict.values():
            if comp['type'] not in elite_comp_types:
                continue
            if comp['id'] in children or comp['id'] in parents:
                continue
            if any(s in comp['slug'] for s in slug_blacklist):
                continue
            params = {**comp, 'parent_type': None}
            elite_comps[comp['id']] = CFCompetition(**params)

        self._elite_competition_cache = elite_comps
        return elite_comps

    @staticmethod
    def get_page_count(blob):
        data = blob.download_as_bytes(start=64, end=128)
        text = data.decode('utf-8')
        m = re.search(r'"?totalPages"?\s*[:=]\s*(\d+)\s*(?:,|})', text)
        if m:
            return int(m.group(1))
        return None
    
    @staticmethod
    def get_storage_path(comp: CFCompetition, division: int, page: int = 1):
        root_prefix = f'crossfit/comp={comp.comp_id}'
        if comp.type == 'open':
            scaled_str = '/scaled=0/'
        else:
            scaled_str = '/'
        return f'{root_prefix}/division={division}{scaled_str}page={page}.json'

    def inventory_page_one(self, competitions: list[CFCompetition] | None = None):
        manifest = {}
        print('Inventorying first pages of competition leaderboards...')
        if competitions is None:
            competitions = list(self.elite_competitions.values())
        for comp in tqdm(competitions, desc='Competitions'):
            for division in [1,2]:
                manifest.setdefault(comp.comp_id, {}).setdefault(division, {})['status'] = comp.status
                if comp.status != 'completed':
                    continue
                try:
                    path = self.get_storage_path(comp, division, page=1)
                    blob = self.bucket.blob(path)
                    if blob.exists():
                        n_pages = self.get_page_count(blob)
                    else:
                        page_one = self.api_client.get_leaderboard_page(
                            path=comp.api_url_path,
                            division=division,
                            params=comp.api_url_params,
                            page=1
                        )
                        assert page_one['competition']['competitionId'] == comp.comp_id
                        assert page_one['competition']['division'] == division
                        assert page_one['competition']['scaled'] == 0
                        n_pages = int(page_one['pagination']['totalPages'])
                        blob.upload_from_string(json.dumps(page_one))
                    manifest[comp.comp_id][division]['path_to_page_one'] = path
                    manifest[comp.comp_id][division]['n_pages'] = n_pages
                    if n_pages == 1:
                        manifest[comp.comp_id][division]['all_pages'] = True
                except Exception as e:
                    print(f'Error inventorying {comp.comp_id} {division}: {e}')
        return manifest

    def inventory_multiple_pages(self, manifest: dict):
        print('Inventorying multiple pages of competition leaderboards...')
        for c, d in tqdm(manifest.items(), desc='Competitions'):
            for d, data in d.items():
                if data['status'] != 'completed':
                    continue
                if data.get('all_pages', False):
                    continue
                n_pages = data['n_pages']
                path_to_page_one = data['path_to_page_one']
                root_prefix = '/'.join(path_to_page_one.split('/')[:-1])
                expected_files = [
                    f'{root_prefix}/page={p}.json'
                    for p in range(1, n_pages + 1)
                ]
                blob_list = self.bucket.list_blobs(match_glob=root_prefix+'/page=*.json')
                existing_files = [b.name for b in blob_list]
                missing_files = list(set(expected_files) - set(existing_files))
                
                for mf in missing_files:
                    try:
                        page = int(mf.split('/')[-1].split('=')[-1])
                        comp = self.elite_competitions[c]
                        page_data = self.api_client.get_leaderboard_page(
                            path=comp.api_url_path,
                            division=d,
                            params=comp.api_url_params,
                            page=page
                        )
                        blob = self.bucket.blob(mf)
                        blob.upload_from_string(json.dumps(page_data))
                    except Exception as e:
                        manifest[c][d].setdefault('page_errors', []).append(page)

                manifest[c][d]['all_pages'] = len(manifest[c][d].get('page_errors', [])) == 0

        return manifest

def parse_cf_leaderboard_page(data: dict):
    comp = data['competition']
    page = data['pagination']

    entrants = []
    scores = []
    for row in data['leaderboardRows']:
        overall = {k:v for k,v in row.items() if 'overall' in k}
        entrant = row['entrant']
        entrant_params = {**comp,**overall,**page,**entrant}
        entrant_model = CFEntrant(**entrant_params)
        entrants.append(entrant_model)

        for score in row['scores']:
            score_params = {**comp,**entrant,**score}
            score_model = CFScore(**score_params)
            scores.append(score_model)

    return entrants, scores
        