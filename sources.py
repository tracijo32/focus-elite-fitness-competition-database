from google.cloud import storage
from api import CrossFitAPIRequestClient
from models import CFCompetition, CFEntrant, CFScore
from util import GoogleCloudParameters
import json
from datetime import datetime, timezone, timedelta

gcp_params = GoogleCloudParameters()

class CrossfitCompetitionManager:
    def __init__(self, cache_refresh_days: int = 1):
        self.storage_client = storage.Client(project=gcp_params.project_id)
        self.api_client = CrossFitAPIRequestClient()
        self.bucket_name = gcp_params.bucket_name
        self.all_competitions_json_cache = None
        self._refresh = cache_refresh_days

    def _get_blob_created(self, blob_name: str):
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(blob_name)
        if not blob.exists():
            return None
        blob = bucket.get_blob(blob_name)
        return blob.time_created

    def _get_blob_last_modified(self, blob_name: str):
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(blob_name)
        if not blob.exists():
            return None
        blob = bucket.get_blob(blob_name)
        return blob.updated

    def _download_json_blob(self, blob_name: str):
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(blob_name)
        return json.loads(blob.download_as_string())

    @property
    def all_competitions_json(self):
        if self.all_competitions_json_cache is not None:
            return self.all_competitions_json_cache
        
        blob_name = 'crossfit/all_competitions.json'
        bucket = self.storage_client.bucket(self.gcs_bucket_name)
        blob = bucket.blob(blob_name)

        if not blob.exists():
            needs_refresh = True
        else:
            last_modified = self._get_blob_last_modified(blob_name)
            if last_modified is None:
                last_modified = self._get_blob_created(blob_name)
            refresh_deadline = datetime.now(timezone.utc) - timedelta(days=self._refresh)
            needs_refresh = last_modified < refresh_deadline

        if needs_refresh:
            self.all_competitions_json_cache = self.api_client.get_events()
            blob.upload_from_string(json.dumps(self.all_competitions_json_cache))
        else:
            self.all_competitions_json_cache = self._download_json_blob(blob_name)

        return self.all_competitions_json_cache

    @property
    def elite_competitions(self):
        comps_dict = {c['id']: c for c in self.all_competitions_json}

        relations = {}
        for comp in comps_dict.values():
            if comp['parent_competition_id'] is not None:
                relations.setdefault(comp['parent_competition_id'], []).append(comp['id'])

        parents = set(relations.keys())
        children = set(c for cs in relations.values() for c in cs)

        elite_comp_types = [
            'open','games','regional','sanctional',
            'quarterfinalsindividual'
        ]

        elite_comps = {}
        for parent, children in relations.items():
            for child in children:
                child_type = comps_dict[child]['type']
                if child_type not in elite_comp_types:
                    continue
                parent_type = comps_dict[parent]['type']
                params = {**comps_dict[child], 'parent_type': parent_type}
                elite_comps[child] = CFCompetition(**params)

        for comp in comps_dict.values():
            if comp['type'] not in elite_comp_types:
                continue
            if comp['id'] in children or comp['id'] in parents:
                continue
            params = {**comp, 'parent_type': None}
            elite_comps[comp['id']] = CFCompetition(**params)

        return elite_comps

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
        