import time, requests
import requests, time

class APIRequestClient:
    def __init__(
        self,
        base_url: str,
        request_delay_seconds: float = 0.5,
        max_retries: int = 5,
        backoff_base_seconds: float = 1.0,
        timeout_seconds: int = 30,
    ):
        self.base_url = base_url
        self.request_delay_seconds = request_delay_seconds
        self.max_retries = max_retries
        self.backoff_base_seconds = backoff_base_seconds
        self.timeout_seconds = timeout_seconds
        self._last_request_ts = 0.0

    def _is_rate_limited_response(self, response: requests.Response, payload) -> bool:
        if response.status_code == 429:
            return True
        if isinstance(payload, dict):
            text = " ".join(
                str(v).lower() for v in payload.values() if isinstance(v, (str, int, float))
            )
            return any(
                term in text
                for term in ["too frequent", "rate limit", "too many requests", "slow down"]
            )
        if isinstance(payload, str):
            lower_payload = payload.lower()
            return any(
                term in lower_payload
                for term in ["too frequent", "rate limit", "too many requests", "slow down"]
            )
        return False

    def _request_json(self, endpoint: str, params: dict | None = None):
        url = self.base_url + endpoint
        attempt = 0
        while True:
            now = time.time()
            elapsed = now - self._last_request_ts
            if elapsed < self.request_delay_seconds:
                time.sleep(self.request_delay_seconds - elapsed)

            response = requests.get(url, params=params, timeout=self.timeout_seconds)
            self._last_request_ts = time.time()

            try:
                payload = response.json()
            except requests.exceptions.JSONDecodeError:
                payload = response.text

            rate_limited = self._is_rate_limited_response(response, payload)
            if rate_limited and attempt < self.max_retries:
                backoff = self.backoff_base_seconds * (2**attempt)
                time.sleep(backoff)
                attempt += 1
                continue

            response.raise_for_status()
            return payload

class CrossFitAPIRequestClient(APIRequestClient):
    def __init__(self):
        super().__init__(base_url="https://c3po.crossfit.com/api")

    def get_events(self):
        return self._request_json("/competitions/v1/competitions")

    def get_leaderboard_page(
        self, 
        path: str,
        division: int,
        params: dict = {},
        page: int | None = None
    ):
        if not url_path.startswith('/'):
            url_path = '/' + url_path
        params['division'] = division
        if page is not None:
            params['page'] = page
        return self._request_json(url_path, params=params)

class CompetitionCornerAPIRequestClient(APIRequestClient):
    def __init__(self, *args, **kwargs):
        super().__init__(base_url="https://competitioncorner.net/api2/v1", *args, **kwargs)

    def get_events(
        self,
        per_page: int = 100, 
        timestamp: int | None = None
    ):
        params = {
            'page': 1,
            'perPage': per_page,
        }
        if timestamp:
            params['timestamp'] = timestamp

        events_all = []
        while True:
            events = self._request_json("/events/filtered", params=params)
            if len(events) == 0:
                break
            events_all.extend(events)
            params['page'] += 1

        return events_all

    def get_event(self,event_id: int):
        return self._request_json(f"/events/{event_id}")

    def get_event_divisions(self, event_id: int):
        return self._request_json(f"/leaderboard/{event_id}")

    def get_event_leaderboard(
        self, 
        event_id: int, 
        division_key: str,
        per_page: int = 50
    ):
        params = {
            'start': 0,
            'end': per_page,
            'athletesOnly': True
        }
        all_rows = []
        while True:
            payload = self._request_json(f"/leaderboard/{event_id}/tab/{division_key}", params=params)
            rows = payload.get('athletes', [])
            if len(rows) == 0:
                break
            all_rows.extend(rows)
            params['start'] = params['end'] + 1
            params['end'] += per_page
        return all_rows

    def get_event_workouts(
        self, 
        event_id: int, 
        division_key: str
    ):
        params = {
            'hasAthletes': False
        }
        payload = self._request_json(f"/leaderboard/{event_id}/tab/{division_key}", params=params)
        return payload.get('workouts', [])

    def get_participant(
        self,
        division_key: str,
        roster_id: int
    ):
        params = {
            'preview': False,
            'rosterId': roster_id
        }
        return self._request_json(f"/leaderboard/{division_key}/participantdata", params=params)

    def get_workout(self, event_id: int, workout_id: int):
        params = {
            'preview': False
        }
        return self._request_json(f"/events/{event_id}/workouts/{workout_id}/public", params=params)

    def get_athlete_page(self, profile_key: str):
        return self._request_json(f"/accounts/athletepage/{profile_key}")

class StrongestAPIRequestClient(APIRequestClient):
    def __init__(self):
        base_url = "https://compete-strongest-com.global.ssl.fastly.net/api/p"
        super().__init__(base_url)

    def _request_json_data_only(self, url: str, *args, **kwargs):
        return self._request_json(url, *args, **kwargs)['data']

    def get_competition(self, competition_key: str):
        return self._request_json_data_only(f"/competitions/{competition_key}/")

    def get_divisions(self, competition_key: str):
        return self._request_json_data_only(f"/competitions/{competition_key}/divisions/")

    def get_workouts(self, competition_key: str):
            return self._request_json_data_only(f"/competitions/{competition_key}/workouts/")

    def get_leaderboard_page(self, division_key: str, page: int = 1):
        return self._request_json_data_only(
            f"/divisions/{division_key}/leaderboard/",
            params={'p': page}) 

    def get_athlete_profile(self, profile_key: str):
        return self._request_json_data_only(f"/athletes/{profile_key}/")