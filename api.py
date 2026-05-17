import time
import requests

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

    def _request_json(
        self,
        endpoint: str,
        params: dict | None = None,
        *,
        method: str = "GET",
        data: dict | None = None,
    ):
        url = self.base_url + endpoint
        m = method.upper()
        if m not in ("GET", "POST"):
            raise ValueError(f"method must be 'GET' or 'POST', got {method!r}")
        if m == "GET" and data is not None:
            raise ValueError("data= is only supported with method='POST'")
        attempt = 0
        while True:
            now = time.time()
            elapsed = now - self._last_request_ts
            if elapsed < self.request_delay_seconds:
                time.sleep(self.request_delay_seconds - elapsed)

            if m == "POST":
                response = requests.post(
                    url,
                    params=params,
                    data=data,
                    timeout=self.timeout_seconds,
                )
            else:
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

    def fetch_metadata(self, comp_id):
        return

    def fetch_divisions(self, comp_id):
        return

    def fetch_leaderboard_page(self, comp_id, div_id, page: int = 1):
        return

    def fetch_workouts(self, comp_id, div_id):
        return

    def fetch_profile(self, profile_id):
        return

class CrossFitAPIRequestClient(APIRequestClient):
    def __init__(self):
        super().__init__(base_url="https://c3po.crossfit.com/api")

    def fetch_competitions(self):
        return self._request_json("/competitions/v1/competitions")

    def fetch_leaderboard_page(
        self, 
        comp_id,
        comp_type,
        year,
        division,
        page: int = 1
    ):
        params = {
            'division': division
        }
        if 'regional' in comp_type:
            cpath = 'regionals'
            params['regional'] = comp_id
        elif 'sanctional' in comp_type:
            cpath = 'sanctionals'
            params['sanctional'] = comp_id
        elif 'semifinal' in comp_type:
            cpath = 'semifinals'
            params['semifinal'] = comp_id
        elif 'open' in comp_type:
            cpath = 'open'
            params['scaled'] = 0
        else:
            cpath = comp_type

        if page > 1:
            params['page'] = page
            
        url_path = '/'.join([
            '',
            'leaderboards',
            'v2',
            'competitions',
            cpath,
            str(year),
            'leaderboards'
        ])
        return self._request_json(url_path, params=params)

class CompetitionCornerAPIRequestClient(APIRequestClient):
    def __init__(self, *args, **kwargs):
        super().__init__(base_url="https://competitioncorner.net/api2/v1", *args, **kwargs)

    def fetch_competitions(self):
        params = {
            'page': 1,
            'perPage': 100,
        }
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
        per_page: int = 500
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

    def fetch_metadata(self, comp_id):
        return self._request_json(f"/events/{comp_id}")

    def fetch_divisions(self, comp_id):
        return self._request_json(f"/leaderboard/{comp_id}")

    def fetch_leaderboard_page(self, comp_id, div_id, page: int = 1):
        n = 500
        params = {
            'start': 0,
            'end': n,
            'athletesOnly': False
        }
        data = self._request_json(
            f"/leaderboard/{comp_id}/tab/{div_id}",
            params=params
        )
        a = data['athletes']
        while len(a) == n:
            params['athletesOnly'] = True
            params['start'] += n
            params['end'] += n
            a = self._request_json(
                f"/leaderboard/{comp_id}/tab/{div_id}",
                params=params
            )['athletes']
            data['athletes'].extend(a)
        return data

    def fetch_athlete(self, profile_id):
        return self._request_json(f"/athletes/{profile_id}")

    def fetch_participant(self, comp_id ,div_id ,roster_id):
        params = {
            'preview': False,
            'rosterId': roster_id
        }
        return self._request_json(f"/leaderboard/{div_id}/participantdata", params=params)

class StrongestAPIRequestClient(APIRequestClient):
    def __init__(self):
        base_url = "https://compete-strongest-com.global.ssl.fastly.net/api/p"
        super().__init__(base_url)

    def fetch_metadata(self, comp_id):
        return self._request_json(f"/competitions/{comp_id}/")

    def fetch_divisions(self, comp_id):
        return self._request_json(f"/competitions/{comp_id}/divisions/")

    def fetch_workouts(self, comp_id):
            return self._request_json(f"/competitions/{comp_id}/workouts/")

    def fetch_leaderboard_page(self, comp_id, div_id, page: int = 1):
        return self._request_json(
            f"/divisions/{div_id}/leaderboard/",
            params={'p': page}) 

    def fetch_profile(self, profile_id: str):
        return self._request_json(f"/athletes/{profile_id}")

    def fetch_scoring_policies(self, comp_id: str):
        return self._request_json(f"/competitions/{comp_id}/scoring-policies/")

class ScoreItAPIRequestClient(APIRequestClient):
    def __init__(self, *args, **kwargs):
        super().__init__(base_url="https://scoreit.co.za", *args, **kwargs)

    def get_events(self):
        passed = self._request_json('/events/passedEvents')
        upcoming = self._request_json('/events/upcomingEvents')
        return passed + upcoming

    def get_event(self, event_ref: str):
        path = f'/events/upcomingEvent/{event_ref}'
        return self._request_json(path)

    def get_event_leaderboard(self, event_ref: str, division_ref: str):
        path = f'/EventTeamScoring/leaderboard/{event_ref}'
        params = {'divisionRef': division_ref}
        return self._request_json(path, params=params)

class WodcastAPIRequestClient(APIRequestClient):
    def __init__(self):
        super().__init__(
            base_url='https://www.wodcast.com/services'
        )

    def get_workout_results_page(
        self, 
        event_id: int , 
        gender: str, 
        event_number: int,  
        page_number: int = 1,
        page_size: int = 200,
    ):
        data = {
            'eventID': event_id,
            'gender': gender,
            'eventNumber': event_number,
            'pageNumber': page_number,
            'pageSize': page_size,
        }
        params = {'format': 'json'}

        path = '/GetAffiliateEventResultsService.php'

        return self._request_json(path, method='POST', params=params, data=data)

    def get_overall_results_page(
        self,
        event_id: int,
        gender: str,
        page_number: int = 1,
        page_size: int = 200
    ):
        data = {
            'eventID': event_id,
            'gender': gender,
            'pageNumber': page_number,
            'pageSize': page_size
        }
        params = {'format': 'json'}

        path = '/GetAffiliateEventOverallResultsService.php'

        return self._request_json(path, method='POST', params=params, data=data)
