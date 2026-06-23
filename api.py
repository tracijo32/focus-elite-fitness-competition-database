import time
import requests
import re
from bs4 import BeautifulSoup

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
        headers: dict | None = None,
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
                    headers=headers,
                    timeout=self.timeout_seconds,
                )
            else:
                response = requests.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout_seconds,
                )
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

    def fetch_metadata(self, **kwargs):
        return

    def fetch_divisions(self, **kwargs):
        return

    def fetch_leaderboard_page(self, **kwargs):
        return

    def fetch_workouts(self, **kwargs):
        return

    def fetch_profile(self, **kwargs):
        return

class CrossFitAPIRequestClient(APIRequestClient):
    def __init__(self):
        super().__init__(base_url="https://c3po.crossfit.com/api")

    def fetch_competitions(self):
        return self._request_json("/competitions/v1/competitions")

    def fetch_leaderboard_page(
        self, 
        comp_id: int,
        comp_type: str,
        year: int,
        div_id: int,
        page: int = 1,
        **kwargs
    ):
        params = {
            'division': div_id
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
    def __init__(self):
        super().__init__(base_url="https://competitioncorner.net/api2/v1")

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

    def fetch_metadata(self, comp_id: int, **kwargs):
        return self._request_json(f"/events/{comp_id}")

    def fetch_divisions(self, comp_id: int, **kwargs):
        return self._request_json(f"/leaderboard/{comp_id}")

    def fetch_workouts(self, comp_id: int, div_id: int | str, **kwargs):
        path = f'/lookups/{comp_id}/workouts'
        if isinstance(div_id, str):
            div_id = re.search(r'(\d+)', div_id).group(1)
        params = {
            'divisionId': div_id,
            'excludeCalculatedWorkouts': True,
            'preview': False
        }
        return self._request_json(path, params=params)

    def fetch_workout_description(self, comp_id: int, workout_id: int, **kwargs):
        path = f'/events/{comp_id}/workouts/{workout_id}/public'
        params = {
            'preview': False
        }
        return self._request_json(path, params=params)

    def fetch_workout_schedule(self, workout_id: int, div_id: int | str, **kwargs):
        if isinstance(div_id, str):
            div_id = re.search(r'(\d+)', div_id).group(1)
        path = f'/schedule/workout/{workout_id}'
        params = {
            'divisionId': div_id
        }
        try:
            return self._request_json(path, params=params)
        except:
            return []

    def fetch_leaderboard_page(
        self, 
        comp_id: int, 
        div_id: str, 
        page: int = 1, 
        **kwargs):

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

    def fetch_athlete(self, profile_id: int, **kwargs):
        return self._request_json(f"/athletes/{profile_id}")

    def fetch_participant(self, div_id: str, roster_id: int, **kwargs):
        params = {
            'preview': False,
            'rosterId': roster_id
        }
        return self._request_json(f"/leaderboard/{div_id}/participantdata", params=params)

class StrongestAPIRequestClient(APIRequestClient):
    def __init__(self):
        base_url = "https://compete-strongest-com.global.ssl.fastly.net/api/p"
        super().__init__(base_url)

    def fetch_metadata(self, comp_id: str, **kwargs):
        return self._request_json(f"/competitions/{comp_id}/")

    def fetch_divisions(self, comp_id: str, **kwargs):
        return self._request_json(f"/competitions/{comp_id}/divisions/")

    def fetch_workouts(self, comp_id: str, **kwargs):
            return self._request_json(f"/competitions/{comp_id}/workouts/")

    def fetch_leaderboard_page(self, div_id: str, page: int = 1, **kwargs):
        return self._request_json(
            f"/divisions/{div_id}/leaderboard/",
            params={'p': page}) 

    def fetch_profile(self, profile_id: int, **kwargs):
        return self._request_json(f"/athletes/{profile_id}")

    def fetch_scoring_policies(self, comp_id: str, **kwargs):
        return self._request_json(f"/competitions/{comp_id}/scoring-policies/")

    def fetch_event_configs(self, comp_id: str, **kwargs):
        return self._request_json(f"/competitions/{comp_id}/event-configs/")

class ScoreItAPIRequestClient(APIRequestClient):
    def __init__(self, *args, **kwargs):
        super().__init__(base_url="https://scoreit.co.za")

    def fetch_competitions(self):
        passed = self._request_json('/events/passedEvents')
        upcoming = self._request_json('/events/upcomingEvents')
        return passed + upcoming

    def fetch_metadata(self, comp_id: str, **kwargs):
        return self._request_json(f'/events/upcomingEvent/{comp_id}')

    def fetch_leaderboard_page(
        self,
        comp_id: str,
        div_id: str,
        **kwargs
    ):
        path = f'/EventTeamScoring/leaderboard/{comp_id}'
        params = {'divisionRef': div_id}
        return self._request_json(path, params=params)

class WodcastAPIRequestClient(APIRequestClient):
    def __init__(self):
        super().__init__(
            base_url='https://www.wodcast.com'
        )

    def fetch_workout_results_page(
        self,
        comp_id: int,
        div_id: str,
        seq: int,
        page: int = 1,
        page_size: int = 200,
    ):
        data = {
            'eventID': comp_id,
            'gender': div_id,
            'eventNumber': seq,
            'pageNumber': page,
            'pageSize': page_size,
        }
        params = {'format': 'json'}

        path = '/services/GetAffiliateEventResultsService.php'
        return self._request_json(path,method="POST",params=params,data=data)

    def fetch_overall_results_page(
        self,
        comp_id: int,
        div_id: str,
        page: int = 1,
        page_size: int = 200
    ):
        data = {
            'eventID': comp_id,
            'gender': div_id,
            'pageNumber': page,
            'pageSize': page_size
        }
        params = {'format': 'json'}

        path = '/services/GetAffiliateEventOverallResultsService.php'
        return self._request_json(path,method="POST",params=params,data=data)

    def fetch_leaderboard_page(
        self,
        page: int = 1, 
        page_size: int = 200,
        **kwargs
    ):

        data = {}
        res = self.fetch_overall_results_page(
            comp_id=kwargs['comp_id'],
            div_id=kwargs['div_id'],
            page=page,
            page_size=page_size
        )
        data['overall'] = res['athletes']
        data['currentPage'] = res['currentPage']
        data['totalPages'] = res['totalPages']
        
        n = 1
        while True:
            try:
                wod_res = self.fetch_workout_results_page(
                    comp_id=kwargs['comp_id'],
                    div_id=kwargs['div_id'],
                    seq=n,
                    page=page,
                    page_size=page_size
                )
            except Exception as e:
                break
            data[f'event_{n}'] = wod_res['athletes']
            n += 1
        
        return data

class LocalCompAPIRequestClient(APIRequestClient):
    def __init__(self):
        super().__init__(
            base_url="https://local-comp.com"
        )

    def fetch_competitions(self):
        html =  self._request_json(
            "/controller/event/view-all"
        )
        soup = BeautifulSoup(html, 'html.parser')
        event_spans = soup.find_all('span',attrs={'class': 'eventDetailLink'})
        events = []
        for span in event_spans:
            event_id = span.attrs.get('eventid')
            event_name = span.text.strip()
            details = soup.find('div',
                attrs={'id': f'eventDetailArea-{event_id}'}).text\
                    .replace('\t','').split('\n')
            details = [d for d in details if len(d) > 0]
            events.append({
                'id': event_id,
                'name': event_name,
                'date': details[1]
                })
        return events

    def fetch_divisions(
        self, comp_id: int
    ):
        html = self._request_json(
            "/controller/event/leaderboard",
            params = {
                'eventId': comp_id
            }
        )
        soup = BeautifulSoup(html, 'html.parser')
        dropdown = soup.find('td',attrs={'id': 'eventDivisionDropdownCell'})
        options = dropdown.find_all('option')
        divisions = []
        for option in options:
            txt = option.text.strip().replace('\t','').split('\n')
            txt = " ".join([t for t in txt if len(t) > 0])
            value = option.attrs.get('value')
            divisions.append({'division_id': value, 'division_name': txt})
        return divisions

    @staticmethod
    def get_teams(soup: BeautifulSoup):
        name_divs = soup.find_all('div',attrs={'class': 'teamName'})
        entrants = []
        for div in name_divs:
            team_name = div.text.strip().title()
            team_id = div.attrs.get('teamid')
            r1 = div.find_parent().find_previous_sibling()
            r2 = r1.find_previous_sibling()
            if r2 is None:
                rank = r1.text.strip()
            else:
                rank = r2.text.strip()
            g = div.find_next_sibling()
            if g is None:
                gym = None
            else:
                gym = g.text.strip()
            entrants.append({
                'name': team_name,
                'id': team_id,
                'rank': rank,
                'gym': gym
            })
        return entrants

    @staticmethod
    def get_workouts(soup: BeautifulSoup):
        workout_cells = soup.find_all('td',attrs={'class': 'resultDetailHeader'})
        workouts = []
        for cell in workout_cells:
            workout_name = cell.attrs.get('title')
            enum = cell.text.strip()
            workouts.append({
                'name': workout_name,
                'enum': enum
            })
        return workouts

    @staticmethod
    def get_points(soup: BeautifulSoup):
        points_cells = soup.find_all('td',attrs={'class': 'pointsCell'})
        points = []
        for cell in points_cells:
            points.append(cell.text.strip())
        return points

    @staticmethod
    def get_results(soup: BeautifulSoup):
        result_cells = soup.find_all('td',attrs={'class': 'resultDetailCell'})
        results = []
        for cell in result_cells:
            values = [v.text.strip() for v in cell.find_all('div')]
            results.append(values)
        return results
    
    def fetch_leaderboard_page(
        self, comp_id: int, div_id: int, 
        return_soup: bool = False,
        **kwargs
    ):
        html = self._request_json(
            "/controller/event/ajax/leaderboard-detail",
            params = {
                'eventDivisionId': div_id,
                'eventId': comp_id
            }
        )
        soup = BeautifulSoup(html, 'html.parser')
        if return_soup:
            return soup
        teams = self.get_teams(soup)
        workouts = self.get_workouts(soup)
        results = self.get_results(soup)
        points = self.get_points(soup)

        entrants = [
            {**t, 'points': p}
            for t, p in zip(teams, points)
        ]
        scores = [
            {
                'id': entrants[i // len(workouts)]['id'],
                'enum': workouts[i % len(workouts)]['enum'],
                'results': results[i]
            }
            for i in range(len(results))
        ]
        output = {
            'comp_id': comp_id,
            'division_id': div_id,
            'entrants': entrants,
            'workouts': workouts,
            'scores': scores
        }
        return output

class RogueFitnessAPIRequestClient(APIRequestClient):
    def __init__(self):
        super().__init__(
            base_url='https://www.roguefitness.com'
        )
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.roguefitness.com/invitational/leaderboard",
            "Origin": "https://www.roguefitness.com",
        }
    
    def fetch_competitions(self):
        url = '/svc_event/events'
        return self._request_json(url, headers=self.headers)
    
    def fetch_metadata(self, comp_id: str):
        url = f'/svc_event/events/{comp_id}'
        return self._request_json(url, headers=self.headers)
    
    def fetch_leaderboard_page(self, comp_id: str, div_id: str, year: int | None = None, page: int = 1):
        if year is None:
            url = f'/svc_event/events/{comp_id}/divisions/{div_id}/leaderboard'
        else:
            url = f'/svc_event/events/{year}/{comp_id}/divisions/{div_id}/leaderboard'
        params = {
            'page': page,
            'sortEventNumber': 1,
            'sortType': 'over_rank',
            'sortOrder': 'asc'
        }
        return self._request_json(url, headers=self.headers, params=params)

class Circle21APIRequestClient(APIRequestClient):
    def __init__(self, base_url='https://api.circle21.events/api'):
        super().__init__(base_url=base_url)

    def _paginate(
        self, path: str, 
        params: dict = {}, 
        per_page: int = 100
    ):
        data = []
        params['page'] = 1
        params['per_page'] = per_page
        while True:
            r = self._request_json(path, params)
            data.extend(r['data'])
            if r['next_page_url'] is None:
                break
            params['page'] += 1
        return data

    def fetch_competitions(self, per_page: int = 100):
        return self._paginate('/competition', per_page=per_page)

    def fetch_metadata_by_slug(self, slug: str):
        path = f'/competition/slug/{slug}'
        return self._request_json(path)

    def fetch_metadata(self, comp_id: str):
        path = f'/competition/{comp_id}'
        return self._request_json(path)

    def fetch_workouts(self, comp_id: str, per_page: int = 100):
        path = '/wods'
        params = {'competition_id': comp_id}
        return self._paginate(path, params=params, per_page=per_page)

    def fetch_leaderboard_page(self, comp_id: str, div_id: str, page: int = 1):
        path = f'/leaderboard'
        params = {'competition_id': comp_id, 'division_id': div_id}
        return self._request_json(path, params=params)

class CaptureFitAPIRequestClient(APIRequestClient):
    def __init__(self):
        super().__init__(base_url='https://capturefit.com')

    @staticmethod
    def get_events_titles_and_dates(soup: BeautifulSoup):
        events = soup.find_all(
            'div',
            attrs={'class':'event'}
        )
        
        data = []
        for event in events:
            tag = event.find(
                'a', href=True,
                attrs={'class':'title'}
                )
            link = tag['href']
            title = tag.text.strip()
            dates = tag.next_sibling.next_sibling.text.strip()
            year = int(event['data-year'])
            data.append(
                {
                    'event_link': link,
                    'title': title,
                    'dates': dates,
                    'year': year
                }
            )
        return data

    def fetch_competitions(self,return_soup=False):
        path = '/past-events'
        html = self._request_json(path)
        soup = BeautifulSoup(html, 'html.parser')
        if return_soup:
            return soup
        data = self.get_events_titles_and_dates(soup)
        new_data = [
            {
                'slug': d['event_link'].split('/')[-1],
                'comp_id': self.fetch_comp_id_from_slug(d['event_link'].split('/')[-1]),
                **d
            } for d in data
        ]
        return new_data

    def fetch_comp_id_from_slug(self, slug: str):
        import re
        path = f'/leaderboard/{slug}'
        html = self._request_json(path)
        soup = BeautifulSoup(html, 'html.parser')
        scripts = soup.find_all('script')
        for script in scripts:
            if 'staticleaderboard' in script.text:
                match = re.search(r"""var id\s*=\s*['"]([^'"]+)['"]""", script.text)
                return match.group(1)
        
    def fetch_divisions(self, comp_id: str):
        path = f'/api/event-workouts/divisions/{comp_id}'        
        return self._request_json(path)

    def fetch_leaderboard_page(
        self,
        comp_id: str,
        entrytype: str,
        category: str,
        gender: str,
        **kwargs
    ):
        path = "/api/leaderboards/get-calulated-leaderboard"
        payload = {
            "event": comp_id,
            "entrytype": entrytype, 
            "category": category,
            "gender": gender,
        }
        return self._request_json(path,method="POST",data=payload)

class BTWBWireAPIRequestClient(APIRequestClient):
    def __init__(self):
        super().__init__(
            base_url = 'https://us-central1-btwb-thewire.cloudfunctions.net'
        )   
    def fetch_config(
        self,
        div_id: int,
        **kwargs
    ):
        path = '/ProdLeaderboards'
        params = {
            'division_id': div_id
        }
        return self._request_json(path,params=params)

    def fetch_leaderboard_page(
        self,
        leaderboard_id: int,
        page: int = 1,
        **kwargs
    ):
        path = f'/ProdLeaderboardPages'
        params = {
            'leaderboard_id': leaderboard_id,
            'page': page-1
        }
        return self._request_json(path,params=params)

class BTWBRogueAPIRequestClient(APIRequestClient):
    def __init__(self):
        super().__init__(
            base_url="https://rogue.btwb.com"
        )

    def fetch_leaderboard_page(
        self,
        comp_id: str,
        div_id: int,
        **kwargs
    ):
        path = f'/data/{comp_id}/standings-200-{div_id}.json'
        return self._request_json(path)

class BTWBRogueInventoryManager(InventoryManager):
    def __init__(self, api_data_path: str = 'api'):
        super().__init__(
            api_client = BTWBRogueAPIRequestClient(),
            source='btwb-rogue',
            api_data_path=api_data_path
        )