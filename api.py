import requests

class CompetitionCornerAPI:
    def __init__(self):
        self.base_url = "https://competitioncorner.net/api2/v1"

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

        url = self.base_url + "/events/filtered"

        events_all = []
        while True:
            r = requests.get(url, params=params)
            events = r.json()
            if len(events) == 0:
                break
            events_all.extend(events)
            params['page'] += 1

        return events_all

    def get_event(self,event_id: int):
        url = self.base_url + f"/events/{event_id}"
        r = requests.get(url)
        return r.json()

    def get_event_divisions(self, event_id: int):
        url = self.base_url + f'/leaderboard/{event_id}'
        r = requests.get(url)
        return r.json()

    def get_event_leaderboard(
        self, 
        event_id: int, 
        division_key: str,
        per_page: int = 50
    ):
        url = self.base_url + f'/leaderboard/{event_id}/tab/{division_key}'
        params = {
            'start': 0,
            'end': per_page,
            'athletesOnly': True
        }
        all_rows = []
        while True:
            r = requests.get(url,params=params)
            rows = r.json()['athletes']
            if len(rows) == 0:
                break
            all_rows.extend(rows)
            params['start'] = params['end'] + 1
            params['end'] += per_page
        return all_rows

    def get_participant(
        self,
        division_key: str,
        roster_id: int
    ):
        url = self.base_url + f'/leaderboard/{division_key}/participantdata'
        params = {
            'preview': False,
            'rosterId': roster_id
        }
        r = requests.get(url,params=params)
        return r.json()
