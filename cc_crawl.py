import pandas as pd
from api import CompetitionCornerAPI
import os
import json
from pathlib import Path
from dotenv import load_dotenv
from tqdm import tqdm
load_dotenv()

def download_all_events(refresh=False):
    root_data_path = os.getenv('ELITE_COMPETITION_DATA_PATH')
    events_path = os.path.join(root_data_path,'competition-corner','events_all.jsonl')
    if os.path.exists(events_path) and not refresh:
        events_df = pd.read_json(events_path, orient="records", lines=True)
    else:
        cc = CompetitionCornerAPI()
        events = cc.get_events()
        events_df = pd.DataFrame(events)
        events_df.to_json(events_path, orient="records", lines=True)
    return events_df

def download_event(event_id: int, refresh=False):
    cc = CompetitionCornerAPI()
    root_path = os.getenv('ELITE_COMPETITION_DATA_PATH')

    root_path = Path(root_path) / 'competition-corner'
    divisions_path = root_path / 'divisions'

    divisions_path.mkdir(parents=True, exist_ok=True)
    division_file = divisions_path / f'{event_id}.json'
    if not division_file.exists() or refresh:
        divs = cc.get_event_divisions(event_id)
        divs_df = pd.DataFrame(divs).T
        divs_df.index.name = 'division_key'
        divs_df['event_id'] = event_id
        divs_df = divs_df.reset_index()\
            .set_index(['event_id','division_key'])\
                .reset_index()
        divs_df.to_json(division_file, orient="records", lines=True)
    else:
        divs_df = pd.read_json(division_file, orient="records", lines=True)

    lb_path = root_path / 'leaderboard'
    lb_path.mkdir(parents=True, exist_ok=True)
    for division_key in divs_df['division_key']:
        lb_file = lb_path / f'{event_id}_{division_key}.json'
        if not lb_file.exists() or refresh:
            lb = cc.get_event_leaderboard(event_id, division_key)
            lb_df = pd.DataFrame(lb)
            lb_df['event_id'] = event_id
            lb_df['division_key'] = division_key
            lb_df = lb_df.set_index(['event_id','division_key']).reset_index()
            lb_df.to_json(lb_file, orient="records", lines=True)
        else:
            lb_df = pd.read_json(lb_file, orient="records", lines=True)

        participant_path = root_path / 'participant'
        participant_path.mkdir(parents=True, exist_ok=True)

        for roster_division_key, roster_id in zip(divs_df['division_key'], divs_df['rosterID']):
            participant_file = participant_path / f'{event_id}_{roster_division_key}_{roster_id}.json'
            if not participant_file.exists() or refresh:
                participant = cc.get_participant(roster_division_key, roster_id)
                participant_df = pd.DataFrame(participant)
                participant_df.to_json(participant_file, orient="records", lines=True)


def _read_target_events(events_file: str):
    events_df = pd.read_json(events_file, orient="records", lines=True)
    if "event_id" not in events_df.columns:
        raise ValueError(f"Missing 'event_id' column in {events_file}")
    return sorted(events_df["event_id"].dropna().astype(int).unique().tolist())


def _extract_public_profile_alias(participant_payload: dict):
    data = participant_payload.get("data", {})
    if not isinstance(data, dict):
        return None
    alias = data.get("publicProfileAlias")
    if isinstance(alias, str) and alias.strip():
        return alias.strip()
    return None


def download_target_events(
    target_event_ids: list[int],
    refresh: bool = False,
    request_delay_seconds: float = 0.5,
):
    root_path = os.getenv("ELITE_COMPETITION_DATA_PATH")
    if not root_path:
        raise ValueError("ELITE_COMPETITION_DATA_PATH is not set")

    root_path = Path(root_path) / "competition-corner"
    root_path.mkdir(parents=True, exist_ok=True)

    cc = CompetitionCornerAPI(request_delay_seconds=request_delay_seconds)

    events_path = root_path / "event"
    divisions_path = root_path / "divisions"
    participants_path = root_path / "participant"
    leaderboard_path = root_path / "leaderboard"
    workouts_path = root_path / "workouts"
    profiles_path = root_path / "profile"

    for path in [events_path, divisions_path, participants_path, leaderboard_path, workouts_path, profiles_path]:
        path.mkdir(parents=True, exist_ok=True)

    profile_aliases = set()

    event_iterator = tqdm(target_event_ids, desc="Events", unit="event")
    for event_id in event_iterator:
        event_iterator.set_postfix_str(f"event_id={event_id}")
        event_file = events_path / f"{event_id}.json"
        if not event_file.exists() or refresh:
            event_payload = cc.get_event(event_id)
            with open(event_file, "w", encoding="utf-8") as f:
                f.write(json.dumps(event_payload) + "\n")

        division_file = divisions_path / f"{event_id}.json"
        if not division_file.exists() or refresh:
            divisions = cc.get_event_divisions(event_id)
            divs_df = pd.DataFrame(divisions).T
            divs_df.index.name = "division_key"
            divs_df["event_id"] = event_id
            divs_df = divs_df.reset_index().set_index(["event_id", "division_key"]).reset_index()
            divs_df.to_json(division_file, orient="records", lines=True)
        else:
            divs_df = pd.read_json(division_file, orient="records", lines=True)

        division_pairs = list(divs_df["division_key"])
        division_iterator = tqdm(
            division_pairs,
            desc=f"Divisions for {event_id}",
            unit="division",
            leave=False,
        )
        for division_key in division_iterator:
            division_iterator.set_postfix_str(f"{division_key}")
            leaderboard_file = leaderboard_path / f"{event_id}_{division_key}.json"
            if not leaderboard_file.exists() or refresh:
                leaderboard_rows = cc.get_event_leaderboard(event_id, division_key)
                with open(leaderboard_file, "w", encoding="utf-8") as f:
                    for row in leaderboard_rows:
                        f.write(json.dumps(row) + "\n")
            else:
                leaderboard_rows = pd.read_json(leaderboard_file, orient="records", lines=True).to_dict("records")

            roster_ids = []
            for row in leaderboard_rows:
                roster_id = row.get("rosterID") or row.get("rosterId")
                if roster_id is not None:
                    roster_ids.append(int(roster_id))

            for roster_id in tqdm(
                sorted(set(roster_ids)),
                desc=f"Participants {division_key}",
                unit="participant",
                leave=False,
            ):
                participant_file = participants_path / f"{event_id}_{division_key}_{roster_id}.json"
                if not participant_file.exists() or refresh:
                    participant_payload = cc.get_participant(division_key, roster_id)
                    with open(participant_file, "w", encoding="utf-8") as f:
                        f.write(json.dumps(participant_payload) + "\n")
                else:
                    with open(participant_file, "r", encoding="utf-8") as f:
                        participant_payload = json.loads(f.readline())

                profile_alias = _extract_public_profile_alias(participant_payload)
                if profile_alias:
                    profile_aliases.add(profile_alias)

            workouts_file = workouts_path / f"{event_id}_{division_key}.json"
            if not workouts_file.exists() or refresh:
                workouts_payload = cc.get_event_workouts(event_id, division_key)
                with open(workouts_file, "w", encoding="utf-8") as f:
                    for workout in workouts_payload:
                        f.write(json.dumps(workout) + "\n")

    for profile_key in tqdm(sorted(profile_aliases), desc="Athlete pages", unit="profile"):
        profile_file = profiles_path / f"{profile_key}.json"
        if not profile_file.exists() or refresh:
            profile_payload = cc.get_athlete_page(profile_key)
            with open(profile_file, "w", encoding="utf-8") as f:
                f.write(json.dumps(profile_payload) + "\n")

if __name__ == "__main__":
    events_to_crawl = pd.read_json('events_to_crawl.jsonl', orient='records', lines=True)
    target_event_ids = events_to_crawl['eventId'].tolist()
    download_target_events(target_event_ids, refresh=False)
