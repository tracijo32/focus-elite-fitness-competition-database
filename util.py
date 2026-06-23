"""Small helpers not tied to API / storage models."""

import re
from models import Entrant, Score

def _parse_colon_duration_to_seconds(s: str) -> float | None:
    """Parse ``HH:MM:SS.ss``, ``MM:SS.ss``, or ``:SS.ss`` into total seconds."""
    s = s.strip()
    if not s:
        return None

    if s.startswith(":") and s.count(":") == 1:
        try:
            return float(s[1:])
        except ValueError:
            return None

    parts = s.split(":")
    try:
        if len(parts) == 3:
            h, mm, sec_s = int(parts[0]), int(parts[1]), float(parts[2])
            return float(h) * 3600.0 + float(mm) * 60.0 + sec_s
        if len(parts) == 2:
            mm, sec_s = int(parts[0]), float(parts[1])
            return float(mm) * 60.0 + sec_s
    except ValueError:
        pass
    return None


def _time_cap_value_to_seconds(value: str | int | float | None) -> float | None:
    """Seconds from a numeric cap, or the same colon duration formats as scores."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return _parse_colon_duration_to_seconds(value)
    return None

def parse_timed_workout_score(
    score: str | int | float | None,
    *,
    time_cap: str | int | float | None = None,
    time_cap_seconds: str | int | float | None = None,
) -> float | None:
    """
    Parse a timed workout display string into total seconds (float).

    Colon formats (after stripping whitespace):
      - ``HH:MM:SS`` or ``HH:MM:SS.ss``
      - ``MM:SS`` or ``MM:SS.ss``
      - ``:SS`` or ``:SS.ss`` (leading colon → seconds only)

    If the string does not match those patterns, it is treated as a cap score
    like ``CAP+20``: returns cap duration in seconds plus ``remaining_reps``
    (each rep adds one second), e.g. ``time_cap="10:00"`` and ``CAP+20`` →
    620.0.

    ``time_cap`` may be seconds (``int`` / ``float``) or the same colon string
    formats as the score. If neither ``time_cap`` nor ``time_cap_seconds`` is
    set for a cap-style score, returns ``None``. When both are set,
    ``time_cap`` wins. ``time_cap_seconds`` is a legacy alias for ``time_cap``.

    Non-string score inputs are returned as ``float``.
    """
    cap_base = _time_cap_value_to_seconds(
        time_cap if time_cap is not None else time_cap_seconds
    )

    if score is None:
        return None
    if isinstance(score, bool):
        return None
    if isinstance(score, (int, float)):
        return float(score)
    if not isinstance(score, str):
        return None

    s = score.strip()
    if not s:
        return None

    colon = _parse_colon_duration_to_seconds(s)
    if colon is not None:
        return colon

    cap_m = re.match(r"(?i)cap\s*\+\s*(\d+)", s)
    if cap_m:
        if cap_base is None:
            return None
        return cap_base + float(cap_m.group(1))

    return None

def convert_seconds_to_time_score(seconds: float) -> str:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = float(seconds % 60)

    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:05.2f}"
    elif minutes > 0:
        return f"{minutes:02d}:{secs:05.2f}"
    else:
        return f"{secs:05.2f}"

def convert_value_to_display(score_type: str, value: float) -> str:
    from pandas import isna
    if isna(value):
        return None
    elif value == 0:
        return '--'
    elif score_type == 'time':
        return convert_seconds_to_time_score(value)
    else:
        return str(int(value))

def recover_points_table(
    entrants: list[Entrant],
    scores: list[Score],
):

    """
    This is a linear programming problem that solves for the points awarded for each rank.
    Required inputs: simplied to be a list of entrants and scores in pydantic models for a SINGLE
    competition and gender division.
    (More than what we actually, need, which is just the list of athletes, how many times they
    got each rank in individual workouts ,and their total points at the end of the competition).
    Output: dictionary of rank: points
    """
    ## convert the entrants and scores to dataframes"""
    import pandas as pd
    entrants_df = pd.DataFrame(
        [e.model_dump() for e in entrants]
    )
    scores_df = pd.DataFrame(
        [s.model_dump() for s in scores]
    )

    ## make sure we are only working with one competition and gender division
    assert scores_df[['source_comp_id','gender']].drop_duplicates().shape[0] == 1
    assert entrants_df[['source_comp_id','gender']].drop_duplicates().shape[0] == 1

    ## filter out entrants and scores with no points, non-finishers
    entrants_df = entrants_df.loc[
        entrants_df['overall_points'].notnull(),
        ['source_athlete_id','overall_points']
    ].astype({'source_athlete_id':str,'overall_points':int})

    ## filter out scores with no rank, non-finishers
    scores_df['rank'] = pd.to_numeric(scores_df['rank'],errors='coerce')
    scores_df = scores_df[
        scores_df['source_athlete_id'].isin(entrants_df['source_athlete_id']) &
        scores_df['rank'].gt(0) & scores_df['rank'].notnull()
    ].astype({'source_workout_id':str,'rank':int})

    ## create a dataframe where the rows are the athletes,
    ## thc columns are the ranks/positions
    ## and values are the number of times the athlete has that rank/position
    rank_counts = pd.pivot_table(
        scores_df,
        index=['source_athlete_id'],
        columns=['rank'],
        values='source_workout_id',
        aggfunc=len,
        fill_value=0
    )
    max_rank = max([c for c in rank_counts.columns])

    ## mmerge with the entrants dataframe to get the total points for each athlete
    ## that target should be the sum(count of rank * rank points)
    ## we are solving for rank points
    rank_counts = pd.merge(
        rank_counts,
        entrants_df.set_index('source_athlete_id')['overall_points'],
        left_index=True,
        right_index=True
    )
    
    import pulp
    model = pulp.LpProblem("recover_points_table", pulp.LpMinimize)

    # unknown points awarded for each rank, between 0 and 100
    points = {
        r: pulp.LpVariable(f"points_rank_{r}", lowBound=0, upBound=100, cat="Integer")
        for r in range(1, max_rank + 1)
    }

    ## we know that first place gets 100 points
    model += points[1] == 100

    ## we know that each rank gets more points than the next rank
    for r in range(1, max_rank):
        model += points[r] >= points[r + 1] + 1

    ## set the predicted scores for each athlete to the sum of the rank points
    ## and the actual scores for each athlete to the overall points
    ## we are solving for the rank points
    predicted_scores = {}
    errors = {}
    for i, row in rank_counts.iterrows():
        predicted = pulp.lpSum(
            rank_counts.loc[i, r] * points[r]
            for r in range(1, max_rank + 1)
        )
        predicted_scores[i] = predicted
        actual = int(row["overall_points"])

        err = pulp.LpVariable(f"abs_error_{i}", lowBound=0, cat="Integer")
        errors[i] = err
        model += predicted - actual <= err
        model += actual - predicted <= err

    model += pulp.lpSum(errors.values())

    ## solve the model
    model.solve()

    ## get the recovered points for each rank
    ## dictionary of rank: points
    recovered_points = {
        r: int(pulp.value(points[r]))
        for r in range(1, max_rank + 1)
    }

    return recovered_points

from pycountry import countries
from rapidfuzz import process
from unidecode import unidecode

COUNTRY_NAMES = {}
for c in countries:
    try:
        n = unidecode(c.official_name.lower())
        COUNTRY_NAMES[n] = c.alpha_3
    except:
        pass

    try:
        n = unidecode(c.name.lower())
        COUNTRY_NAMES[n] = c.alpha_3
    except:
        pass

    try:
        n = unidecode(c.common_name.lower())
        COUNTRY_NAMES[n] = c.alpha_3
    except:
        pass

    COUNTRY_NAMES['palestinian territory'] = countries.get(alpha_3='PSE').alpha_3
    COUNTRY_NAMES['estados unidos'] = countries.get(alpha_2='US').alpha_3
    COUNTRY_NAMES['deutschland'] = countries.get(alpha_2='DE').alpha_3
    COUNTRY_NAMES['espana'] = countries.get(alpha_2='ES').alpha_3
    COUNTRY_NAMES['turkey'] = countries.get(alpha_3='TUR').alpha_3
    COUNTRY_NAMES['iroquois'] = 'HAU'
    COUNTRY_NAMES['haudenosaunee'] = 'HAU'
    COUNTRY_NAMES['america'] = countries.get(alpha_2='US').alpha_3
    COUNTRY_NAMES['rossiia'] = countries.get(alpha_2='RU').alpha_3
    COUNTRY_NAMES['osterreich'] = countries.get(alpha_2='AT').alpha_3

ALPHA_3_REMAP = {
    'SUI': 'CHE',
    'DEN': 'DNK',
}

def fuzzy_match_country(x):
    match, _, _ = process.extractOne(
        unidecode(x.lower()).strip(),
        COUNTRY_NAMES.keys(),
        score_cutoff=70
    )
    return COUNTRY_NAMES[match] if match is not None else None

def get_country_code(x: str | None) -> str | None:
    if x is None:
        return None

    if len(x) == 3:
        try:
            for k, v in ALPHA_3_REMAP.items():
                x = x.replace(k, v)
            c = countries.get(alpha_3=x)
            if c is not None:
                return c.alpha_3
        except ValueError:
            pass
    elif len(x) == 2:
        try:
            c = countries.get(alpha_2=x)
            if c is not None:
                return c.alpha_3
        except ValueError:
            pass
    else:
        try:
            c = fuzzy_match_country(x)
            if c is not None:
                return c
        except:
            pass

    return None
