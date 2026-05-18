## Pydantic models for parsing JSON responses from APIs 

from pydantic import BaseModel, Field, field_validator, model_validator, AliasChoices
from datetime import datetime, timezone
import re

def parse_integer(value: str | int | None):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def parse_measurement(
    measurement: str | int | None,
    desired_unit: str
):
    if measurement is None:
        return None
    if isinstance(measurement, int):
        return measurement
    if not isinstance(measurement, str) or not measurement.strip():
        return None

    measurement = measurement.lower()
    v = re.search(r'(\d+)', measurement)
    if not v:
        return None
    v = float(v.group(1))

    u = re.search(r'(kg|lb|in|cm)', measurement)
    if not u:
        return None
    m_unit = u.group(1)

    if m_unit == desired_unit:
        return int(round(v))

    if m_unit == 'kg' and desired_unit == 'lb':
        return int(round(v * 2.20462))
    if m_unit == 'lb' and desired_unit == 'kg':
        return int(round(v / 2.20462))
    if m_unit == 'in' and desired_unit == 'cm':
        return int(round(v * 2.54))
    if m_unit == 'cm' and desired_unit == 'in':
        return int(round(v / 2.54))

    return None

def parse_digits(v):
    if isinstance(v, str):
        m = re.search(r"\d+", v)
        return int(m.group()) if m else None
    if isinstance(v, int):
        return v
    return None

def parse_float(v):
    try:
        return float(v)
    except:
        return None

class CFCompetition(BaseModel):
    comp_id: int = Field(alias='id')
    type: str
    parent_type: str | None = None
    year: int
    start_date: datetime
    end_date: datetime
    name: str
    status: str = Field(alias='leaderboard_mode')

    @field_validator('start_date', 'end_date', mode='before')
    @classmethod
    def string_to_utc_datetime(cls, v):
        if isinstance(v, datetime):
            if v.tzinfo is None:
                return v.replace(tzinfo=timezone.utc)
            return v.astimezone(timezone.utc)
        if isinstance(v, str):
            s = v.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        return v

    @property
    def api_url_path(self):
        return "/".join([
            'leaderboards',
            'v2',
            'competitions',
            self.parent_type or self.type, 
            str(self.year),
            'leaderboards'
        ])

    @property
    def api_url_params(self):
        params = {}
        if self.parent_type:
            p = self.parent_type[:-1]
            params[p] = self.comp_id
        if self.type == 'open':
            params['scaled'] = 0
            params['region'] = 0
        return params

class CFEntrant(BaseModel):
    comp_id: int = Field(alias='competitionId')
    division_id: int = Field(alias='division')
    cf_id: int = Field(alias='competitorId')
    first_name: str = Field(alias='firstName')
    last_name: str = Field(alias='lastName')
    country_code: str = Field(alias='countryOfOriginCode')
    age: int | None = None
    height_in: int | None = Field(default=None, alias='height')
    weight_lb: int | None = Field(default=None, alias='weight')
    comp_status: str = Field(alias='status')
    overall_rank: int | None = Field(default=None, alias='overallRank')
    overall_score: int | None = Field(default=None, alias='overallScore')
    lb_page: int = Field(alias='currentPage')

    @field_validator('height_in', mode='before')
    @classmethod
    def parse_height_to_inches(cls, height: str | None):
        return parse_measurement(height, 'in')

    @field_validator('weight_lb', mode='before')
    @classmethod
    def parse_weight_to_lbs(cls, weight: str | None):
        return parse_measurement(weight, 'lb')

    @field_validator('overall_rank', mode='before')
    @classmethod
    def parse_overall_rank(cls, rank: str | int | None):
        return parse_integer(rank)

    @field_validator('overall_score', mode='before')
    @classmethod
    def parse_overall_score(cls, score: str | int | None):
        return parse_integer(score)

    @field_validator('age', mode='before')
    @classmethod
    def parse_age(cls, age: str | int | None):
        return parse_integer(age)

class CFScore(BaseModel):
    comp_id: int = Field(alias='competitionId')
    division_id: int = Field(alias='division')
    cf_id: int = Field(alias='competitorId')
    ordinal: int
    rank: int | None = None
    score_points: int | None = Field(alias='score')
    score_display: str = Field(alias='scoreDisplay')

    @field_validator('rank', mode='before')
    @classmethod
    def parse_workout_rank(cls, rank: str | int | None):
        return parse_integer(rank)

    @field_validator('score_points', mode='before')
    @classmethod
    def parse_workout_score(cls, score: str | int | None):
        return parse_integer(score)

class StrongestCompetition(BaseModel):
    comp_id: str = Field(alias='id')
    title: str
    venue_name: str = Field(alias='venueName')
    address: str = Field(alias='venueAddress')
    lat: float | None = Field(alias='place')
    lng: float | None = Field(alias='place')
    start_date: str = Field(alias='dateTimeStart')
    end_date: str = Field(alias='dateTimeEnd')
    timezone: str
    virtual: bool
    link: str = Field(alias='fullLink')
    banner_image_url: str = Field(alias='bannerImageUrl')
    workouts: list[str]

    @field_validator('lat', mode='before')
    def parse_lat(cls, v):
        if isinstance(v, dict):
            return v['geometry']['location']['lat']
        return None

    @field_validator('lng', mode='before')
    def parse_lng(cls, v):
        if isinstance(v, dict):
            return v['geometry']['location']['lng']
        return None

    @model_validator(mode="after")
    def update_link_and_title(self):
        comp_id = self.comp_id
        y = re.search(r'(\d{4})', self.title)
        year = y.group() if y else '2020'
        if comp_id.startswith('ri'):
            self.link = f"https://www.roguefitness.com/invitational/leaderboard"
            self.title = f"Rogue Invitational {year}"
        elif comp_id.startswith('mayhem'):
            if 'qualifier' in self.title.lower():
                self.title = f"Mayhem Classic {year} Qualifier"
            else:
                self.title = f"Mayhem Classic {year}"
        else:
            self.link = self.link
            self.title = self.title
        return self

class StrongestWorkout(BaseModel):
    workout_id: str = Field(alias='id')
    comp_id: str = Field(alias='competitionId')
    title: str
    html_content: str = Field(alias='content')

class StrongestEntrant(BaseModel):
    comp_id: str
    division_id: str
    name: str = Field(alias='competitor_name')
    username: str | None = Field(alias='teamProfiles')
    country: str | None = Field(alias='teamProfiles')
    registration_id: str = Field(alias='registrationId')
    wd: bool = False
    overall_rank: int | None = Field(alias=AliasChoices('ordinalRank','overall'))
    overall_score: int | None = Field(alias='cum_workout_rank')

    @field_validator('username', mode='before')
    def parse_username(cls, v):
        if isinstance(v, list):
            p = v[0]
            if isinstance(p,dict):
                return p.get('username')
        return None

    @field_validator('country', mode='before')
    def parse_country(cls, v):
        if isinstance(v, list):
            p = v[0]
            if isinstance(p,dict):
                return p.get('country')
        return None

    @field_validator('overall_rank', mode='before')
    def parse_overall_rank(cls, v):
        return parse_digits(v)

class StrongestScore(BaseModel):
    comp_id: str
    division_id: str
    registration_id: str
    workout_id: str
    rank: int | None = Field(alias='workout_rank')
    score_label: str = Field(alias='workout_score_label')
    score_value: float | None = Field(alias='workout_score_value')
    score_units: str | None = Field(alias='workout_score_units',default=None)
    tiebreaker_value: float | None = Field(alias='workout_tiebreaker_value',default=None)
    tiebreaker_units: str | None = Field(alias='workout_tiebreaker_units',default=None)
    tiebreaker_2_value: float | None = Field(alias='workout_tiebreaker_2_value',default=None)
    tiebreaker_2_units: str | None = Field(alias='workout_tiebreaker_2_units',default=None)
    score_points: int | None = Field(alias='workout_score_points',default=None)

    @field_validator('rank', mode='before')
    def parse_rank(cls, v):
        return parse_digits(v)

    @field_validator('score_value', mode='before')
    def parse_score_value(cls, v):
        return parse_float(v)

    @field_validator('tiebreaker_value', mode='before')
    def parse_tiebreaker_value(cls, v):
        return parse_float(v)

class StrongestScoringPolicy(BaseModel):
    comp_id: str = Field(alias='competitionId')
    division_id: str = Field(alias='division')
    workout_id: str = Field(alias='workout')
    points_table: list[int] = Field(alias='customPointsTable')
    tiebreaker_2_enabled: bool = Field(alias='tiebreaker2Enabled')
    scoring_policy: str = Field(alias='scoringPolicy')
    score_type: str = Field(alias='scoreType')
    tiebreaker_score_type: str = Field(alias='tiebreakerScoreType')
    tiebreaker_2_score_type: str = Field(alias='tiebreaker2ScoreType')
    
    @field_validator('points_table', mode='before')
    def parse_points_table(cls, v):
        if isinstance(v, str):
            return [int(x) for x in v.split(',')]
        return v

class ManualCompetition(BaseModel):
    comp_id: str
    title: str
    start_date: datetime
    end_date: datetime
    venue_name: str | None = None
    address: str | None = None
    lat: float | None = None
    lng: float | None = None
    virtual: bool
    source_url: str | None = None
    source_name: str | None = None
    source_description: str | None = None
    acknowledgement: str | None = None

class ManualEntrant(BaseModel):
    comp_id: str
    gender: str
    athlete_id: int
    name: str
    overall_rank: int | None = None
    overall_points: int | None = None

class ManualScore(BaseModel):
    comp_id: str
    gender: str
    athlete_id: int
    ordinal: int
    score: str | None = None
    tiebreak: str | None = None
    rank: int | None = None
    points: int | None = None

class ScoreItCompetition(BaseModel):
    comp_id: int = Field(alias='eventId')
    event_ref: str = Field(alias='ref')
    title: str = Field(alias='eventName')
    address: str | None = Field(alias='eventAddress',default=None)
    start_date: datetime = Field(alias='dateActiveFrom')
    end_date: datetime = Field(alias='dateActiveTo')

class ScoreItEntrant(BaseModel):
    event_ref: str
    division_ref: str
    entrant_ref: str = Field(alias='teamRef')
    entrant_name: str = Field(alias='teamName')
    overall_rank: int | None = Field(alias='position',default=None)
    overall_points: int | None = Field(alias='totalPoints',default=None)

class ScoreItScore(BaseModel):
    event_ref: str
    division_ref: str
    entrant_ref: str
    workout_ref: str = Field(alias='courseWorkoutRef')
    code: str = Field(alias='scoringMeasurementCode')
    value: float | None = Field(alias='value',default=None)
    time: str | None = None
    rep_count: int | None = Field(alias='repCount',default=None)
    weight: float | None = Field(alias='weight',default=None)
    points: int | None = Field(alias='pointsEarned',default=None)
    rank: int | None = Field(alias='position',default=None)
    tiebreak: str | None = Field(alias='tiebreak',default=None)

class Entrant(BaseModel):
    source_comp_id: str
    gender: str
    display_name: str
    source_athlete_id: str
    overall_rank: int | None = None
    overall_points: float | None = None

class Score(BaseModel):
    source_comp_id: str
    gender: str
    source_athlete_id: str
    ordinal: int
    score_display: str
    tiebreak_display: str | None = None
    rank: int | None = None
    points: float | None = None

class Metadata(BaseModel):
    source_comp_id: str
    title: str
    start_date: datetime
    end_date: datetime
    venue_name: str | None = None
    address: str | None = None
    lat: float | None = None
    lng: float | None = None
    virtual: bool