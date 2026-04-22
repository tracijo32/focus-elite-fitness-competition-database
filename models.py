## Pydantic models for parsing JSON responses from APIs 

from pydantic import BaseModel, Field, field_validator
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
