## Pydantic models for parsing JSON responses from APIs 

from pydantic import BaseModel, Field, field_validator
from datetime import datetime, timezone
import re

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
    age: int
    height_in: int | None = Field(default=None, alias='height')
    weight_lb: int | None = Field(default=None, alias='weight')
    comp_status: str = Field(alias='status')
    overall_rank: int | None = Field(default=None, alias='overallRank')
    overall_score: int | None = Field(default=None, alias='overallScore')
    lb_page: int = Field(alias='currentPage')

    @field_validator('height_in', mode='before')
    @classmethod
    def parse_height_to_inches(cls, height: str | None):
        if height is None or (isinstance(height, str) and not height.strip()):
            return None
        v = re.search(r'(\d+)', height)
        if not v:
            return None
        v = float(v.group(1))
        h = height.lower()
        if 'cm' in h:
            return int(round(v / 2.54))
        if 'in' in h:
            return int(v)
        raise ValueError(f'Unknown height format: {height}')

    @field_validator('weight_lb', mode='before')
    @classmethod
    def parse_weight_to_lbs(cls, weight: str | None):
        if weight is None or (isinstance(weight, str) and not weight.strip()):
            return None
        v = re.search(r'(\d+)', weight)
        if not v:
            return None
        v = float(v.group(1))
        w = weight.lower()
        if 'kg' in w:
            return int(round(v * 2.20462))
        if 'lb' in w:
            return int(v)
        raise ValueError(f'Unknown weight format: {weight}')

class CFScore(BaseModel):
    comp_id: int = Field(alias='competitionId')
    division_id: int = Field(alias='division')
    cf_id: int = Field(alias='competitorId')
    ordinal: int
    rank: int | None = None
    score_points: int = Field(alias='score')
    score_display: str = Field(alias='scoreDisplay')

    @field_validator('rank', mode='before')
    @classmethod
    def parse_rank(cls, rank: str | int | None):
        if rank is None:
            return None
        try:
            return int(rank)
        except (ValueError, TypeError):
            return None