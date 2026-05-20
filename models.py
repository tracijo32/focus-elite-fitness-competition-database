from pydantic import BaseModel
from datetime import datetime

class Entrant(BaseModel):
    source_comp_id: str
    gender: str
    display_name: str
    source_athlete_id: str
    overall_rank: int | None = None
    overall_points: float | None = None
    disqualified: bool = False

class Score(BaseModel):
    source_comp_id: str
    gender: str
    source_athlete_id: str
    source_workout_id: str
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

from pydantic import BaseModel

class CrossFitEntrant(BaseModel):
    cf_id: int
    comp_id: int
    comp_type: str
    year: int
    overall_rank: str | None = None
    overall_score: str | None = None
    name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    status: str | None = None
    gender: str | None = None
    profile_pic: str | None = None
    country_code: str | None = None
    country_name: str | None = None
    region_id: str | None = None
    region_name: str | None = None
    division_id: str | None = None
    affiliate_id: str | None = None
    affiliate_name: str | None = None
    age: int | None = None
    height: str | None = None
    weight: str | None = None

class CrossFitScore(BaseModel):
    cf_id: int
    comp_id: int
    gender: str
    ordinal: int
    rank: str | None = None
    score: int
    score_display: str | None = None
    valid: bool
    time: float | None = None
    judge: str | None = None
    affiliate: str | None = None
    points: int | None = None
    judge_user_id: int | None = None

