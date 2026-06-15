from pydantic import BaseModel, field_validator
from datetime import datetime, date
from pycountry import countries

def convert_date_to_string(v: str | datetime | date | None) -> str | None:
    if isinstance(v, datetime) or isinstance(v, date):
        return v.strftime('%Y-%m-%d')
    elif isinstance(v, str):
        try:
            return datetime.strptime(v.split('T')[0], '%Y-%m-%d')\
                .strftime('%Y-%m-%d')
        except ValueError:
            raise ValueError(f"Invalid date format: {v}")
    else:
        return v

def convert_time_to_string(v: str | datetime | None) -> str | None:
        if isinstance(v, datetime):
            return v.strftime('%Y-%m-%d %H:%M')
        elif isinstance(v, str):
            try:
                dt = " ".join(v.split(' ')[:2])
                datetime.strptime(dt,'%Y-%m-%d %H:%M')
                return v
            except ValueError:
                raise ValueError(f"Invalid time format: {v}")
        else:
            return None

class Entrant(BaseModel):
    source_comp_id: str
    gender: str
    display_name: str
    source_athlete_id: str
    source_division_id: str | None = None
    home_gym: str | None = None
    country_code: str | None = None
    overall_rank: int | None = None
    overall_points: float | None = None
    dq: bool = False
    wd: bool = False
    dnf: bool = False

    @field_validator('country_code', mode='before')
    def validate_country_code(cls, v):
        if v is None:
            return None
        try:
            return countries.get(alpha_3=v).alpha_3
        except ValueError:
            raise ValueError(f"Invalid country code: {v}")

class Score(BaseModel):
    source_comp_id: str
    gender: str
    source_athlete_id: str
    source_workout_id: str
    score_display: str
    tiebreak_display: str | None = None
    rank: int | None = None
    points: float | None = None
    source_division_id: str | None = None

class Metadata(BaseModel):
    source_comp_id: str
    title: str
    start_date: str
    end_date: str
    venue_name: str | None = None
    address: str | None = None
    lat: float | None = None
    lng: float | None = None
    virtual: bool

    @field_validator('start_date', 'end_date',mode='before')
    def validate_date(cls, v):
        return convert_date_to_string(v)

class Workout(BaseModel):
    source_comp_id: str
    source_workout_id: str
    workout_name: str
    seq: int
    date: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    description: str | None = None

    @field_validator('date',mode='before')
    def validate_date(cls, v):
        return convert_date_to_string(v)

    @field_validator('start_time', 'end_time',mode='before')
    def validate_time(cls, v):
        return convert_time_to_string(v)

class Source(BaseModel):
    global_comp_id: str
    priority: int
    source: str
    source_comp_id: str
    division_male: str
    division_female: str

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

class CrossFitStage(BaseModel):
    global_comp_id: str
    season: int
    stage: str
    comp_id: int | None = None
    previous_stage: str | None = None
    next_stage: str | None = None

    @field_validator('comp_id', mode='before')
    def validate_comp_id(cls, v):
        if v is None:
            return None
        try:
            return int(v)
        except ValueError:
            raise ValueError(f"Invalid comp_id: {v}")