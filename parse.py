from pydantic import BaseModel
import inventory
import pandas as pd
from models import Entrant, Score, Metadata, Workout
from util import convert_value_to_display
from parameters import GoogleCloudParameters
from geopy.geocoders import GoogleV3
from bs4 import BeautifulSoup
from util import get_country_code

class Parser:
    def __init__(self, manager: inventory.InventoryManager):
        self.manager = manager()
        gcp_params = GoogleCloudParameters()
        self.geolocator = GoogleV3(api_key=gcp_params.maps_api_key)

    def build_parsed_blob_name(self, model_name: str, **kwargs):
        c = kwargs['comp_id']
        blob_name = f'{self.manager.source}/parsed/{c}/{model_name}.ndjson'
        return blob_name

    def save_model_to_ndjson(
        self, 
        models: list[BaseModel],
        blob_name: str
    ):
        model_type = type(models[0])
        assert all(isinstance(m, model_type) for m in models)

        blob = self.manager.bucket.blob(blob_name)
        models_json = "\n".join([m.model_dump_json() for m in models])
        blob.upload_from_string(models_json)

    def get_leaderboard_frame(
        self,
        comp_id: str,
        division_male: str,
        division_female: str,
        refresh: bool = False,
        **kwargs
    ):
        lb = [
            self.manager.load_leaderboard(
                comp_id=comp_id,
                refresh=refresh,
                **d
            )
            for d in [
                {'div_id': division_male, 'gender': 'M'},
                {'div_id': division_female, 'gender': 'F'}
            ]
        ]
        df = pd.DataFrame([p for page in lb for p in page])
        return df
    
    def dump_metadata(self, metadata: Metadata, comp_id: str):
        blob_name = self.build_parsed_blob_name(
            model_name='metadata',comp_id=comp_id)
        self.save_model_to_ndjson(
            models=[metadata],
            blob_name=blob_name
        )
        return

    def dump_frame(
        self, 
        df: pd.DataFrame, 
        model_name: str,
        model: BaseModel,
        **kwargs
    ):
        ## convert the entrants to pydantic models
        models = [
            model(**row.dropna().to_dict()) 
            for _, row in df.iterrows()
        ]
        blob_name = self.build_parsed_blob_name(
            model_name=model_name,**kwargs)
        self.save_model_to_ndjson(
            models=models,
            blob_name=blob_name
        )
        return

    def dump_entrants_frame(
        self, entrants_df: pd.DataFrame, **kwargs
    ):
        self.dump_frame(
            df=entrants_df,
            model_name='entrants',
            model=Entrant,
            **kwargs
        )
        return
    
    def dump_scores_frame(
        self, scores_df: pd.DataFrame, **kwargs
    ):
        self.dump_frame(
            df=scores_df,
            model_name='scores',
            model=Score,
            **kwargs
        )
        return

    def dump_workouts_frame(
        self, workouts_df: pd.DataFrame, **kwargs
    ):
        self.dump_frame(
            df=workouts_df,
            model_name='workouts',
            model=Workout,
            **kwargs
        )
        return

class StrongestParser(Parser):
    def __init__(self):
        super().__init__(manager=inventory.StrongestInventoryManager)

    def get_policy_frame(self, comp_id: str, refresh: bool = False):
        scoring_policies = self.manager.load_scoring_policies(comp_id=comp_id, refresh=refresh)
        sp_df = pd.DataFrame(scoring_policies['data']).assign(comp_id=comp_id)\
            [['comp_id','id','division','workout','scoreType',
            'tiebreakerScoreType','tiebreaker2ScoreType','customPointsTable']]\
            .rename(columns={
                'division': 'div_id',
                'workout': 'workout_id'
            })
        return sp_df

    def get_workout_frame(self, comp_id: str, refresh: bool = False):
        workouts = self.manager.load_workouts(comp_id=comp_id, refresh=refresh)
        wo_df = pd.DataFrame(workouts['data']).assign(comp_id=comp_id)\
            [['comp_id','id','title','content']]\
            .rename(columns={
                'id': 'workout_id',
                'title': 'workout_name',
                'content': 'description'
            })
        return wo_df

    def get_config_frame(
        self,
        comp_id: str,
        refresh: bool = False
    ):
        config = self.manager.load_event_configs(comp_id=comp_id, refresh=refresh)
        if len(config['data']) == 0:
            return pd.DataFrame()
        conf_df = pd.DataFrame(config['data']).assign(comp_id=comp_id)\
            [['comp_id','division','workout','startTime','endTime']]\
                .rename(columns={
                    'division': 'div_id',
                    'workout': 'workout_id',
                    'startTime': 'start_time',
                    'endTime': 'end_time'
                })
        conf_df['start_time'] = pd.to_datetime(conf_df['start_time'])
        conf_df['end_time'] = pd.to_datetime(conf_df['end_time'])
        return conf_df

    def get_leaderboard_frame(
        self, 
        comp_id: str, 
        division_male: str, 
        division_female: str,
        refresh: bool = False
    ):
        df = super().get_leaderboard_frame(
            comp_id=comp_id,
            division_male=division_male,
            division_female=division_female,
            refresh=refresh
        )
        df = pd.merge(
            df[['comp_id','div_id','gender']],
            df['data'].apply(pd.Series)['data'].apply(pd.Series),
            left_index=True,
            right_index=True
        )

        df = df[['comp_id','gender','div_id','body_rows']]\
            .explode('body_rows',ignore_index=True)

        return df

    def get_entrants_and_scores_frame(
        self,
        comp_id: str,
        division_male: str,
        division_female: str,
        refresh: bool = False
    ):
        ## load all the data
        df = self.get_leaderboard_frame(
            comp_id=comp_id,
            division_male=division_male,
            division_female=division_female,
            refresh=refresh
        )
        sp_df = self.get_policy_frame(comp_id=comp_id, refresh=refresh)
        wo_df = self.get_workout_frame(comp_id=comp_id, refresh=refresh)\
            .drop(columns=['description'])

        ## create a dataframe of the entrants
        ## has entrant name, overall rank, and overall points
        entrants_df = pd.merge(
            df[['comp_id','gender','div_id']],
            df['body_rows'].apply(lambda x: x[0]).apply(pd.Series),
            left_index=True,
            right_index=True
        ).explode('teamProfiles', ignore_index=True)

        entrants_df = pd.merge(
            entrants_df.drop(columns=['teamProfiles']),
            entrants_df['teamProfiles'].apply(pd.Series)[['country']],
            left_index=True,
            right_index=True
        ).reindex(
            columns=['comp_id','div_id','gender',
            'competitor_name','gym','country',
            'overall','registrationId','cum_workout_rank']
        ).rename(columns={
            'overall': 'overall_rank',
            'cum_workout_rank': 'overall_points',
            'comp_id': 'source_comp_id',
            'div_id': 'source_division_id',
            'registrationId': 'source_athlete_id',
            'competitor_name': 'display_name',
            'country': 'country_code',
            'gym': 'home_gym'
        })
        entrants_df['country_code'] = entrants_df['country_code'].apply(
            get_country_code
        )

        entrants_df['overall_rank'] = entrants_df['overall_rank'].str.extract(r'(\d+)')\
            .apply(pd.to_numeric, errors='coerce')
        entrants_df['overall_points'] = pd.to_numeric(
            entrants_df['overall_points'],
            errors='coerce'
        )
        entrants_df['dq'] = False

        ## create a dataframe of the scores
        ## has workout number, score, rank, and points

        ## get the registration id of the athlete
        ## to match back to the entrant dataframe
        scores_df = pd.merge(
            df[['comp_id','gender','div_id']],
            df['body_rows'].apply(lambda x: x[0]).apply(pd.Series)['registrationId'],
            left_index=True,
            right_index=True
        ).rename(columns={
            'registrationId':'source_athlete_id'
        })

        ## get the workout number, score, rank, and points
        scores_df = pd.merge(
            scores_df,
            df['body_rows'].apply(lambda x: x[1:]),
            left_index=True,
            right_index=True
        ).explode('body_rows',ignore_index=True)

        scores_df = pd.merge(
            scores_df[['comp_id','gender','source_athlete_id','div_id']],
            scores_df['body_rows'].apply(pd.Series),
            left_index=True,
            right_index=True
        )

        ## merge in the workout and scoring policy data
        ## we need this for score types and points tables
        scores_df = pd.merge(
            scores_df,
            wo_df[['comp_id','workout_id','workout_name']],
            on=['comp_id','workout_name'],
            how='left',
            indicator=True
        )
        assert scores_df['_merge'].value_counts()['left_only'] == 0

        scores_df = pd.merge(
            scores_df.drop(columns=['_merge']),
            sp_df,
            on=['comp_id','div_id','workout_id'],
            indicator=True
        )
        assert scores_df['_merge'].value_counts()['left_only'] == 0
        scores_df = scores_df.drop(columns=['_merge'])

        ## properly create the score display based on the numeric score
        ## and the score type + units
        scores_df['workout_score_value'] = scores_df['workout_score_value'].astype(float)

        scores_df['score_units'] = scores_df['scoreType'].fillna(scores_df['scoreType'])
        scores_df.loc[
            scores_df['score_units'].eq('time'),
            'score_units'
        ] = None

        scores_df['score_display'] = scores_df.apply(
            lambda x: None if pd.isna(x['workout_score_value']) else
            convert_value_to_display(x['scoreType'], x['workout_score_value']),
            axis=1
        )

        ## replace the label of the score with the label of the workout
        ## if the score was capped
        scores_df['label'] = scores_df['workout_score_label'].astype(str)\
            .str.split('(',n=1,expand=True)[0].str.strip()

        scores_df.loc[
            ~scores_df['label'].str.contains('CAP',case=False),
            'label'
        ] = None

        scores_df['score_display'] = scores_df['label'].fillna(scores_df['score_display'])

        ## do the same display formatting for the tiebreaker
        ## if it exists, else just set it as blank
        if 'workout_tiebreaker_value' in scores_df.columns:
            scores_df['workout_tiebreaker_value'] = scores_df['workout_tiebreaker_value'].astype(float)
            scores_df['tiebreaker_display'] = scores_df.apply(
                lambda x: convert_value_to_display(
                    x['tiebreakerScoreType'], x['workout_tiebreaker_value']
                ),
                axis=1
            )

            scores_df['tiebreaker_units'] = scores_df['tiebreakerScoreType']
            scores_df.loc[
                scores_df['workout_tiebreaker_value'].isna() | 
                scores_df['workout_tiebreaker_value'].eq(0) | 
                scores_df['tiebreakerScoreType'].eq('time'),
                'tiebreaker_units'
            ] = None

            scores_df['tiebreaker_display'] = scores_df[['tiebreaker_display','tiebreaker_units']].apply(
                lambda x: ' '.join(x.dropna().astype(str)),
                axis=1
            )

            scores_df.loc[
                scores_df['tiebreaker_display'].eq(''),
                'tiebreaker_display'] = None
        else:
            scores_df['tiebreaker_display'] = None

        ## points method #1: points equal rank in workout, 
        ## i.e., first place gets 1 point, second place gets 2 points, etc.
        ## winner has total lowest rank points
        ## convert rank to numeric (remove 'st', 'nd', 'rd', 'th', etc. and 'T' for ties)
        scores_df['rank'] = scores_df['workout_rank'].astype(str).str.extract(r'(\d+)')

        ## we'll compute 2 points systems for each workout and select the one that fits best
        ## 1. points equal rank in workout, i.e., first place gets 1 point, second place gets 2 points, etc.
        scores_df['rank_points'] = pd.to_numeric(scores_df['rank'],errors='coerce')

        ## 2. each rank gets a fixed number of points, most points goes to first place
        ## points tables are stored in the scoring policies
        ## highest total points wins

        ## convert points table from a comma-separated string to a dictionary
        ## with the key being the rank and the value being the points
        scores_df['points_table'] = scores_df['customPointsTable'].apply(
            lambda x: {str(r+1):str(p) for r,p in enumerate(x.split(','))}
        )

        ## select the points for the rank of each row
        scores_df['games_points'] = scores_df.apply(
            lambda x: x['points_table'].get(x['rank']),
            axis=1
        )
        scores_df['games_points'] = pd.to_numeric(scores_df['games_points'],errors='coerce')

        ## give 0 points to scores when the athlete did no work
        scores_df.loc[
            scores_df['score_display'].eq('--') | scores_df['score_display'].isna(),
            'games_points'
        ] = 0

        scores_df = scores_df.rename(columns={
            'comp_id':'source_comp_id',
            'div_id':'source_division_id',
            'workout_id':'source_workout_id'
        })

        ## compute the total points for each entrant
        e_pts = entrants_df[['source_comp_id','gender','source_athlete_id','overall_points']]
        e_pts['overall_points'] = pd.to_numeric(e_pts['overall_points'],errors='coerce')

        s_pts = scores_df.groupby(['source_comp_id','gender','source_athlete_id'])\
            [['rank_points','games_points']].sum().reset_index()

        ## merge the listed points totals with the scores points
        ## this will allow us to see which points system is more accurate
        ## ideally, the points will be equal to the listed points
        pts = pd.merge(
            e_pts,
            s_pts,
            on=['source_comp_id','gender','source_athlete_id'],
        )

        ## check if the points are equal to the listed points
        pts['is_rank_based'] = pts['rank_points'].eq(pts['overall_points'])
        pts['is_games_based'] = pts['games_points'].eq(pts['overall_points'])

        ## if the points are not equal to the listed points, print a message
        summary = pts[['is_rank_based','is_games_based']].mean()
        if not summary.eq(1).any():
            print(f'there are inaccuracies in the points for {comp_id}')

        ## select the points system that is more accurate
        if summary.idxmax() == 'is_rank_based':
            scores_df['points'] = scores_df['rank_points']
        else:
            scores_df['points'] = scores_df['games_points']

        scores_df = scores_df.reindex(
            columns = ['source_comp_id','gender','source_workout_id',
            'source_athlete_id','points','rank','score_display',
            'source_division_id','tiebreak_display']
        )

        return entrants_df, scores_df

    def parse_leaderboard(
        self, refresh: bool = False, **kwargs
    ):
        entrants_df, scores_df = self.get_entrants_and_scores_frame(refresh=refresh, **kwargs)
        self.dump_entrants_frame(entrants_df, **kwargs)
        self.dump_scores_frame(scores_df, **kwargs)
        return

    def parse_metadata(self, comp_id: str, refresh: bool = False):
        data = self.manager.load_metadata(comp_id=comp_id, refresh=refresh)['data']

        start = pd.to_datetime(data['dateTimeStart']).date()
        end = pd.to_datetime(data['dateTimeEnd']).date()

        kwargs = {
            'source_comp_id': comp_id,
            'title': data['title'],
            'start_date': start,
            'end_date': end,
            'virtual': data['virtual'],
        }

        if not data['virtual']:
            kwargs['venue_name'] = data['venueName']
            kwargs['address'] = data['venueAddress']
            kwargs['lat'] = data['place']['lat']
            kwargs['lng'] = data['place']['lng']

        self.dump_metadata(Metadata(**kwargs), comp_id=comp_id)
        return

    def parse_workouts(
        self, 
        comp_id: str, 
        division_male: str, 
        division_female: str,
        refresh: bool = False
    ):
        wo_df = self.get_workout_frame(comp_id=comp_id, refresh=refresh)
        wo_df['description'] = wo_df['description'].apply(
            lambda x: ''.join(str(p) for p in 
            BeautifulSoup(x, 'html.parser').find_all('p'))
        )

        conf_df = self.get_config_frame(comp_id=comp_id)
        if len(conf_df) == 0:
            df = wo_df.rename(columns={
                'comp_id':'source_comp_id','workout_id':'source_workout_id'
            })
            df['seq'] = df['workout_name'].rank().astype(int)
        else:
            conf_df = conf_df[
                conf_df['div_id'].isin([division_male,division_female])
            ]
            conf_df = conf_df.groupby('workout_id').agg(
                start_time=('start_time','min'),
                end_time=('end_time','max')
            ).reset_index()
            conf_df['seq'] = conf_df['start_time'].rank().astype(int)
            conf_df['date'] = conf_df['end_time'].dt.strftime('%Y-%m-%d')
            conf_df['start_time'] = conf_df['start_time'].dt.strftime('%Y-%m-%d %H:%M')
            conf_df['end_time'] = conf_df['end_time'].dt.strftime('%Y-%m-%d %H:%M')
            
            df = pd.merge(
                conf_df,
                wo_df,
                on=['workout_id']
            )
            df = df.sort_values(by=['seq'])\
                .rename(columns={
                    'comp_id': 'source_comp_id',
                    'workout_id': 'source_workout_id'
                })

        self.dump_workouts_frame(df, comp_id=comp_id)
        return

class ScoreItParser(Parser):
    def __init__(self):
        super().__init__(manager=inventory.ScoreItInventoryManager)

    def parse_metadata(self, comp_id, refresh: bool = False):
        data = self.manager.load_metadata(comp_id=comp_id, refresh=refresh)

        event_loc = data['eventAddress']
        s = event_loc.split(' - ')
        if len(s) == 2:
            venue_name = s[0].strip()
            venue_address = s[1].strip()
        else:
            s = event_loc.split(', ')
            venue_name = s[0].strip()
            venue_address = ', '.join(s[1:])

        start = pd.to_datetime(data['dateActiveFrom']).date()
        end = pd.to_datetime(data['dateActiveTo']).date()

        geo_loc = self.geolocator.geocode(venue_address)
        lat = geo_loc.latitude
        lng = geo_loc.longitude
        address = geo_loc.address
    
        kwargs = {
            'source_comp_id': comp_id,
            'title': data['eventName'],
            'venue_name': venue_name,
            'address': address,
            'lat': lat,
            'lng': lng,
            'start_date': start,
            'end_date': end,
            'virtual': False
        }

        self.dump_metadata(Metadata(**kwargs), comp_id=comp_id)
        return

    def get_leaderboard_frame(
        self,
        comp_id: str,
        division_male: str,
        division_female: str,    
        refresh: bool = False
    ):
        df = super().get_leaderboard_frame(
            comp_id=comp_id,
            division_male=division_male,
            division_female=division_female,
            refresh=refresh
        )
        df = pd.merge(
            df[['comp_id','div_id','gender']],
            df['data'].apply(pd.Series),
            left_index=True,
            right_index=True
        )
        df = pd.merge(
            df[['comp_id','div_id','gender']],
            df['teamDetails'],
            left_index=True,
            right_index=True
        ).explode('teamDetails',ignore_index=True)

        df = pd.merge(
            df[['comp_id','div_id','gender']],
            df['teamDetails'].apply(pd.Series),
            left_index=True,
            right_index=True
        )

        df = df.rename(
            columns={
                'comp_id':'source_comp_id',
                'div_id':'source_division_id',
                'teamName':'display_name',
                'teamRef': 'source_athlete_id',
                'totalPoints': 'overall_points',
                'position': 'overall_rank'
            }
        )
        
        return df

    def get_entrants_frame(self,df: pd.DataFrame):
        ## parse entrants
        entrants_df = df.reindex(
            columns=['source_comp_id','gender',
            'source_athlete_id','source_division_id',
            'display_name','overall_points','overall_rank']
        )
        entrants_df['dq'] = False
        return entrants_df

    def get_scores_frame(self,df: pd.DataFrame):
        scores_df = pd.merge(
            df[['source_comp_id','gender',
            'source_athlete_id','source_division_id']],
            df['leaderboardColumnValues'],
            left_index=True,
            right_index=True
        ).explode('leaderboardColumnValues',ignore_index=True)

        scores_df = pd.merge(
            scores_df.drop(columns=['leaderboardColumnValues']),
            scores_df['leaderboardColumnValues'].apply(pd.Series),
            left_index=True,
            right_index=True
        ).rename(columns={
            'comp_id':'source_comp_id',
            'courseWorkoutRef':'source_workout_id',
            'position':'rank',
            'pointsEarned':'points',
            'tiebreakerTime':'tiebreak_display'
        })

        ## scores are in 3 different columns: time, reps, weight
        ## select the appropriate column based on the scoringMeasurementCode
        scores_df['score_display_time'] = scores_df['time']\
            .where(scores_df['scoringMeasurementCode'].eq('TIME'))
        scores_df['score_display_reps'] = scores_df['repCount']\
            .apply(lambda x: f'{int(x)} reps' if not pd.isna(x) else None)\
            .where(scores_df['scoringMeasurementCode'].eq('REPCOUNT'))
        scores_df['score_display_weight'] = scores_df['weight']\
            .apply(lambda x: f'{int(x)} kg' if not pd.isna(x) else None)\
            .where(scores_df['scoringMeasurementCode'].eq('WEIGHT'))

        scores_df['score_display'] = scores_df['score_display_time']\
            .fillna(scores_df['score_display_reps'])\
            .fillna(scores_df['score_display_weight'])

        scores_df = scores_df.reindex(columns=[
            'source_comp_id','gender','source_athlete_id',
            'source_division_id','source_workout_id',
            'score_display','tiebreak_display','rank','points'
        ])
        return scores_df

    def parse_leaderboard(
        self,
        comp_id: str,
        division_male: str,
        division_female: str,
        refresh: bool = False
    ):
        df = self.get_leaderboard_frame(
            comp_id=comp_id,
            division_male=division_male,
            division_female=division_female,
            refresh=refresh
        )

        entrants_df = self.get_entrants_frame(df)
        scores_df = self.get_scores_frame(df)

        ## output entrants and scores into pydantic models
        entrants = [
            Entrant(**row.dropna().to_dict())
            for _, row in entrants_df.iterrows()
        ]
        scores = [
            Score(**row.dropna().to_dict())
            for _, row in scores_df.iterrows()
        ]
        
        ## upload models as ndjson files
        blob_name_entrants = self.build_parsed_blob_name(
            model_name='entrants',comp_id=comp_id)
        self.save_model_to_ndjson(
            models=entrants,
            blob_name=blob_name_entrants
        )
        blob_name_scores = self.build_parsed_blob_name(
            model_name='scores',comp_id=comp_id)
        self.save_model_to_ndjson(
            models=scores,
            blob_name=blob_name_scores
        )
        return

class CompetitionCornerParser(Parser):
    def __init__(self):
        super().__init__(manager=inventory.CompetitionCornerInventoryManager)

    def parse_metadata(self, comp_id, refresh: bool = False):
        data = self.manager.load_metadata(comp_id=comp_id, refresh=refresh)

        virtual = data['locationType'] != 'onsite'
        location = data.get('location')

        kwargs = {
            'source_comp_id': str(comp_id),
            'title':  data.get('name',''),
            'start_date': pd.to_datetime(data['startDate']).date(),
            'end_date': pd.to_datetime(data['endDate']).date(),
            'virtual': virtual,
        }

        if location and not virtual:
            kwargs['venue_name'] = location.get('venue','')
            street = location.get('street','')
            city = location.get('city','')
            region = location.get('region','')
            country = location.get('country','')
            try:
                lat = float(location.get('lat'))
                lng = float(location.get('lng'))
            except:
                lat = None
                lng = None

            addr = f"{street} {city} {region} {country}".strip()
            if not lat or not lng:
                location = self.geolocator.geocode(addr)
                if location:
                    lat = location.latitude
                    lng = location.longitude
                    addr = location.address

            kwargs['lat'] = lat
            kwargs['lng'] = lng
            kwargs['address'] = addr

        self.dump_metadata(Metadata(**kwargs), comp_id=comp_id)
        return

    def get_leaderboard_frame(
        self,
        comp_id: str,
        division_male: str,
        division_female: str,
        refresh: bool = False
    ):
        df = super().get_leaderboard_frame(
            comp_id=comp_id,
            division_male=division_male,
            division_female=division_female,
            refresh=refresh
        )
        df = pd.merge(
            df[['comp_id','div_id','gender']],
            df['data'].apply(pd.Series)['athletes'],
            left_index=True,
            right_index=True
        ).explode('athletes',ignore_index=True)

        df = pd.merge(
            df.drop(columns=['athletes']),
            df['athletes'].apply(pd.Series),
            left_index=True,
            right_index=True
        ).rename(
            columns = {
                'comp_id': 'source_comp_id',
                'div_id': 'source_division_id',
                'rosterID': 'source_athlete_id',
            }
        ).astype({
            'source_comp_id':str,
            'source_division_id':str,
            'source_athlete_id':str
        })
        return df

    def get_entrants_frame(self,df: pd.DataFrame):
        ## parse out the entrant data
        entrants_df = df.rename(columns={
            'name': 'display_name',
            'place': 'overall_rank',
            'totalPoints': 'overall_points',
            'isDisqualified': 'dq',
            'affiliate': 'home_gym'
        })

        cc1 = entrants_df['countryCode'].apply(get_country_code)
        cc2 = entrants_df['countryShortCode'].apply(get_country_code)
        cc3 = entrants_df['countryName'].apply(get_country_code)

        entrants_df['country_code'] = cc1.fillna(cc2).fillna(cc3)

        entrants_df = entrants_df.reindex(
            columns = [
                'source_comp_id','source_division_id','gender',
                'source_athlete_id','display_name',
                'overall_rank','overall_points',
                'dq','wd','dnf',
                'country_code','home_gym'
            ]
        )

        entrants_df['overall_points'] = pd.to_numeric(entrants_df['overall_points'],errors='coerce')
        entrants_df['overall_rank'] = pd.to_numeric(entrants_df['overall_rank'],errors='coerce')

        return entrants_df

    def get_scores_frame(self,df: pd.DataFrame):
        ## merge entrant data with the workout scores
        scores_df = pd.merge(
            df[['source_comp_id','source_division_id',
            'source_athlete_id','gender']],
            df['workoutScores'].apply(lambda x: list(x.values())),
            left_index=True,
            right_index=True
        ).explode('workoutScores',ignore_index=True)

        scores_df = pd.merge(
            scores_df.drop(columns=['workoutScores']),
            scores_df['workoutScores'].apply(pd.Series),
            left_index=True,
            right_index=True
        ).rename(columns={
            'workoutId':'source_workout_id',
        })

        ## tiebreakers are in parentheses inside <span> tags
        ## canonical score is before the span tags or is standalone
        scores_df = pd.merge(
            scores_df.drop(columns=['res']),
            scores_df['res'].str.extract(
                r'^(?P<score_display>.*?)(?:<span>\s*\((?P<tiebreaker>[^)]+)\)\s*</span>)?$'
            ),
            left_index=True,
            right_index=True
        )

        ## map the units based on what the caption has
        scores_df['unit'] = scores_df['caption'].replace(
            {
                'Weight (kg)':'kg',
                'Weight (lb)':'lb',
                'Time': '',
                'Placement': '',
                'Meters': 'm',
                'Reps': 'rep',
                'Rounds': 'round'
            }
        )

        ## if there isn't a number, then it's probably an invalid score
        ## i.e., WD, DNF, CUT, etc.
        ## don't append any units to these
        scores_df.loc[
            ~scores_df['score_display'].str.contains(r'\d+',regex=True),
        'unit'] = ''

        scores_df['score_display'] = scores_df['score_display'].astype(str) + ' ' + scores_df['unit']
        scores_df['score_display'] = scores_df['score_display'].str.strip()

        scores_df = scores_df.reindex(
            columns=[
                'source_comp_id','source_athlete_id',
                'gender','source_division_id','source_workout_id',
                'score_display','tiebreaker_display',
                'rank','points']
        ).astype({
            'source_comp_id':str,
            'source_division_id':str,
            'source_athlete_id':str,
            'source_workout_id':str,
        })
        scores_df['points'] = pd.to_numeric(scores_df['points'],errors='coerce')
        scores_df['rank'] = pd.to_numeric(scores_df['rank'],errors='coerce')

        return scores_df

    def parse_leaderboard(
        self,
        comp_id: str,
        division_male: str,
        division_female: str,
        refresh: bool = False
    ):
        df = self.get_leaderboard_frame(
            comp_id=comp_id,
            division_male=division_male,
            division_female=division_female,
            refresh=refresh
        )

        entrants_df = self.get_entrants_frame(df)
        scores_df = self.get_scores_frame(df)

        self.dump_entrants_frame(entrants_df,comp_id=comp_id)
        self.dump_scores_frame(scores_df,comp_id=comp_id)
        return

    def get_workout_frames(
        self,
        comp_id: int,
        division_male: str,
        division_female: str,
        refresh: bool = False
    ):

        wo_list = [
            {
                'div_id': div_id,
                **d
            }
            for div_id in [division_male, division_female]
            for d in self.manager.load_workouts(
                comp_id=comp_id,
                div_id=div_id,
                refresh=refresh
            )
        ]

        wo_sch = [
            {
                'div_id': w['div_id'],
                'key': w['key'],
                **d
            }
            for w in wo_list
            for d in self.manager.load_workout_schedule(
                comp_id=comp_id,
                div_id=w['div_id'],
                workout_id=w['key']
            )
        ]

        wo_ids = set(w['key'] for w in wo_list)
        wo_desc = [
            self.manager.load_workout_description(
                comp_id=comp_id,
                workout_id=w
            )
            for w in wo_ids
        ]

        if len(wo_sch) > 0:
            ## schedule contains heat start times
            sch_df = pd.DataFrame(wo_sch)[['key','time']]\
                .rename(columns={'key':'workout_id','time':'start_time'})
        else:
            sch_df = pd.DataFrame()

        ## description contains workout name and description
        desc_df = pd.DataFrame(wo_desc)[['id','name','description','scheduleDate']]\
            .rename(columns={'id':'workout_id','name':'workout_name','scheduleDate':'date'})

        return sch_df, desc_df

    def parse_workouts(
        self,
        comp_id: int,
        division_male: str,
        division_female: str,
        refresh: bool = False
    ):
        sch_df, desc_df = self.get_workout_frames(
            comp_id=comp_id,
            division_male=division_male,
            division_female=division_female,
            refresh=refresh
        )

        ## the description frame has the date and the schdule has the time
        ## we need to get the year from the metadata and add it to the date
        metadata = self.manager.load_metadata(comp_id=comp_id)
        yr = pd.to_datetime(metadata['startDate']).year
        desc_df['date'] = desc_df['date'] + ', ' + str(yr)
        desc_df['date'] = pd.to_datetime(desc_df['date'],format='%A, %B %d, %Y')\
            .dt.strftime('%Y-%m-%d')

        if len(sch_df) > 0:
            heats = pd.merge(
                sch_df, 
                desc_df[['workout_id','date']],
                on='workout_id'
            )
            heats['start_time'] = heats['date'] + ' ' + heats['start_time']
            t1 = pd.to_datetime(heats['start_time'],
                        format='%Y-%m-%d %I:%M %p',errors='coerce')
            t2 = pd.to_datetime(heats['start_time'],
                        format='%Y-%m-%d %H:%M',errors='coerce')
            heats['start_time'] = t1.fillna(t2)
            
            ## we can get the end times by figureing out the time between each heat
            ## and then filling in the blanks with the minimum duration
            heats = heats.sort_values(by=['workout_id','start_time'])\
                .drop(columns=['date'])
            heats['end_time'] = heats.groupby('workout_id')['start_time'].shift(-1)
            heats['duration'] = heats['end_time'] - heats['start_time']
            heats['duration'] = heats['duration'].fillna(
                heats.groupby('workout_id')['duration'].transform('min')
            )

            heats['end_time'] = heats['start_time'] + heats['duration']

            heats = heats.groupby('workout_id').agg(
                start_time=('start_time','min'),
                end_time=('end_time','max')
            ).reset_index()

            heats['start_time'] = heats['start_time'].dt.strftime('%Y-%m-%d %H:%M')
            heats['end_time'] = heats['end_time'].dt.strftime('%Y-%m-%d %H:%M')

            df = pd.merge(
                desc_df,
                heats,
                on='workout_id',
                how='left'
            )
        else:
            df = desc_df

        df = df.assign(source_comp_id=str(comp_id))\
            .rename(columns={'workout_id':'source_workout_id'})\
                .reindex(columns=[
                    'source_comp_id','source_workout_id','workout_name',
                    'description','date','start_time','end_time'
                ]).sort_values(by=['date','source_workout_id'])\
                    .reset_index(drop=True)
        df['source_workout_id'] = df['source_workout_id'].astype(str)
        df['seq'] = df.index + 1

        self.dump_workouts_frame(df,comp_id=comp_id)
        return

class CrossFitParser(Parser):
    def __init__(self):
        super().__init__(
            manager=inventory.CrossFitInventoryManager
        )

    def build_parsed_blob_name(self, model_name: str, **kwargs):
        c = kwargs['comp_id']
        d = kwargs['div_id']
        p = kwargs['page']
        blob_name = f'{self.manager.source}/parsed/{model_name}_{c}_{d}_{p}.ndjson'
        blob_name = f'{self.manager.source}/parsed/comp={c}/division={d}/page={p}/{model_name}.ndjson'
        return blob_name

    def get_total_pages(
        self,
        **kwargs
    ):
        data = self.manager.load_leaderboard_page(
            **kwargs,page=1)
        return data['pagination']['totalPages']

    def get_leaderboard_page_frame(
        self, 
        page: int = 1,
        refresh: bool = False,
        **kwargs
    ):
        data = self.manager.load_leaderboard_page(
            **kwargs, page=page, refresh=refresh)
        comp = data['competition']
        df = pd.DataFrame(data['leaderboardRows'])\
            .rename(columns={
                'overallRank':'overall_rank',
                'overallScore':'overall_points'
            })
        df['source_comp_id'] = str(comp['competitionId'])

        ## get entrant data
        df = pd.merge(
            df[['source_comp_id','overall_rank','overall_points','scores']],
            df['entrant'].apply(pd.Series),
            left_index=True,
            right_index=True
        ).rename(columns={
            'competitorId':'source_athlete_id',
            'competitorName':'display_name'
        })
        df['overall_rank'] = pd.to_numeric(df['overall_rank'],errors='coerce')
        df['source_division_id'] = df['gender'].map({'M':'1','F':'2'})

        ## some leaderboards are copied from another source, so they didn't bother
        ## to link up the athletes with CF IDs. In that case, we'll generate one.
        if not 'source_athlete_id' in df.columns:
            df['source_athlete_id'] = \
                'C' + df['source_comp_id'] + '-' + \
                df['gender'] + '-' + \
                pd.util.hash_pandas_object(df['display_name']).astype(str) + \
                '-' + str(page)
        
        return df

    def get_entrants_frame(
        self, df: pd.DataFrame
    ):
        entrants_df = df.reindex(columns=[
            'source_comp_id','gender','source_athlete_id',
            'display_name','overall_rank','overall_points','status',
            'countryOfOriginCode','countryOfOriginName',
            'affiliateName','source_division_id',
            'age','height','weight'
        ]).rename(columns={'affiliateName':'home_gym'})

        entrants_df.loc[entrants_df['home_gym'].eq(''),'home_gym'] = None
        entrants_df.loc[entrants_df['height'].eq(''),'height'] = None
        entrants_df.loc[entrants_df['weight'].eq(''),'weight'] = None
        entrants_df['age'] = pd.to_numeric(entrants_df['age'],errors='coerce')

        ## map the country code
        cc1 = entrants_df['countryOfOriginCode'].apply(get_country_code)
        cc2 = entrants_df['countryOfOriginName'].apply(get_country_code)
        entrants_df['country_code'] = cc1.fillna(cc2)
        entrants_df = entrants_df.drop(
            columns=['countryOfOriginCode','countryOfOriginName']
        )
        entrants_df['wd'] = entrants_df['status'].astype(str)\
            .str.contains('WD',case=False).fillna(False)
        entrants_df['dnf'] = entrants_df['status'].astype(str)\
            .str.contains('DNF',case=False).fillna(False)
        entrants_df['dq'] = entrants_df['status'].astype(str)\
                .str.contains('DQ',case=False).fillna(False)
        entrants_df['cut'] = entrants_df['status'].astype(str)\
            .str.contains('CUT',case=False).fillna(False)
        if entrants_df['overall_points'].str.contains(':').all():
            entrants_df['overall_points'] = entrants_df['overall_points']\
                .str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1]))
        else:
            entrants_df['overall_points'] = pd.to_numeric(
                entrants_df['overall_points'],errors='coerce')
        entrants_df = entrants_df.drop(columns=['status'])

        entrants_df = entrants_df.reindex(
            columns=['source_comp_id','source_division_id','gender',
            'source_athlete_id','display_name','overall_score','overall_points',
            'dq','wd','dnf','cut','country_code','home_gym']
        )
        return entrants_df

    def get_scores_frame(
        self, df: pd.DataFrame
    ):
        scores_df = df[['source_comp_id','gender',
            'source_athlete_id','source_division_id','scores']]\
            .explode('scores',ignore_index=True)
        scores_df = pd.merge(
            scores_df[['source_comp_id','gender',
            'source_athlete_id','source_division_id']],
            scores_df['scores'].apply(pd.Series),
            left_index=True,
            right_index=True
        ).rename(columns={
            'ordinal':'source_workout_id',
            'scoreDisplay':'score_display'
        })
        if 'points' not in scores_df.columns:
            scores_df['points'] = scores_df['score']

        scores_df['source_workout_id'] = scores_df['source_workout_id'].astype(str)
        scores_df['rank'] = pd.to_numeric(scores_df['rank'],errors='coerce')
        scores_df['points'] = pd.to_numeric(scores_df['points'],errors='coerce')
        scores_df.loc[
            scores_df['score_display'].str.len().eq(0),
            'score_display'
        ] = '--'

        ## tiebreaker is sometimes found in the breakdown of the workout
        if 'breakdown' in scores_df.columns:
            scores_df['tiebreak_display'] = scores_df['breakdown']\
                .str.lower().str.extract('tiebreak: (.*)')
        else:
            scores_df['tiebreak_display'] = None

        scores_df = scores_df.reindex(columns=[
            'source_comp_id','gender','source_athlete_id',
            'source_division_id','source_workout_id',
            'score_display','tiebreak_display',
            'rank','points'
        ])

        return scores_df

    def parse_leaderboard_page(
        self, page: int = 1, refresh: bool = False, **kwargs
    ):
        df = self.get_leaderboard_page_frame(**kwargs,page=page,refresh=refresh)
        entrants_df = self.get_entrants_frame(df)
        scores_df = self.get_scores_frame(df)

        self.dump_entrants_frame(entrants_df,**kwargs)
        self.dump_scores_frame(scores_df,**kwargs)

        return

class LocalCompParser(Parser):
    def __init__(self):
        super().__init__(manager=inventory.LocalCompInventoryManager)

    def get_leaderboard_frame(
        self, 
        comp_id: int, 
        division_male: int, 
        division_female: int,
        refresh: bool = False
    ):
        df = super().get_leaderboard_frame(
            comp_id=comp_id,
            division_male=division_male,
            division_female=division_female,
            refresh=refresh
        )
        df = pd.merge(
            df[['gender','page']],
            df['data'].apply(pd.Series),
            left_index=True,
            right_index=True
        )
        return df

    def get_entrants_and_scores_frame(self,
        comp_id: int,
        division_male: int,
        division_female: int,
        refresh: bool = False
    ):
        df = self.get_leaderboard_frame(
            comp_id=comp_id,
            division_male=division_male,
            division_female=division_female,
            refresh=refresh
        )

        entrants_df = df[['comp_id','gender','division_id','entrants']]\
            .explode('entrants',ignore_index=True)
        entrants_df = pd.merge(
            entrants_df[['comp_id','division_id','gender']],
            entrants_df['entrants'].apply(pd.Series),
            left_index=True,
            right_index=True
        ).rename(columns={
            'comp_id': 'source_comp_id',
            'division_id': 'source_division_id',
            'id': 'source_athlete_id',
            'name': 'display_name',
            'rank': 'overall_rank',
            'points': 'overall_points',
            'gym': 'home_gym'
        }).astype({
            'source_comp_id': str,
            'source_division_id': str,
            'source_athlete_id': str,
        })

        entrants_df['overall_points'] = pd.to_numeric(entrants_df['overall_points'],errors='coerce')
        entrants_df['overall_rank'] = pd.to_numeric(entrants_df['overall_rank'],errors='coerce')
        entrants_df.loc[entrants_df['home_gym'].eq(''),'home_gym'] = None

        scores_df = df[['comp_id','division_id','gender','scores']]\
            .explode('scores',ignore_index=True)
        scores_df = pd.merge(
            scores_df[['comp_id','division_id','gender']],
            scores_df['scores'].apply(pd.Series),
            left_index=True,
            right_index=True
        ).rename(columns={
            'comp_id': 'source_comp_id',
            'id': 'source_athlete_id',
            'division_id': 'source_division_id',
            'enum': 'source_workout_id'
        }).astype(
            {
                'source_comp_id': str,
                'source_division_id': str,
                'source_athlete_id': str,
                'source_workout_id': str
            }
        )

        scores_df['rank_raw'] = scores_df['results'].apply(
            lambda x: x[0] if len(x) > 0 else None
        )
        scores_df['rank_numeric'] = pd.to_numeric(scores_df['rank_raw'],errors='coerce')

        scores_df['score_display'] = scores_df['results'].apply(
            lambda x: x[1] if len(x) > 1 else None
        )

        scores_df[['points','rank']] = scores_df['rank_raw'].str.extract(
            r'(?P<points>\d+\.\d+)\s+\((?P<rank>\d+)\)'
        )

        scores_df['rank'] = scores_df['rank'].fillna(scores_df['rank_numeric'])

        scores_df['score_display'] = scores_df['score_display'].fillna('--')

        return entrants_df, scores_df

    def parse_leaderboard(
        self, refresh: bool = False, **kwargs
    ):
        entrants_df, scores_df = self.get_entrants_and_scores_frame(
            refresh=refresh,
            **kwargs
        )
        self.dump_entrants_frame(entrants_df,**kwargs)
        self.dump_scores_frame(scores_df,**kwargs)
        return

class Circle21Parser(Parser):
    def __init__(self):
        super().__init__(manager=inventory.Circle21InventoryManager)
        gcp_params = GoogleCloudParameters()
        self.geolocator = GoogleV3(api_key=gcp_params.maps_api_key)

    def parse_metadata(self, comp_id: str):
        data = self.manager.load_metadata(comp_id=comp_id)
        
        meta = {
            'source_comp_id': comp_id, 
            'title': data['name'],
            'start_date': data['date_from'],
            'end_date': data['date_to'],
            'virtual': not bool(data['onsite'])
        }
        
        place_id = data.get('location')
        if place_id:
            loc = self.geolocator.geocode(place_id=place_id)
            meta['address'] = loc.address
            meta['lat'] = loc.latitude
            meta['lng'] = loc.longitude

        self.dump_metadata(Metadata(**meta), comp_id=comp_id)
        return

class CaptureFitParser(Parser):
    def __init__(self):
        super().__init__(manager=inventory.CaptureFitInventoryManager)

    def get_leaderboard_frame(
        self,**kwargs
    ):
        df = super().get_leaderboard_frame(**kwargs)
        df = pd.merge(
            df[['comp_id','div_id','gender']],
            df['data'].apply(pd.Series)['leaderboard'],
            left_index=True,
            right_index=True
        ).explode('leaderboard', ignore_index=True)

        df = pd.merge(
            df.drop(columns=['leaderboard']),
            df['leaderboard'].apply(pd.Series)\
                .drop(columns=['gender']),
            left_index=True,
            right_index=True
        ).drop(columns=[])

        df = pd.merge(
            df.drop(columns=['evententrydisplay',
            '_id','event','evententry','entrynumber']),
            df['evententrydisplay'].apply(pd.Series),
            left_index=True,
            right_index=True
        ).rename(columns={
            'comp_id':'source_comp_id',
            'div_id':'source_division_id',
            'user':'source_athlete_id',
            'entrynumber':'source_entrant_id'
        })
        return df

    @staticmethod
    def get_entrants_frame(df):
        entrants_df = df.rename(columns=
            {
                'name':'display_name',
                'total':'overall_points',
                'position':'overall_rank',
                'gymname':'home_gym',
                'country':'country_code'
            }
        ).reindex(
            columns=['source_comp_id','source_division_id','gender',
            'source_athlete_id','source_entrant_id','display_name',
            'overall_points','overall_rank','home_gym','country_code']
        )

        entrants_df['country_code'] = entrants_df['country_code']\
            .fillna('').apply(get_country_code)
        
        return entrants_df

    @staticmethod
    def get_scores_frame(df):
        scores_df = df.reindex(columns=[
            'source_comp_id','source_division_id','gender',
            'source_athlete_id','source_entrant_id','scores'
        ]).explode('scores',ignore_index=True)

        scores_df = pd.merge(
            scores_df.drop(columns=['scores']),
            scores_df['scores'].apply(pd.Series),
            left_index=True,
            right_index=True
        )

        scores_df['source_workout_id'] = scores_df['_id'].astype(str)
        scores_df['seq'] = scores_df['workoutnumber'].astype(int)
        scores_df['score_display'] = scores_df['time'].fillna('--')
        scores_df['rank'] = pd.to_numeric(scores_df['position'],errors='coerce')
        scores_df['tiebreak_display'] = scores_df['tiebreaker'].apply(
            lambda x: str(int(x)) if not pd.isna(x) else x
        )

        scores_df = scores_df.reindex(
            columns=[
                'source_comp_id','source_division_id','gender',
                'source_athlete_id','source_entrant_id','source_workout_id',
                'score_display','rank','points','tiebreak_display'
            ]
        ).dropna(subset=['source_workout_id'])
        return scores_df
        
    def get_workout_frame(self,**kwargs):
        df =super().get_leaderboard_frame(**kwargs)

        name_df = df['data'].apply(pd.Series)['leaderboard']\
            .explode().apply(pd.Series)['scores']\
                .explode().apply(pd.Series)[['workoutname','workoutnumber']]\
                    .drop_duplicates().dropna()\
                        .rename(columns={
                            'workoutname':'workout_name',
                            'workoutnumber':'seq'
                        })
        name_df['seq'] = name_df['seq'].astype(int)

        wo_df = df['data'].apply(pd.Series)['eventworkout'].apply(pd.Series)\
            ['workouts'].explode().apply(pd.Series)\
                .reindex(columns=['_id','name','content','heatstart'])\
                    .drop_duplicates()\
            .rename(columns={
                '_id':'source_workout_id',
                'name':'workout_name',
                'content':'description'
            })

        wo_df = pd.merge(
            wo_df, name_df,
            on=['workout_name'],
            how='left'
        ).assign(
            source_comp_id=kwargs['comp_id']
        )

        return wo_df

    def parse_workouts(self,**kwargs):
        wo_df = self.get_workout_frame(**kwargs)
        self.dump_workouts_frame(wo_df,**kwargs)
        return

    def parse_leaderboard(self,**kwargs):
        df = self.get_leaderboard_frame(**kwargs)
        entrants_df = self.get_entrants_frame(df)
        scores_df = self.get_scores_frame(df)
        self.dump_entrants_frame(entrants_df,**kwargs)
        self.dump_scores_frame(scores_df,**kwargs)
        return

    def parse_metadata(self,**kwargs):
        meta = self.manager.load_metadata(**kwargs)
        meta_df = pd.DataFrame(meta)
        meta_df[['start_date','end_date']] = meta_df['dates'].str.split(' - ', expand=True)
        meta_df['start_date'] = pd.to_datetime(meta_df['start_date'])
        meta_df['end_date'] = pd.to_datetime(meta_df['end_date'])

        meta_df = meta_df[meta_df['comp_id'] == kwargs['comp_id']]\
            .rename(columns={'comp_id':'source_comp_id'})

        wo_df = self.get_workout_frame(**kwargs)
        meta_df['virtual'] = wo_df['heatstart'].isna().all()

        meta = Metadata(**meta_df.to_dict(orient='records')[0])
        self.dump_metadata(meta, kwargs['comp_id'])
        return
        
