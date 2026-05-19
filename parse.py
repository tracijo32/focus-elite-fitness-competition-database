from pydantic import BaseModel
import inventory
import pandas as pd
from models import Entrant, Score, Metadata
from util import convert_value_to_display

class Parser:
    def __init__(self, manager: inventory.InventoryManager):
        self.manager = manager()

    def save_model_to_ndjson(
        self, 
        models: list[BaseModel],
        model_name: str,
    ):
        model_type = type(models[0])
        assert all(isinstance(m, model_type) for m in models)

        model_json = "\n".join([m.model_dump_json() for m in models])
        blob_name = f'{self.manager.source}/parsed/{model_name}.ndjson'
        blob = self.manager.bucket.blob(blob_name)
        blob.upload_from_string(model_json)

class StrongestParser(Parser):
    def __init__(self):
        super().__init__(manager=inventory.StrongestInventoryManager)

    def get_policy_frame(self, comp_id: str):
        scoring_policies = self.manager.load_scoring_policies(comp_id=comp_id)
        sp_df = pd.DataFrame(scoring_policies['data']).assign(comp_id=comp_id)\
            [['comp_id','id','division','workout','scoreType',
            'tiebreakerScoreType','tiebreaker2ScoreType','customPointsTable']]\
            .rename(columns={
                'division': 'div_id',
                'workout': 'workout_id'
            })
        return sp_df

    def get_workout_frame(self, comp_id: str):
        workouts = self.manager.load_workouts(comp_id=comp_id)
        wo_df = pd.DataFrame(workouts['data']).assign(comp_id=comp_id)\
            [['comp_id','id','title']]\
            .rename(columns={
                'id': 'workout_id',
                'title': 'workout_name'
            })
        return wo_df

    def get_leaderboard_frame(
        self, 
        comp_id: str, 
        division_male: str, 
        division_female: str
    ):

        lb = [
            self.manager.load_leaderboard(
                comp_id=comp_id,
                **d
            )
            for d in [
                {'div_id': division_male, 'gender': 'M'},
                {'div_id': division_female, 'gender': 'F'}
            ]
        ]

        df = pd.DataFrame([p for page in lb for p in page])
        df = pd.merge(
            df[['comp_id','div_id','gender']],
            df['data'].apply(pd.Series)['data'].apply(pd.Series),
            left_index=True,
            right_index=True
        )

        df = df[['comp_id','gender','div_id','body_rows']]\
            .explode('body_rows',ignore_index=True)

        return df

    def parse_leaderboard(
        self,
        comp_id: str,
        division_male: str,
        division_female: str
    ):
        ## load all the data
        df = self.get_leaderboard_frame(
            comp_id=comp_id,
            division_male=division_male,
            division_female=division_female
        )
        sp_df = self.get_policy_frame(comp_id=comp_id)
        wo_df = self.get_workout_frame(comp_id=comp_id)

        ## create a dataframe of the entrants
        ## has entrant name, overall rank, and overall points
        entrants_df = pd.merge(
            df[['comp_id','gender']],
            df['body_rows'].apply(lambda x: x[0]).apply(pd.Series),
            left_index=True,
            right_index=True
        ).reindex(
            columns = ['comp_id','gender','competitor_name',
            'overall','registrationId','cum_workout_rank'])\
        .rename(columns={
            'overall': 'overall_rank',
            'cum_workout_rank': 'overall_points',
            'comp_id': 'source_comp_id',
            'registrationId': 'source_athlete_id',
            'competitor_name': 'display_name'
        })

        entrants_df['overall_rank'] = entrants_df['overall_rank'].str.extract(r'(\d+)')\
            .apply(pd.to_numeric, errors='coerce')
        entrants_df['overall_points'] = pd.to_numeric(
            entrants_df['overall_points'],
            errors='coerce'
        )

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
            scores_df[['comp_id','gender','source_athlete_id',
            'div_id']],
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

        scores_df = scores_df.rename(columns=
        {'comp_id':'source_comp_id','workout_id':'source_workout_id'})

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
            'tiebreak_display']
        )

        ## convert the entrants and scores to pydantic models
        entrants = [
            Entrant(**row.dropna().to_dict()) 
            for _, row in entrants_df.iterrows()
        ]
        scores = [
            Score(**row.dropna().to_dict())
            for _, row in scores_df.iterrows()
        ]

        ## upload the models to the inventory
        self.save_model_to_ndjson(
            models=entrants,
            model_name='entrants'
        )
        self.save_model_to_ndjson(
            models=scores,
            model_name='scores'
        )

        return

    def parse_metadata(self,comp_id: str):
        data = self.manager.load_metadata(comp_id=comp_id)['data']

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

        meta = [Metadata(**kwargs)]
        self.save_model_to_ndjson(
            models=meta,
            model_name='metadata'
        )
        return

class ScoreItParser(Parser):
    def __init__(self):
        super().__init__(manager=inventory.ScoreItInventoryManager)
        from parameters import GoogleCloudParams
        from geopy.geocoders import GoogleV3
        gcp_params = GoogleCloudParams()
        self.geolocator = GoogleV3(api_key=gcp_params.maps_api_key)

    def parse_metadata(self, comp_id):
        data = self.manager.load_metadata(comp_id=comp_id)

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

        meta = [Metadata(**kwargs)]
        self.save_model_to_ndjson(
            models=meta,
            model_name='metadata'
        )
        return

    def get_leaderboard_frame(
        self,
        comp_id: str,
        division_male: str,
        division_female: str    
    ):
        lb = [
            self.manager.load_leaderboard(
                comp_id=comp_id,
                **d
            )
            for d in [
                {'div_id': division_male, 'gender': 'M'},
                {'div_id': division_female, 'gender': 'F'}
            ]
        ]
        df = pd.DataFrame([p for page in lb for p in page])

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
                'teamName':'display_name',
                'teamRef': 'source_athlete_id',
                'totalPoints': 'overall_points',
                'position': 'overall_rank'
            }
        )
        
        return df

    def parse_leaderboard(
        self,
        comp_id: str,
        division_male: str,
        division_female: str
    ):
        df = self.get_leaderboard_frame(
            comp_id=comp_id,
            division_male=division_male,
            division_female=division_female
        )

        ## parse entrants
        entrants_df = df.reindex(
            columns=['source_comp_id','gender','source_athlete_id',
            'display_name','overall_points','overall_rank']
        )

        scores_df = pd.merge(
            df[['source_comp_id','gender','source_athlete_id']],
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
            'source_comp_id','gender','source_athlete_id','source_workout_id',
            'score_display','tiebreak_display','rank','points'
        ])

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
        self.save_model_to_ndjson(
            models=entrants,
            model_name='entrants'
        )
        self.save_model_to_ndjson(
            models=scores,
            model_name='scores'
        )
        return
