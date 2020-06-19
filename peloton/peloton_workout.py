import logging

import requests

from google.cloud import bigquery
import pandas as pd

import peloton_user


class PelotonWorkout:

    _base_url = 'https://api.onepeloton.com'
    _headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'peloton'
        }

    # constructor
    def __init__(self, peloton_user, workout_id):

        self.peloton_user = peloton_user
        self.workout_id = workout_id
        self.logger = logging.getLogger('peloton')
        
        workout_url = f'{self._base_url}/api/workout/{self.workout_id}'
        resp = peloton_user.session.get(workout_url)

        if resp.status_code != 200:
            logging.error(f'Failed to fetch workout id {self.workout_id}')
            raise ValueError(f'Failed to fetch workout id {self.workout_id}') 
        
        self.logger.debug(f'Successfully fetched workout id {self.workout_id}')

        resp_json = resp.json()

        self.created_at_epoch = resp_json['created_at']
        self.logger.debug(f'Set created_at to {self.created_at_epoch}')

        self.is_total_work_personal_record = resp_json['is_total_work_personal_record']
        self.logger.debug(f'Set is_total_work_personal_record to {self.is_total_work_personal_record}')

        self.device_type = resp_json['device_type']
        self.logger.debug(f'Set device_type to {self.device_type}')

        self.difficulty_estimate = resp_json['ride']['difficulty_estimate']
        self.logger.debug(f'Set ride\'s difficulty_estimate to {self.difficulty_estimate}')

        self.duration = resp_json['ride']['duration']
        self.logger.debug(f'Set ride\'s duration to {self.duration}')

        self.fitness_discipline = resp_json['fitness_discipline']
        self.logger.debug(f'Set ride\'s fitness_discipline to {self.fitness_discipline}')

        self.instructor_id = resp_json['ride']['instructor_id']
        self.logger.debug(f'Set ride\'s instructor_id to {self.instructor_id}')

        self.user_id = resp_json['user_id']
        self.logger.debug(f'Set user_id to {self.user_id}')

        self.status = resp_json['status']
        self.logger.debug(f'Set status to {self.status}')

        self.ride_id = resp_json['ride']['id']
        self.logger.debug(f'Set ride id to {self.ride_id}')

        self.ride_title = resp_json['ride']['title']
        self.logger.debug(f'Set ride title to {self.ride_title}')


    def get_workout_summary(self):

        workout_url = f'{self._base_url}/api/workout/{self.workout_id}/summary'

        resp = self.peloton_user.session.get(workout_url)

        if resp.status_code != 200:
            raise ValueError(f'Failed to get summary workout data for id {self.workout_id}') 
        
        self.logger.debug(f'Successfully fetched summary workout data for id {self.workout_id}')

        resp_json = resp.json()

        # calories
        self.calories = resp_json['calories']
        self.logger.debug(f'Set summary.calories to {self.calories}')

        # heart rate
        self.avg_heart_rate = resp_json['avg_heart_rate']
        self.logger.debug(f'Set summary.avg_heart_rate to {self.avg_heart_rate}')

        self.max_heart_rate = resp_json['max_heart_rate']
        self.logger.debug(f'Set summary.max_heart_rate to {self.max_heart_rate}')

        # resistance
        self.avg_resistance = resp_json['avg_resistance']
        self.logger.debug(f'Set summary.avg_resistance to {self.avg_resistance}')

        self.max_resistance = resp_json['max_resistance']
        self.logger.debug(f'Set summary.max_resistance to {self.max_resistance}')

        # speed
        self.avg_speed = resp_json['avg_speed']
        self.logger.debug(f'Set summary.avg_speed to {self.avg_speed}')

        self.max_speed = resp_json['max_speed']
        self.logger.debug(f'Set summary.max_speed to {self.max_speed}')

        # total work
        self.total_work_joule = resp_json['total_work']
        self.logger.debug(f'Set total work to {self.total_work_joule}')

        # distance miles
        self.distance_miles = resp_json['distance']
        self.logger.debug(f'Set distance to {self.distance_miles}')


    def get_performance_graph_df(self):

        performance_graph_url = f'{self._base_url}/api/workout/{self.workout_id}/performance_graph'

        resp = self.peloton_user.session.get(performance_graph_url)

        if resp.status_code != 200:
            raise ValueError(f'Failed to get performance graph for workout id {self.workout_id}') 
        
        self.logger.debug(f'Successfully fetched performance graph for id {self.workout_id}')

        resp_json = resp.json()

        performance_graph = next((metric for metric in resp_json['metrics'] if metric['display_name'] == 'Heart Rate'), None)

        # running pace
        self.avg_pace_dict = next((summary for summary in resp_json['average_summaries'] if summary['display_name'] == 'Avg Pace'), None)
        
        if self.avg_pace_dict is not None:
            self.avg_pace = self.avg_pace_dict['value']
        else:
            self.avg_pace = 0.0

        output = {
            'workout_id': [],
            'display_name': [],
            'range': [],
            'minimum_value': [],
            'maximum_value': [],
            'duration_seconds': [],
        }

        if performance_graph is not None:
            # if performance_graph has key 'zones'
            if 'zones' in performance_graph:
                # if value of zones is not None
                #if performance_graph['zones'] is not None:
                # iterate through each zone
                for zone in performance_graph['zones']:
                    if 'duration' in zone:
                        output['workout_id'].append(self.workout_id)
                        output['display_name'].append(zone['display_name'])
                        output['range'].append(zone['range'])
                        output['minimum_value'].append(zone['min_value'])
                        output['maximum_value'].append(zone['max_value'])
                        output['duration_seconds'].append(zone['duration'])

        df = pd.DataFrame(output)

        if len(df.index) > 0:
            return df
        else:
            return None


    def get_bigquery_job_config(self):
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField('workout_id', 'STRING'),
                bigquery.SchemaField('user_id', 'STRING'),
                bigquery.SchemaField('ride_id', 'STRING'),
                bigquery.SchemaField('created_at', 'TIMESTAMP'),
                bigquery.SchemaField('fitness_discipline', 'STRING'),
                bigquery.SchemaField('total_work_joule', 'FLOAT'),
                bigquery.SchemaField('total_calories', 'FLOAT'),
                bigquery.SchemaField('distance_miles', 'FLOAT'),
                bigquery.SchemaField('average_pace', 'FLOAT'),
                bigquery.SchemaField('average_speed', 'FLOAT'),
                bigquery.SchemaField('maximum_speed', 'FLOAT'),
                bigquery.SchemaField('average_heart_rate', 'FLOAT'),
                bigquery.SchemaField('maximum_heart_rate', 'FLOAT'),
                bigquery.SchemaField('is_total_work_personal_record', 'BOOLEAN'),
                bigquery.SchemaField('status', 'STRING'),
            ],
            write_disposition='WRITE_TRUNCATE'
        )

        return job_config


    def to_df(self):
        output = {
            'workout_id': [self.workout_id],
            'user_id': [self.peloton_user.userid],
            'ride_id': [self.ride_id],
            'created_at': [pd.to_datetime(self.created_at_epoch, unit='s')],
            'fitness_discipline': [self.fitness_discipline.capitalize()],
            'total_work_joule': [self.total_work_joule],
            'total_calories': [self.calories],
            'distance_miles': [self.distance_miles],
            'average_pace': [self.avg_pace],
            'average_speed': [self.avg_speed],
            'maximum_speed': [self.max_speed],
            'average_heart_rate': [self.avg_heart_rate],
            'maximum_heart_rate': [self.max_heart_rate],
            'is_total_work_personal_record': [self.is_total_work_personal_record],
            'status': [self.status],
        }

        df = pd.DataFrame(output)

        return df

