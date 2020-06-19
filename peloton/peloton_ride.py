import logging

import requests

from google.cloud import bigquery
import pandas as pd

import peloton_user


class PelotonRide:

    _base_url = 'https://api.onepeloton.com'
    _headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'peloton'
        }

    # constructor
    def __init__(self, peloton_user, ride_id):

        self.peloton_user = peloton_user
        self.ride_id = ride_id
        self.logger = logging.getLogger('peloton')
        
        ride_url = f'{self._base_url}/api/ride/{self.ride_id}/details'
        resp = peloton_user.session.get(ride_url)

        if resp.status_code != 200:
            logging.error(f'Failed to fetch ride id {self.ride_id}')
            raise ValueError(f'Failed to fetch ride id {self.ride_id}') 
        
        self.logger.debug(f'Successfully fetched ride id {self.ride_id}')

        resp_json = resp.json()

        self.instructor_id = resp_json['ride']['instructor_id']
        self.logger.debug(f'Set instructor_id to {self.instructor_id}')

        self.ride_type_id = resp_json['ride']['ride_type_id']
        self.logger.debug(f'Set ride_type_id to {self.ride_type_id}')

        self.title = resp_json['ride']['title']
        self.logger.debug(f'Set title to {self.title}')

        self.description = resp_json['ride']['description']
        self.logger.debug(f'Set description to {self.description}')

        # duration in seconds
        self.duration = resp_json['ride']['duration']
        self.logger.debug(f'Set duration to {self.duration}')

        self.fitness_discipline = resp_json['ride']['fitness_discipline_display_name']
        self.logger.debug(f'Set fitness_discipline to {self.fitness_discipline}')

        self.difficulty_estimate = resp_json['ride']['difficulty_estimate']
        self.logger.debug(f'Set difficulty_estimate to {self.difficulty_estimate}')


    def get_bigquery_job_config(self):
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField('ride_id', 'STRING'),
                bigquery.SchemaField('ride_type_id', 'STRING'),
                bigquery.SchemaField('instructor_id', 'STRING'),
                bigquery.SchemaField('title', 'STRING'),
                bigquery.SchemaField('description', 'STRING'),
                bigquery.SchemaField('duration_minutes', 'INTEGER'),
                bigquery.SchemaField('fitness_discipline', 'STRING'),
            ],
            write_disposition='WRITE_TRUNCATE'
        )

        return job_config


    def to_df(self):
        output = {
            'ride_id': [self.ride_id],
            'ride_type_id': [self.ride_type_id],
            'instructor_id': [self.instructor_id],
            'title': [self.title],
            'description': [self.description],
            'duration_minutes': [self.duration / 60],
            'fitness_discipline': [self.fitness_discipline],
        }

        df = pd.DataFrame(output)

        return df

