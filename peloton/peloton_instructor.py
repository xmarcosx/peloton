import logging

import requests

from google.cloud import bigquery
import pandas as pd


class PelotonInstructor:

    _base_url = 'https://api.onepeloton.com'
    _headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'peloton'
        }

    # constructor
    def __init__(self, instructor_id):

        self.instructor_id = instructor_id
        self.logger = logging.getLogger('peloton')
        self.session = requests.Session()
        
        instructor_url = f'{self._base_url}/api/instructor/{self.instructor_id}'

        resp = self.session.get(instructor_url)
        

        if resp.status_code != 200:
            logging.error(f'Failed to fetch instructor id {self.instructor_id}')
            raise ValueError(f'Failed to fetch instructor id {self.instructor_id}') 
        
        self.logger.debug(f'Successfully fetched instructor id {self.instructor_id}')

        resp_json = resp.json()

        self.display_name = resp_json['name']
        self.logger.debug(f'Set display_name to {self.display_name}')

        self.first_name = resp_json['first_name']
        self.logger.debug(f'Set first_name to {self.first_name}')

        self.last_name = resp_json['last_name']
        self.logger.debug(f'Set last_name to {self.last_name}')

        self.spotify_playlist_uri = resp_json['spotify_playlist_uri']
        self.logger.debug(f'Set spotify_playlist_uri to {self.spotify_playlist_uri}')

        self.image_url = resp_json['image_url']
        self.logger.debug(f'Set image_url to {self.image_url}')

        self.fitness_disciplines = resp_json['fitness_disciplines']
        self.logger.debug(f'Set fitness_disciplines to {self.fitness_disciplines}')


    def get_bigquery_job_config(self):
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField('instructor_id', 'STRING'),
                bigquery.SchemaField('last_name', 'STRING'),
                bigquery.SchemaField('first_name', 'STRING'),
                bigquery.SchemaField('display_name', 'STRING'),
                bigquery.SchemaField('spotify_playlist_uri', 'STRING'),
                bigquery.SchemaField('image_url', 'STRING'),
            ],
            write_disposition='WRITE_TRUNCATE'
        )

        return job_config


    def to_df(self):
        output = {
            'instructor_id': [self.instructor_id],
            'last_name': [self.last_name],
            'first_name': [self.first_name],
            'display_name': [self.display_name],
            'spotify_playlist_uri': [self.spotify_playlist_uri],
            'image_url': [self.image_url],
        }

        df = pd.DataFrame(output)

        return df

