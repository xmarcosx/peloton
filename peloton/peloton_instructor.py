import logging

import requests


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
        
        self.logger.info(f'Successfully fetched instructor id {self.instructor_id}')

        resp_json = resp.json()

        self.name = resp_json['name']
        self.logger.debug(f'Set name to {self.name}')

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
