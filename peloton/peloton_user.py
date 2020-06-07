import logging
import math

import requests


class PelotonUser:

    _base_url = 'https://api.onepeloton.com'
    _headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'peloton'
        }

    # constructor
    def __init__(self, username, password):
        
        self.username = username
        self.password = password
        self.logger = logging.getLogger('peloton')
        self.cycling_ftp = None
        self.email = None
        self.last_workout_epoch = None
        self.name = None
        self.userid = None
        self.workout_ids = None

        self.session = requests.Session()
        self.__login()


    def __login(self):
        
        auth_login_url = f'{self._base_url}/auth/login'
        auth_payload = {
            'username_or_email': self.username,
            'password': self.password
        }

        resp = self.session.post(auth_login_url, json=auth_payload, headers=self._headers)

        if resp.status_code != 200:
            logging.error(f'Failed to login using {self.username}')
            raise ValueError('Failed to login using supplied credentials') 

        self.logger.info(f'Successfully logged in as {self.username}')
        
        resp_json = resp.json()
        
        self.userid = resp_json['user_id']
        self.logger.debug(f'Set userid to {self.userid}')

        if 'user_data' in resp_json:
            self.cycling_ftp = resp_json['user_data']['cycling_ftp']
            self.logger.debug(f'Set cycling_ftp to {self.cycling_ftp}')

            self.email = resp_json['user_data']['email']
            self.logger.debug(f'Set email to {self.email}')

            self.last_workout_epoch = resp_json['user_data']['last_workout_at']
            self.logger.debug(f'Set last_workout_epoch to {self.last_workout_epoch}')

            self.name = resp_json['user_data']['name']
            self.logger.debug(f'Set name to {self.name}')

            self.total_workouts = resp_json['user_data']['total_workouts']
            self.logger.debug(f'Set total_workouts to {self.total_workouts}')


    def get_workout_ids(self):

        base_workout_url = f'{self._base_url}/api/user/{self.userid}/workouts?sort_by=-created'

        workout_list = list()
        page = 0
        total_pages = math.ceil(self.total_workouts / 100)

        while page < total_pages:
            workout_url = f'{base_workout_url}&page={page}&limit=100'
            resp = self.session.get(workout_url)

            if resp.status_code != 200:
                raise Exception('Failed to fetch workout data') 
        
            resp_json = resp.json()
            workout_list.extend(resp_json['data'])

            page += 1

        # creates list of only the workout ids
        self.workout_ids = [x['id'] for x in workout_list]

        self.logger.info(f'Returning {len(self.workout_ids)} workout ids')

        return self.workout_ids
