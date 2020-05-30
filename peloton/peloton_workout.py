import requests

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
        
        workout_url = f'{self._base_url}/api/workout/{self.workout_id}'
        resp = peloton_user.session.get(workout_url)

        if resp.status_code != 200:
            raise ValueError('Failed to login using supplied credentials') 
        
        resp_json = resp.json()

        self.created_at_epoch = resp_json['created_at']
        self.device_type = resp_json['device_type']
        self.difficulty_estimate = resp_json['ride']['difficulty_estimate']
        self.duration = resp_json['ride']['duration']
        self.fitness_discipline = resp_json['fitness_discipline']
        self.instructor_id = resp_json['ride']['instructor_id']
        self.status = resp_json['status']

    def get_workout_summary(self):

        workout_url = f'{self._base_url}/api/workout/{self.workout_id}/summary'

        resp = self.peloton_user.session.get(workout_url)

        if resp.status_code != 200:
            raise ValueError('Failed to login using supplied credentials') 
        
        resp_json = resp.json()
