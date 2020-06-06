import logging

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
        self.logger = logging.getLogger('peloton')
        
        workout_url = f'{self._base_url}/api/workout/{self.workout_id}'
        resp = peloton_user.session.get(workout_url)

        if resp.status_code != 200:
            logging.error(f'Failed to fetch workout id {self.workout_id}')
            raise ValueError(f'Failed to fetch workout id {self.workout_id}') 
        
        self.logger.info(f'Successfully fetched workout id {self.workout_id}')

        resp_json = resp.json()

        self.created_at_epoch = resp_json['created_at']
        self.logger.debug(f'Set created_at to {self.created_at_epoch}')

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

        self.status = resp_json['status']
        self.logger.debug(f'Set status to {self.status}')

    def get_workout_summary(self):

        workout_url = f'{self._base_url}/api/workout/{self.workout_id}/summary'

        resp = self.peloton_user.session.get(workout_url)

        if resp.status_code != 200:
            raise ValueError(f'Failed to get summary workout data for id {self.workout_id}') 
        
        self.logger.info(f'Successfully fetched summary workout data for id {self.workout_id}')

        resp_json = resp.json()

        self.workout_summary = dict()
        self.workout_summary['calories'] = resp_json['calories']
        self.logger.debug(f'Set summary.calories to {self.workout_summary["calories"]}')

        # heart rate
        self.workout_summary['avg_heart_rate'] = resp_json['avg_heart_rate']
        self.logger.debug(f'Set summary.avg_heart_rate to {self.workout_summary["avg_heart_rate"]}')

        self.workout_summary['max_heart_rate'] = resp_json['max_heart_rate']
        self.logger.debug(f'Set summary.max_heart_rate to {self.workout_summary["max_heart_rate"]}')

        # resistance
        self.workout_summary['avg_resistance'] = resp_json['avg_resistance']
        self.logger.debug(f'Set summary.avg_resistance to {self.workout_summary["avg_resistance"]}')

        self.workout_summary['max_resistance'] = resp_json['max_resistance']
        self.logger.debug(f'Set summary.max_resistance to {self.workout_summary["max_resistance"]}')


    def get_performance_graph(self):

        performance_graph_url = f'{self._base_url}/api/workout/{self.workout_id}/performance_graph'

        resp = self.peloton_user.session.get(performance_graph_url)

        if resp.status_code != 200:
            raise ValueError(f'Failed to get performance graph for workout id {self.workout_id}') 
        
        self.logger.info(f'Successfully fetched performance graph for id {self.workout_id}')

        resp_json = resp.json()

        self.performance_graph = dict()

        # metrics including heart rate zones
        self.performance_graph['heart_rate_zones'] = next((item for item in resp_json['metrics'] if resp_json['metrics']['display_name'] == 'Heart Rate'), None)
        self.logger.debug(f'Set performance_graph.heart_rate_zones')
