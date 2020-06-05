import os

import log
import peloton_user
import peloton_workout

USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']

# configure logging
logger = log.setup_custom_logger('peloton')

# create peloton user and authenticate
user = peloton_user.PelotonUser(USERNAME, PASSWORD)

# fetch list of workout ids
workout_ids = user.get_workout_ids()

# workouts = [peloton_workout.PelotonWorkout(user, x) for x in workout_ids]
