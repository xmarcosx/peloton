import os

import peloton_user
import peloton_workout

USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']

user = peloton_user.PelotonUser(USERNAME, PASSWORD)

workout_ids = user.get_workout_ids()
workouts = [peloton_workout.PelotonWorkout(user, x) for x in workout_ids]

