import os

import log
import peloton_instructor
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

# create workout objects
workouts = [peloton_workout.PelotonWorkout(user, workout_id) for workout_id in workout_ids]

instructors = dict()
# iterate through workouts
for workout in workouts:
    # if we are not already storing instructor information
    if workout.instructor_id not in instructors.keys():
        instructors['instructor_id'] = peloton_instructor.PelotonInstructor(workout.instructor_id)

# pull workout summary data
#[workout.get_workout_summary() for workout in workouts]

# pull performance graph data
[workout.get_performance_graph() for workout in workouts]
