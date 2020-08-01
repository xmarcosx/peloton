# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import os

from google.cloud import bigquery
from google.cloud import secretmanager
import pandas as pd

import log
import peloton_instructor
import peloton_user
import peloton_ride
import peloton_workout

USERNAME=os.environ['USERNAME']
GCP_PROJECT_ID=os.environ['GCP_PROJECT_ID']
SECRET_MANAGER=os.environ['SECRET_MANAGER']
BIGQUERY_DATASET=os.environ['BIGQUERY_DATASET']

# configure logging
logger = log.setup_custom_logger('peloton')

# initialize bigquery
client = bigquery.Client()

# initalize secret manager and access secret
manager = secretmanager.SecretManagerServiceClient()
name = manager.secret_version_path(GCP_PROJECT_ID, SECRET_MANAGER, 'latest')
response = manager.access_secret_version(name)
PASSWORD = response.payload.data.decode('UTF-8')

# %% [markdown]
# # Users
# In the section below we create the Peloton user, authenticate with the API, and send user data to a table in BigQuery.

# %%

user = peloton_user.PelotonUser(USERNAME, PASSWORD)


# %%
table_id = f'{BIGQUERY_DATASET}.users'

job = client.load_table_from_dataframe(user.to_df(), table_id, job_config=user.get_bigquery_job_config())

job.result()

# %% [markdown]
# # Workouts
# Here we retrieve all workout ids for the user, workout metadata, performance graphs that include heart rate data, and send to a table in BigQuery.

# %%
# retrieve all workout ids
workout_ids = user.get_workout_ids()

# create workout objects
workouts = [peloton_workout.PelotonWorkout(user, workout_id) for workout_id in workout_ids]

# get workout summaries
summaries = [workout.get_workout_summary() for workout in workouts]

# get workout details
details = [workout.get_workout_details() for workout in workouts]


# %%
# send workout data to BigQuery

table_id = f'{BIGQUERY_DATASET}.workouts'

payload = pd.concat([workout.to_df() for workout in workouts])

job = client.load_table_from_dataframe(payload, table_id, job_config=workouts[0].get_bigquery_job_config())

job.result()


# %%
# send performance graph data to BigQuery

# table_id = f'{BIGQUERY_DATASET}.performance_graphs'

# job_config = bigquery.LoadJobConfig(
#     schema=[
#         bigquery.SchemaField('workout_id', 'STRING'),
#         bigquery.SchemaField('display_name', 'STRING'),
#         bigquery.SchemaField('range', 'STRING'),
#         bigquery.SchemaField('minimum_value', 'INTEGER'),
#         bigquery.SchemaField('maximum_value', 'INTEGER'),
#         bigquery.SchemaField('duration_seconds', 'INTEGER'),
#     ],
#     write_disposition='WRITE_TRUNCATE'
# )

# job = client.load_table_from_dataframe(performance_graph_df, table_id, job_config=job_config)

# job.result()

# %% [markdown]
# # Rides
# Here we retrieve class data for the class taken during the workout.

# %%
# pull rides data for list of unique ride ids
ride_ids = [workout.ride_id for workout in workouts]
unique_ride_ids = list(dict.fromkeys(ride_ids))

# create objects
rides = [peloton_ride.PelotonRide(user, ride_id) for ride_id in unique_ride_ids]

# fetch all possible ride types
ride_types = rides[0].get_ride_types()

for ride in rides:
    ride_type = next((ride_type for ride_type in ride_types if ride_type['id'] == ride.ride_type_id), None)
    ride.ride_type_display_name = ride_type['display_name']


# %%
table_id = f'{BIGQUERY_DATASET}.rides'

payload = pd.concat([ride.to_df() for ride in rides])

job = client.load_table_from_dataframe(payload, table_id, job_config=rides[0].get_bigquery_job_config())

job.result()

# %% [markdown]
# # Instructors

# %%
instructor_ids = [ride.instructor_id for ride in rides]
unique_instructor_ids = list(dict.fromkeys(instructor_ids))

instructors = [peloton_instructor.PelotonInstructor(instructor_id) for instructor_id in unique_instructor_ids if instructor_id is not None]


# %%
table_id = f'{BIGQUERY_DATASET}.instructors'

payload = pd.concat([instructor.to_df() for instructor in instructors])

job = client.load_table_from_dataframe(payload, table_id, job_config=instructors[0].get_bigquery_job_config())

job.result()


# %%



