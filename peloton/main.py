import os
import peloton_user

USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']

user = peloton_user.PelotonUser(USERNAME, PASSWORD)

print(f'User ID: {user.userid}')
