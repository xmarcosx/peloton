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
        self.cycling_ftp = None
        self.email = None
        self.last_workout_epoch = None
        self.name = None
        self.userid = None

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
            raise ValueError('Failed to login using supplied credentials') 
        
        resp_json = resp.json()
        
        self.cycling_ftp = resp_json['user_data']['cycling_ftp']
        self.email = resp_json['user_data']['email']
        self.last_workout_epoch = resp_json['user_data']['last_workout_at']
        self.name = resp_json['user_data']['name']
        self.userid = resp_json['user_id']
        self.total_workouts = resp_json['user_data']['total_workouts']
