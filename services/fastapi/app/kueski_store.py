import json
import redis
from cfg import AppCfg

class KueskiStore():
    cfg = AppCfg()

    redis_host = cfg.redis_host
    redis_port = cfg.redis_port
    redis_password = cfg.redis_password

    def __init__(self):
        self.db = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            password=self.redis_password
        )

        # set a key
        self.db.set('test-key', 'test-value')

        # get a value
        value = self.db.get('test-key')
        print(value)

    def get(self, user_id):
        req_data = self.db.get(user_id)
        return json.loads(req_data) if req_data else None