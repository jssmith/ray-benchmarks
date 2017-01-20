import argparse
import base64
import json
import os

from stats import save_ray_events

if 'RAY_REDIS_ADDRESS' in os.environ:
    redis_address = os.environ['RAY_REDIS_ADDRESS']
else:
    raise RuntimeError("Not found: RAY_REDIS_ADDRESS")

parser = argparse.ArgumentParser()
parser.add_argument("--config", required=True, help="configuration information")
args = parser.parse_args()

config = json.loads(base64.b64decode(args.config))

save_ray_events(redis_address, config, 0)
