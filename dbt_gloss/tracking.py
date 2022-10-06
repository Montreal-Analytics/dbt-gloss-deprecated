from distutils.command.config import config
from importlib.metadata import metadata
import uuid
import dbt_gloss
from mixpanel import Mixpanel

# Change to prod token upon merge
project_token = ''
mixpanel = Mixpanel(token=project_token) 
# To do: Define the following variables based on functions
# user_id = {'id': str(uuid.uuid1())} 

def get_config():
    return {'allow_anonymous_tracking': True}  

def format_properties(hook_properties,manifest):
    metadata = manifest.get("metadata", {})
    distinct_id = metadata.get('user_id')
    properties = {
        'project_id': metadata.get('project_id'),
        'dbt_version': metadata.get('dbt_version')
                }
    return distinct_id, properties


def track_hook_event(hook_name,hook_properties,manifest):
    config = get_config()
    distinct_id,properties = format_properties(hook_properties,manifest)
    if config['allow_anonymous_tracking']:
        mixpanel.track(
            distinct_id=distinct_id,
            event_name=hook_name,
            properties=properties   
        )


# Add hook properties to track event
# Open PR for tracking
# test this specific hook

    