from distutils.command.config import config
from importlib.metadata import metadata
import uuid
from mixpanel import Mixpanel

# Change to prod token upon merge
PROJECT_TOKEN = '34ffa16dc37f248c18ad6d1b9ea9c3a8'

# To do: Define the following variables based on functions
# user_id = {'id': str(uuid.uuid1())}
class dbtGlossTracking ():
    def __init__(self):
        self.mixpanel = Mixpanel(token=PROJECT_TOKEN)
        self.allow_anonymous_tracking = True

    def format_properties(self, hook_properties, manifest):
        metadata = manifest.get("metadata")
        distinct_id = metadata.get('user_id')
        metadata.update(hook_properties)

        return distinct_id, metadata

    def track_hook_event(self, hook_name, hook_properties, manifest):

        config = self.allow_anonymous_tracking
        event_properties = self._property_transformations(hook_properties)

        distinct_id, properties = self.format_properties(event_properties, manifest)
        if config:
            self.mixpanel.track(
                distinct_id=distinct_id,
                event_name=hook_name,
                properties=properties
            )

    def _property_transformations(self, hook_properties):
        transformation_func = [self._status_code_to_text]
        for function in transformation_func:
            hook_properties = function(hook_properties)

        return hook_properties

    @staticmethod
    def _status_code_to_text(hook_properties):
        if hook_properties.get('Status') == 0:
            hook_properties['Status'] = 'Success'
        elif hook_properties.get('Status') == 1:
            hook_properties['Status'] = 'Fail'
        else:
            hook_properties['Status'] = f'Unknown Status: {hook_properties.get("Status")}'
        return hook_properties



# Add hook properties to track event
# Open PR for tracking
# test this specific hook
