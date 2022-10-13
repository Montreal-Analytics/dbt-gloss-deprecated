from mixpanel import Mixpanel


class dbtGlossTracking:
    def __init__(self, disable_anonymous_tracking):
        self.token = '34ffa16dc37f248c18ad6d1b9ea9c3a8'
        self.mixpanel = Mixpanel(token=self.token)
        self.disable_anonymous_tracking = disable_anonymous_tracking

    def track_hook_event(self, event_name, event_properties, manifest):

        if not self.disable_anonymous_tracking:
            dbt_metadata = manifest.get("metadata")
            event_properties = self._property_transformations(
                dbt_metadata,
                event_properties
            )
            self.mixpanel.track(
                distinct_id=dbt_metadata.get('user_id'),
                event_name=event_name,
                properties=event_properties
            )

    def _property_transformations(self, dbt_metadata, event_properties):
        event_properties.update(dbt_metadata)
        transformation_func = [self._status_code_to_text]

        for function in transformation_func:
            event_properties = function(event_properties)

        return event_properties

    @staticmethod
    def _status_code_to_text(hook_properties):
        transformed_properties = hook_properties
        if hook_properties.get('Status') == 0:
            transformed_properties['Status'] = 'Success'
        elif hook_properties.get('Status') == 1:
            transformed_properties['Status'] = 'Fail'

        return transformed_properties
