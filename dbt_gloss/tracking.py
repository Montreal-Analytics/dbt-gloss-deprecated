from mixpanel import Mixpanel


class dbtGlossTracking:
    def __init__(self, disable_anonymous_tracking):
        self.token = '34ffa16dc37f248c18ad6d1b9ea9c3a8'
        self.mixpanel = Mixpanel(token=self.token)
        self.disable_anonymous_tracking = disable_anonymous_tracking

    def track_hook_event(self, event_name, event_properties, manifest, is_test):

        if not self.disable_anonymous_tracking:
            dbt_metadata = manifest.get("metadata")
            event_properties = self._property_transformations(
                dbt_metadata,
                event_properties,
                is_test=is_test
            )
            self.mixpanel.track(
                distinct_id=dbt_metadata.get('user_id'),
                event_name=event_name,
                properties=event_properties
            )

    def _property_transformations(self, dbt_metadata, event_properties, **kwargs):
        transformation_func = [self._status_code_to_text, self._is_test]
        for function in transformation_func:
            event_properties = function(event_properties, kwargs)

        event_properties.update(dbt_metadata)
        return event_properties

    @staticmethod
    def _status_code_to_text(hook_properties, kwargs):
        if hook_properties.get('Status') == 0:
            hook_properties['Status'] = 'Success'
        elif hook_properties.get('Status') == 1:
            hook_properties['Status'] = 'Fail'

        return hook_properties

    @staticmethod
    def _is_test(hook_properties, kwargs):
        is_test = kwargs.get('is_test')
        if is_test:
            hook_properties['Is Pytest'] = is_test

        return hook_properties
