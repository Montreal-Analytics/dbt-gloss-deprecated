import os

from mixpanel import Mixpanel


class dbtGlossTracking:
    def __init__(self):
        self.token = '34ffa16dc37f248c18ad6d1b9ea9c3a8'

    def track_hook_event(
            self,
            event_name,
            event_properties,
            manifest,
            script_args
    ):
        disable_tracking = script_args.get('disable_tracking', False)
        if not disable_tracking:
            dbt_metadata = manifest.get("metadata")
            event_properties = self._property_transformations(
                dbt_metadata,
                event_properties
            )
            try:
                mixpanel = Mixpanel(token=self.token)
                mixpanel.track(
                    distinct_id=dbt_metadata.get('user_id'),
                    event_name=event_name,
                    properties=event_properties
                )
            except Exception as error:
                print(f'Mixpanel Error: {error}')

    @staticmethod
    def _ignore_errors(func):
        def decorate(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except Exception as error:
                print(f'Mixpanel Error: {error}')
        return decorate

    def _property_transformations(self, dbt_metadata, event_properties):
        event_properties.update(dbt_metadata)
        transformation_func = [
            self._status_code_to_text,
            self._remove_ext_in_hook_name
        ]

        for function in transformation_func:
            event_properties = function(event_properties)

        return event_properties

    @staticmethod
    def _status_code_to_text(hook_properties):
        transformed_properties = hook_properties
        if hook_properties.get('status') == 0:
            transformed_properties['status'] = 'Success'
        elif hook_properties.get('status') == 1:
            transformed_properties['status'] = 'Fail'

        return transformed_properties

    @staticmethod
    def _remove_ext_in_hook_name(hook_properties):
        transformed_properties = hook_properties
        transformed_properties['hook_name'] = os.path.splitext(
            transformed_properties.get('hook_name'))[0]

        return transformed_properties
