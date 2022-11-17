import os

from dbt_gloss.utils import get_config_file
from mixpanel import Mixpanel
from typing import Any, Dict


class dbtGlossTracking:
    def __init__(self):
        self.token = "34ffa16dc37f248c18ad6d1b9ea9c3a8"
        self.disable_tracking = False

    def track_hook_event(
        self,
        event_name: str,
        event_properties: Dict[str, Any],
        manifest: Dict[str, Any],
        script_args: Dict[str, Any],
    ) -> None:

        self._parse_config(script_args.get('config'))

        if not self.disable_tracking:
            dbt_metadata = manifest.get("metadata")
            event_properties = self._property_transformations(
                dbt_metadata, event_properties
            )
            try:
                mixpanel = Mixpanel(token=self.token)
                mixpanel.track(
                    distinct_id=dbt_metadata.get("user_id"),
                    event_name=event_name,
                    properties=event_properties,
                )
            except Exception as error:
                print(f"Mixpanel Error: {error}")

    def _parse_config(self, config_path: str) -> None:
        config = get_config_file(config_path)
        self.disable_tracking = config.get('disable-tracking', False)

    def _property_transformations(
        self, dbt_metadata: Dict[str, Any], event_properties: Dict[str, Any]
    ) -> Dict[str, Any]:
        event_properties.update(dbt_metadata)
        transformation_func = [self._status_code_to_text, self._remove_ext_in_hook_name]

        for function in transformation_func:
            event_properties = function(event_properties)

        return event_properties

    @staticmethod
    def _status_code_to_text(event_properties: Dict[str, Any]) -> Dict[str, Any]:
        transformed_properties = event_properties
        if event_properties.get("status") == 0:
            transformed_properties["status"] = "Success"
        elif event_properties.get("status") == 1:
            transformed_properties["status"] = "Fail"

        return transformed_properties

    @staticmethod
    def _remove_ext_in_hook_name(event_properties: Dict[str, Any]) -> Dict[str, Any]:
        transformed_properties = event_properties
        transformed_properties["hook_name"] = os.path.splitext(
            transformed_properties.get("hook_name")
        )[0]

        return transformed_properties
