import argparse
import os
import time

from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence
from typing import Set

from dbt_gloss.utils import add_filenames_args
from dbt_gloss.utils import add_manifest_args
from dbt_gloss.utils import add_tracking_args
from dbt_gloss.utils import get_json
from dbt_gloss.utils import get_source_schemas
from dbt_gloss.utils import JsonOpenError

from dbt_gloss.tracking import dbtGlossTracking


def has_freshness(paths: Sequence[str], required_freshness: Set[str]) -> Dict[str, Any]:
    status_code = 0
    ymls = [Path(path) for path in paths]

    # if user added schema but did not rerun
    schemas = get_source_schemas(ymls)

    for schema in schemas:
        source = schema.source_schema
        table = schema.table_schema
        merged = {**source.get("freshness", {}), **table.get("freshness", {})}
        freshness = {
            key
            for key, value in merged.items()
            if set(value.keys()) == {"count", "period"}
        }
        loaded = table.get("loaded_at_field") or source.get("loaded_at_field")
        if not loaded:
            status_code = 1
            print(
                f"{schema.source_name}.{schema.table_name}: "
                f"is missing `loaded_at_field` which is required for freshness."
            )
        if not (freshness == required_freshness):
            status_code = 1
            missing_params = required_freshness.difference(freshness)
            result = "\n- ".join(list(missing_params))  # pragma: no mutate
            print(
                f"{schema.source_name}.{schema.table_name}: "
                f"miss some required freshness parameters:"
                f"\n- {result} "
            )

    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_manifest_args(parser)
    add_tracking_args(parser)

    parser.add_argument(
        "--freshness",
        nargs="+",
        required=True,
        choices=["warn_after", "error_after"],
        help="List of required freshness options.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    hook_properties = has_freshness(
        paths=args.filenames, required_freshness=set(args.freshness)
    )
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtGlossTracking()
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check the source has the freshness.",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
        script_args=script_args,
    )

    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
