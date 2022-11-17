import argparse
import os
import time

from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from dbt_gloss.utils import add_config_args
from dbt_gloss.utils import add_filenames_args
from dbt_gloss.utils import add_manifest_args
from dbt_gloss.utils import add_tracking_args
from dbt_gloss.utils import get_source_schemas
from dbt_gloss.utils import get_json
from dbt_gloss.utils import JsonOpenError

from dbt_gloss.tracking import dbtGlossTracking


def validate_tags(paths: Sequence[str], tags: Sequence[str]) -> Dict[str, Any]:
    status_code = 0
    ymls = [Path(path) for path in paths]

    # if user added schema but did not rerun
    schemas = get_source_schemas(ymls)

    for schema in schemas:
        schema_tags = set(schema.source_schema.get("tags", []))
        table_tags = set(schema.table_schema.get("tags", []))
        source_tags = schema_tags.union(table_tags)
        valid_tags = set(tags)
        if not source_tags.issubset(valid_tags):
            status_code = 1
            list_diff = list(source_tags.difference(valid_tags))
            result = "\n- ".join(list_diff)  # pragma: no mutate
            print(
                f"{schema.source_name}.{schema.table_name}: "
                f"has invalid tags:\n- {result}",
            )

    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_config_args(parser)
    add_filenames_args(parser)
    add_manifest_args(parser)
    add_tracking_args(parser)

    parser.add_argument(
        "--tags",
        nargs="+",
        required=True,
        help="A list of tags that source can have.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    hook_properties = validate_tags(paths=args.filenames, tags=args.tags)
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtGlossTracking()
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check the source has valid tags.",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
        script_args=script_args,
    )

    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
