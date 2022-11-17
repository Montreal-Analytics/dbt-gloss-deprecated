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
from dbt_gloss.utils import get_json
from dbt_gloss.utils import get_source_schemas
from dbt_gloss.utils import add_tracking_args
from dbt_gloss.utils import add_manifest_args
from dbt_gloss.utils import JsonOpenError
from dbt_gloss.tracking import dbtGlossTracking


def check_column_desc(paths: Sequence[str]) -> Dict[str, Any]:
    status_code = 0
    ymls = [Path(path) for path in paths]

    # if user added schema but did not rerun
    schemas = get_source_schemas(ymls)

    for schema in schemas:
        missing_cols = {
            col.get("name")
            for col in schema.table_schema.get("columns", [])
            if not col.get("description")
        }
        if missing_cols and all(missing_cols):
            status_code = 1
            result = "\n- ".join(list(missing_cols))  # pragma: no mutate
            print(
                f"{schema.source_name}.{schema.table_name}: "
                f"following columns are missing description:\n- {result}",
            )
    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_config_args(parser)
    add_filenames_args(parser)
    add_tracking_args(parser)
    add_manifest_args(parser)

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    hook_properties = check_column_desc(paths=args.filenames)
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtGlossTracking()
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check the model has description",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
        script_args=script_args,
    )

    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
