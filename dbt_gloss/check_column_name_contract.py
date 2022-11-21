import argparse
import re
import os
import time

from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence


from dbt_gloss.utils import add_catalog_args
from dbt_gloss.utils import add_config_args
from dbt_gloss.utils import add_manifest_args
from dbt_gloss.utils import add_tracking_args
from dbt_gloss.utils import add_filenames_args
from dbt_gloss.utils import get_filenames
from dbt_gloss.utils import get_json
from dbt_gloss.utils import get_models
from dbt_gloss.utils import JsonOpenError

from dbt_gloss.tracking import dbtGlossTracking


def check_column_name_contract(
    paths: Sequence[str], pattern: str, dtype: str, catalog: Dict[str, Any]
) -> Dict[str, Any]:
    status_code = 0
    sqls = get_filenames(paths, [".sql"])
    filenames = set(sqls.keys())
    models = get_models(catalog, filenames)

    for model in models:
        for col in model.node.get("columns", []).values():
            col_name = col.get("name")
            col_type = col.get("type")

            # Check all files of type dtype follow naming pattern
            if dtype == col_type:
                if re.match(pattern, col_name) is None:
                    status_code = 1
                    print(
                        f"{col_name}: column is of type {dtype} and "
                        f"does not match regex pattern {pattern}."
                    )

            # Check all files with naming pattern are of type dtype
            elif re.match(pattern, col_name):
                status_code = 1
                print(
                    f"{col_name}: column name matches regex pattern {pattern} "
                    f"and is of type {col_type} instead of {dtype}."
                )

    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_config_args(parser)
    add_filenames_args(parser)
    add_catalog_args(parser)
    add_manifest_args(parser)
    add_tracking_args(parser)

    parser.add_argument(
        "--pattern",
        type=str,
        required=True,
        help="Regex pattern to match column names.",
    )
    parser.add_argument(
        "--dtype",
        type=str,
        required=True,
        help="Expected data type for the matching columns.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    try:
        catalog = get_json(args.catalog)
    except JsonOpenError as e:
        print(f"Unable to load catalog file ({e})")
        return 1

    start_time = time.time()
    hook_properties = check_column_name_contract(
        paths=args.filenames,
        pattern=args.pattern,
        dtype=args.dtype,
        catalog=catalog,
    )
    end_time = time.time()

    script_args = vars(args)

    tracker = dbtGlossTracking()
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check column name abides to contract.",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
        script_args=script_args,
    )

    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
