import argparse
import os
import time

from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from dbt_gloss.utils import add_default_args
from dbt_gloss.utils import get_json
from dbt_gloss.utils import get_parent_childs
from dbt_gloss.utils import get_source_schemas
from dbt_gloss.utils import JsonOpenError

from dbt_gloss.tracking import dbtGlossTracking


def check_test_cnt(
    paths: Sequence[str], manifest: Dict[str, Any], test_cnt: int
) -> Dict[str, Any]:
    status_code = 0
    ymls = [Path(path) for path in paths]

    # if user added schema but did not rerun
    schemas = get_source_schemas(ymls)

    for schema in schemas:
        tests = list(
            get_parent_childs(
                manifest=manifest,
                obj=schema,
                manifest_node="child_map",
                node_types=["test"],
            )
        )
        source_test_cnt = len(tests)
        if source_test_cnt < test_cnt:
            status_code = 1
            print(
                f"{schema.source_name}.{schema.table_name}: "
                f"has only {source_test_cnt} tests, but {test_cnt} are required.",
            )
    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument(
        "--test-cnt",
        type=int,
        default=1,
        help="Minimum number of tests required.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    hook_properties = check_test_cnt(
        paths=args.filenames, manifest=manifest, test_cnt=args.test_cnt
    )
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtGlossTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check the source has a number of tests.",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
