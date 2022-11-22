import argparse
import os
import time
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from dbt_gloss.utils import add_config_args
from dbt_gloss.utils import add_filenames_args
from dbt_gloss.utils import add_manifest_args
from dbt_gloss.utils import add_tracking_args
from dbt_gloss.utils import get_json
from dbt_gloss.utils import get_model_sqls
from dbt_gloss.utils import get_models
from dbt_gloss.utils import get_parent_childs
from dbt_gloss.utils import JsonOpenError
from dbt_gloss.utils import Test

from dbt_gloss.tracking import dbtGlossTracking


def check_test_cnt(
    paths: Sequence[str], manifest: Dict[str, Any], test_cnt: int
) -> int:
    status_code = 0
    sqls = get_model_sqls(paths, manifest)
    filenames = set(sqls.keys())

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames)

    for model in models:
        childs = list(
            get_parent_childs(
                manifest=manifest,
                obj=model,
                manifest_node="child_map",
                node_types=["test"],
            )
        )
        tests = [test for test in childs if isinstance(test, Test)]
        model_test_cnt = len(tests)
        if model_test_cnt < test_cnt:
            status_code = 1
            print(
                f"{model.model_name}: "
                f"has only {model_test_cnt} tests, but {test_cnt} are required.",
            )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_config_args(parser)
    add_filenames_args(parser)
    add_manifest_args(parser)
    add_tracking_args(parser)

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
    status_code = check_test_cnt(
        paths=args.filenames, 
        manifest=manifest, 
        test_cnt=args.test_cnt
    )
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtGlossTracking()
    tracker.track_hook_event(
        event_name='Hook Executed',
        manifest=manifest,
        event_properties={
            'hook_name': os.path.basename(__file__),
            'description': 'Check model has tests',
            'status': status_code,
            'execution_time': end_time - start_time,
            'is_pytest': script_args.get('is_test')
        },
        script_args=script_args,
    )

    return status_code

if __name__ == "__main__":
    exit(main())
