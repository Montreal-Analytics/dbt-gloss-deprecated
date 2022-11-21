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
from dbt_gloss.utils import get_filenames
from dbt_gloss.utils import get_json
from dbt_gloss.utils import get_models
from dbt_gloss.utils import get_parent_childs
from dbt_gloss.utils import JsonOpenError

from dbt_gloss.tracking import dbtGlossTracking


def check_parents_schema(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    blacklist: Optional[Sequence[str]],
    whitelist: Optional[Sequence[str]],
) -> int:
    status_code = 0
    sqls = get_filenames(paths, [".sql"])
    filenames = set(sqls.keys())
    blacklist = blacklist or []
    whitelist = whitelist or []

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames)

    for model in models:
        parents = list(
            get_parent_childs(
                manifest=manifest,
                obj=model,
                manifest_node="parent_map",
                node_types=["model", "source"],
            )
        )
        for parent in parents:
            db = parent.node.get("schema")
            if (whitelist and db not in whitelist) or db in blacklist:
                status_code = 1
                print(
                    f"{model.model_name}: "
                    f"has parent {parent.node.get('name')} with invalid schema "
                    f"{db}.",
                )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_config_args(parser)
    add_filenames_args(parser)
    add_manifest_args(parser)
    add_tracking_args(parser)

    white_black = parser.add_mutually_exclusive_group()

    white_black.add_argument(
        "--whitelist",
        nargs="+",
        help="Whitelisted schemas.",
    )

    white_black.add_argument(
        "--blacklist",
        nargs="+",
        help="Blacklisted schemas.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    if not (args.blacklist or args.whitelist):
        print("Please specify at least one `--blacklist` or `--whitelist` option.")
        return 1

    start_time = time.time()
    status_code = check_parents_schema(
                      paths=args.filenames,
                      manifest=manifest,
                      blacklist=args.blacklist,
                      whitelist=args.whitelist,
                  )
    end_time = time.time()
    script_args = vars(args)   

    tracker = dbtGlossTracking()
    tracker.track_hook_event(
        event_name='Hook Executed',
        manifest=manifest,
        event_properties={
            'hook_name': os.path.basename(__file__),
            'description': 'Check model parents schema',
            'status': status_code,
            'execution_time': end_time - start_time,
            'is_pytest': script_args.get('is_test')
        },
        script_args=script_args,
    )         

    return status_code

if __name__ == "__main__":
    exit(main())
