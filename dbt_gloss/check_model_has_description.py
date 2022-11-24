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
from dbt_gloss.utils import get_model_schemas
from dbt_gloss.utils import get_model_sqls
from dbt_gloss.utils import get_models
from dbt_gloss.utils import JsonOpenError

from dbt_gloss.tracking import dbtGlossTracking


def has_description(paths: Sequence[str], manifest: Dict[str, Any]) -> int:
    status_code = 0
    ymls = get_filenames(paths, [".yml", ".yaml"])
    sqls = get_model_sqls(paths, manifest)
    filenames = set(sqls.keys())

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames)
    # if user added schema but did not rerun the model
    schemas = get_model_schemas(list(ymls.values()), filenames)
    # convert to sets
    in_models = {model.filename for model in models if model.node.get("description")}
    in_schemas = {
        schema.model_name for schema in schemas if schema.schema.get("description")
    }
    missing = filenames.difference(in_models, in_schemas)
    for model in missing:
        status_code = 1
        print(
            f"{sqls.get(model)}: "
            f"does not have defined description or properties file is missing.",
        )

    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_config_args(parser)
    add_manifest_args(parser)
    add_tracking_args(parser)

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    hook_properties = has_description(paths=args.filenames, manifest=manifest)
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtGlossTracking(script_args=script_args)
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
    )

    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
