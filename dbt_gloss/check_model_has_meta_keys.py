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

def has_meta_key(
    paths: Sequence[str], manifest: Dict[str, Any], meta_keys: Sequence[str]
) -> int:
    status_code = 0
    ymls = get_filenames(paths, [".yml", ".yaml"])
    sqls = get_model_sqls(paths, manifest)
    filenames = set(sqls.keys())
    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames)
    # if user added schema but did not rerun the model
    schemas = get_model_schemas(list(ymls.values()), filenames)
    # convert to sets
    in_models = {
        model.filename
        for model in models
        if set(model.node.get("meta", {}).keys()) == set(meta_keys)
    }
    in_schemas = {
        schema.model_name
        for schema in schemas
        if set(schema.schema.get("meta", {}).keys()) == set(meta_keys)
    }
    missing = filenames.difference(in_models, in_schemas)

    for model in missing:
        status_code = 1
        result = "\n- ".join(list(meta_keys))  # pragma: no mutate
        print(
            f"{sqls.get(model)}: "
            f"does not have some of the meta keys defined:\n- {result}",
        )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_config_args(parser)
    add_filenames_args(parser)
    add_manifest_args(parser)
    add_tracking_args(parser)

    parser.add_argument(
        "--meta-keys",
        nargs="+",
        required=True,
        help="List of required key in meta part of model.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    status_code = has_meta_key(paths=args.filenames, manifest=manifest, meta_keys=args.meta_keys)
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtGlossTracking()
    tracker.track_hook_event(
        event_name='Hook Executed',
        manifest=manifest,
        event_properties={
            'hook_name': os.path.basename(__file__),
            'description': 'Check model has meta keys',
            'status': status_code,
            'execution_time': end_time - start_time,
            'is_pytest': script_args.get('is_test')
        },
        script_args=script_args,
    )    

    return status_code


if __name__ == "__main__":
    exit(main())
