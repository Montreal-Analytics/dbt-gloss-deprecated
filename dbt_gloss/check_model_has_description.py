import argparse
import os
import time
from distutils.log import debug
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from dbt_gloss.utils import add_filenames_args
from dbt_gloss.utils import add_manifest_args
from dbt_gloss.utils import add_tracking_args
from dbt_gloss.utils import get_filenames
from dbt_gloss.utils import get_json
from dbt_gloss.utils import get_model_schemas
from dbt_gloss.utils import get_model_sqls
from dbt_gloss.utils import get_models
from dbt_gloss.utils import JsonOpenError
from dbt_gloss.utils import APIError

from dbt_gloss.tracking import dbtGlossTracking


def has_description(paths: Sequence[str], manifest: Dict[str, Any]) -> int:
    error_count = 0
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
        error_count += 1
        print(
            f"{sqls.get(model)}: "
            f"does not have defined description or properties file is missing.",
        )

    metadata = {
        'status_code': status_code,
        'object_count': len(list(models))+1,
        'error_count': error_count
    }

    return metadata


def tracking(manifest, hook_metadata, runtime, script_args):
    hook_name = os.path.splitext(os.path.basename(__file__))[0]
    disable_tracking = script_args.get('disable_tracking', False)

    try:
        mixpanel = dbtGlossTracking(disable_tracking)
        mixpanel.track_hook_event(
            event_name=hook_name,
            manifest=manifest,
            event_properties={
                'Hook Name': hook_name,
                'Description': 'Check the model has description',
                'Status': hook_metadata.get('status_code'),
                'Object Count': hook_metadata.get('object_count'),
                'Error Count': hook_metadata.get('error_count'),
                'Execution Runtime': runtime,
            },
            is_test=script_args.get('is_test')
        )

    except APIError as error:
        print(f'Mixpanel Error: {error}')


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_manifest_args(parser)
    add_tracking_args(parser)

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    hook_metadata = has_description(paths=args.filenames, manifest=manifest)
    end_time = time.time()

    tracking(
        manifest=manifest,
        hook_metadata=hook_metadata,
        runtime=end_time - start_time,
        script_args=vars(args)
    )

    return hook_metadata.get('status_code')


if __name__ == "__main__":
    exit(main())
