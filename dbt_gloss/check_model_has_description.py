import argparse
from distutils.log import debug
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from dbt_gloss.utils import add_filenames_args
from dbt_gloss.utils import add_manifest_args
from dbt_gloss.utils import get_filenames
from dbt_gloss.utils import get_json
from dbt_gloss.utils import get_model_schemas
from dbt_gloss.utils import get_model_sqls
from dbt_gloss.utils import get_models
from dbt_gloss.utils import JsonOpenError

import dbt_gloss.utils
import mixpanel
from mixpanel import Mixpanel
import uuid

project_token = '34ffa16dc37f248c18ad6d1b9ea9c3a8'
mixpanel = Mixpanel(token=project_token)
user_id = {'id': str(uuid.uuid1())}


def track_event(distinct_id,event_name,properties,debug=False):

    if debug:
        print(distinct_id)
        print(event_name)
        print(properties)
    else:
        mixpanel.track(
        distinct_id=distinct_id,
        event_name=event_name,
        properties=properties
    )


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
    
    mixpanel.track(
        distinct_id=user_id,
        event_name='check model has description',
        properties={'Status': status_code},
        debug = True
    ) 

    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_manifest_args(parser)

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    return has_description(paths=args.filenames, manifest=manifest)


if __name__ == "__main__":
    exit(main())
