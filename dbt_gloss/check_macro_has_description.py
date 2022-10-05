import argparse
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from dbt_gloss.utils import add_filenames_args
from dbt_gloss.utils import add_manifest_args
from dbt_gloss.utils import get_filenames
from dbt_gloss.utils import get_json
from dbt_gloss.utils import get_macro_schemas
from dbt_gloss.utils import get_macro_sqls
from dbt_gloss.utils import get_macros
from dbt_gloss.utils import JsonOpenError


def has_description(paths: Sequence[str], manifest: Dict[str, Any]) -> int:
    status_code = 0
    ymls = get_filenames(paths, [".yml", ".yaml"])
    sqls = get_macro_sqls(paths, manifest)
    filenames = set(sqls.keys())

    # get manifest macros that pre-commit found as changed
    macros = get_macros(manifest, filenames)
    # if user added schema but did not rerun the macro
    schemas = get_macro_schemas(list(ymls.values()), filenames)
    # convert to sets
    in_macros = {macro.filename for macro in macros if macro.macro.get("description")}
    in_schemas = {
        schema.macro_name for schema in schemas if schema.schema.get("description")
    }
    missing = filenames.difference(in_macros, in_schemas)

    for macro in missing:
        status_code = 1
        print(
            f"{sqls.get(macro)}: "
            f"does not have defined description or properties file is missing.",
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
