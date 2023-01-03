import argparse
import re
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from dbt_gloss.utils import add_catalog_args
from dbt_gloss.utils import add_filenames_args
from dbt_gloss.utils import get_filenames
from dbt_gloss.utils import get_json
from dbt_gloss.utils import get_models
from dbt_gloss.utils import JsonOpenError


def check_id_column_has_name_contract(
    paths: Sequence[str], catalog: Dict[str, Any]
) -> int:
    status_code = 0
    sqls = get_filenames(paths, [".sql"])
    filenames = set(sqls.keys())
    models = get_models(catalog, filenames)

    for model in models:
        for col in model.node.get("columns", []).values():
            col_name = col.get("name")
            col_type = col.get("type")

            # Check that no columns are called "ID".
            # Enforces named ID column names
            if col_name.upper() == "ID":
                status_code = 1
                print(
                    f"{col_name}: column is of type {col_type} and "
                    "does not have a named `id` column. Should "
                    "name the column. E.g. `id` -> `name_me_id`"
                )

    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_catalog_args(parser)

    args = parser.parse_args(argv)

    try:
        catalog = get_json(args.catalog)
    except JsonOpenError as e:
        print(f"Unable to load catalog file ({e})")
        return 1

    return check_id_column_has_name_contract(
        paths=args.filenames,
        catalog=catalog,
    )


if __name__ == "__main__":
    exit(main())
