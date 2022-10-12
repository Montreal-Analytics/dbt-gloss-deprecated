import argparse
from itertools import groupby
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from dbt_gloss.utils import add_filenames_args
from dbt_gloss.utils import add_manifest_args
from dbt_gloss.utils import get_json
from dbt_gloss.utils import get_parent_childs
from dbt_gloss.utils import get_source_schemas
from dbt_gloss.utils import JsonOpenError
from dbt_gloss.utils import ParseDict
from dbt_gloss.utils import Test


def check_test_cnt(
    paths: Sequence[str], manifest: Dict[str, Any], required_tests: Dict[str, int]
) -> int:
    status_code = 0
    ymls = [Path(path) for path in paths]

    # if user added schema but did not rerun
    schemas = get_source_schemas(ymls)

    for schema in schemas:
        childs = get_parent_childs(
            manifest=manifest,
            obj=schema,
            manifest_node="child_map",
            node_types=["test"],
        )
        tests = [test for test in childs if isinstance(test, Test)]
        grouped = groupby(
            sorted(tests, key=lambda x: x.test_name), lambda x: x.test_name
        )
        test_dict = {key: list(value) for key, value in grouped}
        for required_test, required_cnt in required_tests.items():
            test = test_dict.get(required_test, [])
            test_cnt = len(test)
            if not test or required_cnt > test_cnt:
                status_code = 1
                print(
                    f"{schema.source_name}.{schema.table_name}: "
                    f"has only {test_cnt} {required_test} tests, but "
                    f"{required_cnt} are required.",
                )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_manifest_args(parser)

    parser.add_argument(
        "--tests",
        metavar="KEY=VALUE",
        nargs="+",
        required=True,
        help="Set a number of key-value pairs."
        " Key is name of test and value is required "
        "minimal number of tests eg. --test unique=1 not_null=2"
        "(do not put spaces before or after the = sign).",
        action=ParseDict,
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    required_tests = {}

    for test_type, cnt in args.tests.items():
        try:
            test_cnt = int(cnt)
        except ValueError:
            parser.error(f"Unable to cast {cnt} to int.")
        required_tests[test_type] = test_cnt

    return check_test_cnt(
        paths=args.filenames, manifest=manifest, required_tests=required_tests
    )


if __name__ == "__main__":
    exit(main())
