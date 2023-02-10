import pytest

from dbt_gloss.check_id_column_is_named import main

# Input args, valid manifest, expected return value
TESTS = (
    (["aa/bb/id_column_is_named.sql"], True, 0),
    (["aa/bb/id_column_is_named.sql"], False, 1),
    (["aa/bb/id_column_is_not_named.sql"], True, 1),
)


@pytest.mark.parametrize(
    ("input_args", "valid_catalog", "expected_status_code"), TESTS
)
def test_check_id_column_is_named(
    input_args, valid_catalog, expected_status_code, catalog_path_str
):
    if valid_catalog:
        input_args.extend(["--catalog", catalog_path_str])
    status_code = main(input_args)
    assert status_code == expected_status_code
