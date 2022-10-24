import pytest

from dbt_gloss.check_model_name_contract import main


# Input args, valid manifest, expected return value
TESTS = (
    (["aa/bb/catalog_cols.sql",'--is_test'], "catalog_.*", True,True,0),
    (["aa/bb/catalog_cols.sql",'--is_test'], "catalog_.*", False,True,1),
    (["aa/bb/catalog_cols.sql",'--is_test'], "catalog_.*", True,False,1),
    (["aa/bb/catalog_cols.sql",'--is_test'], ".*_cols", True,True, 0),
    (["aa/bb/catalog_cols.sql",'--is_test'], ".*_col", True,True, 0),
    (["aa/bb/catalog_cols.sql",'--is_test'], ".*_col$", True,True, 1),
    (["aa/bb/catalog_cols.sql",'--is_test'], "catalog_cols", True,True, 0),
    (["aa/bb/catalog_cols.sql",'--is_test'], "cat_.*", True,True, 1),
)


@pytest.mark.parametrize(
    ("input_args", "pattern", "valid_catalog" ,"valid_manifest","expected_status_code"), TESTS
)
def test_model_name_contract(
    input_args, pattern, valid_catalog,valid_manifest, expected_status_code, catalog_path_str,manifest_path_str
):
    if valid_catalog:
        input_args.extend(["--catalog", catalog_path_str])

    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    input_args.extend(["--pattern", pattern])    

    status_code = main(input_args)
    assert status_code == expected_status_code
