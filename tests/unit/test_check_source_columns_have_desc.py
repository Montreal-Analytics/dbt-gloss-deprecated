import pytest

from dbt_gloss.check_source_columns_have_desc import main


# Input schema, expected return value
TESTS = (
    (
        """
sources:
-   name: test
    tables:
    -   name: test
        columns:
           - name: col1
             description: test
           - name: col2
             description: test
    """,
        0,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test
        columns:
           - name: col1
           - name: col2
             description: test
    """,
        1,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test
        columns:
           - name: col1
    """,
        1,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test
        columns:
           - name: col1
             description: test
           - name: col2
             description: test
    -   name: test2
        columns:
           - name: col1
           - name: col2
             description: test
    """,
        1,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test
    """,
        0,
    ),
)


@pytest.mark.parametrize(("input_schema", "expected_status_code"), TESTS)
def test_check_source_columns_have_desc(
        input_schema, expected_status_code, tmpdir, manifest_path_str
):
    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)
    status_code = main(
        argv=[str(yml_file), "--manifest", manifest_path_str, '--is_test'])
    assert status_code == expected_status_code
