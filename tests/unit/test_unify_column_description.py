import pytest

from dbt_gloss.unify_column_description import main

TESTS = (  # type: ignore
    (
        """
version: 2

models:
-   name: same_col_desc_1
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
-   name: same_col_desc_2
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
-   name: same_col_desc_3
    columns:
    -   name: test1
        description: test1
    -   name: test2
        description: test2
    """,
        True,
        1,
        """version: 2
models:
- name: same_col_desc_1
  columns:
  - name: test1
    description: test
  - name: test2
    description: test
- name: same_col_desc_2
  columns:
  - name: test1
    description: test
  - name: test2
    description: test
- name: same_col_desc_3
  columns:
  - name: test1
    description: test
  - name: test2
    description: test
""",
        [],
    ),
    (
        """
version: 2

models:
-   name: same_col_desc_1
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
-   name: same_col_desc_2
    columns:
    -   name: test1
        description: test2
    -   name: test2
        description: test
-   name: same_col_desc_3
    columns:
    -   name: test1
        description: test1
    -   name: test2
        description: test2
    """,
        True,
        1,
        """version: 2
models:
- name: same_col_desc_1
  columns:
  - name: test1
    description: test
  - name: test2
    description: test
- name: same_col_desc_2
  columns:
  - name: test1
    description: test2
  - name: test2
    description: test
- name: same_col_desc_3
  columns:
  - name: test1
    description: test1
  - name: test2
    description: test
""",
        [],
    ),
    (
        """
version: 2

models:
-   name: same_col_desc_1
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
-   name: same_col_desc_2
    columns:
    -   name: test1
        description: test2
    -   name: test2
        description: test
-   name: same_col_desc_3
    columns:
    -   name: test1
        description: test1
    -   name: test2
        description: test
    """,
        True,
        0,
        """
version: 2

models:
-   name: same_col_desc_1
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
-   name: same_col_desc_2
    columns:
    -   name: test1
        description: test2
    -   name: test2
        description: test
-   name: same_col_desc_3
    columns:
    -   name: test1
        description: test1
    -   name: test2
        description: test
    """,
        ["--ignore", "test1"],
    ),
    (
        """
version: 2

models:
-   name: same_col_desc_1
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
-   name: same_col_desc_2
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
-   name: same_col_desc_3
    columns:
    -   name: test1
    -   name: test2
    """,
        True,
        1,
        """version: 2
models:
- name: same_col_desc_1
  columns:
  - name: test1
    description: test
  - name: test2
    description: test
- name: same_col_desc_2
  columns:
  - name: test1
    description: test
  - name: test2
    description: test
- name: same_col_desc_3
  columns:
  - name: test1
    description: test
  - name: test2
    description: test
""",
        [],
    ),
    (
        """
version: 2

models:
-   name: same_col_desc_1
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
-   name: same_col_desc_2
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
-   name: same_col_desc_3
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
    """,
        True,
        0,
        """
version: 2

models:
-   name: same_col_desc_1
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
-   name: same_col_desc_2
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
-   name: same_col_desc_3
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
    """,
        [],
    ),
    (
        """
version: 2

models:
-   name: same_col_desc_1
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
-   name: same_col_desc_2
    columns:
    -   name: test1
        description: test2
    -   name: test2
        description: test
-   name: same_col_desc_3
    columns:
    -   name: test1
        description: test1
    -   name: test2
        description: test
    """,
        False,
        0,
        """
version: 2

models:
-   name: same_col_desc_1
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
-   name: same_col_desc_2
    columns:
    -   name: test1
        description: test2
    -   name: test2
        description: test
-   name: same_col_desc_3
    columns:
    -   name: test1
        description: test1
    -   name: test2
        description: test
    """,
        ["--ignore", "test1"],
    ),
)


@pytest.mark.parametrize(
    ("schema_yml", "valid_config", "expected_status_code", "expected_result", "ignore"), TESTS
)
def test_replace_column_description(
    schema_yml, valid_config, expected_status_code, expected_result, ignore, tmpdir, manifest_path_str, config_path_str
):
    yml_file = tmpdir.join("schema.yml")
    yml_file.write(schema_yml)
    input_args = [
        str(yml_file),
        "--is_test",
        "--manifest",
        manifest_path_str,
    ]
    input_args.extend(ignore)

    if valid_config:
        input_args.extend(["--config", config_path_str])

    status_code = main(input_args)
    result = yml_file.read_text("utf-8")
    assert status_code == expected_status_code
    assert expected_result == result


def test_replace_column_description_split(tmpdir, manifest_path_str):
    schema_yml1 = """
version: 2

models:
-   name: same_col_desc_1
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
    """
    schema_yml2 = """
version: 2

models:
-   name: same_col_desc_2
    columns:
    -   name: test1
        description: test_bad
    -   name: test2
    """
    schema_yml3 = """
version: 2

models:
-   name: same_col_desc_3
    columns:
    -   name: test1
        description: test
    -   name: test2
    """
    yml_file1 = tmpdir.join("schema1.yml")
    yml_file2 = tmpdir.join("schema2.yml")
    yml_file3 = tmpdir.join("schema3.yml")
    yml_file1.write(schema_yml1)
    yml_file2.write(schema_yml2)
    yml_file3.write(schema_yml3)
    input_args = [
        str(yml_file1),
        str(yml_file2),
        str(yml_file3),
        "--is_test",
        "--manifest",
        manifest_path_str,
    ]
    status_code = main(input_args)
    assert status_code == 1
    result1 = yml_file1.read_text("utf-8")
    result2 = yml_file2.read_text("utf-8")
    result3 = yml_file3.read_text("utf-8")
    assert (
        result1
        == """
version: 2

models:
-   name: same_col_desc_1
    columns:
    -   name: test1
        description: test
    -   name: test2
        description: test
    """
    )
    assert (
        result2
        == """version: 2
models:
- name: same_col_desc_2
  columns:
  - name: test1
    description: test
  - name: test2
    description: test
"""
    )
    assert (
        result3
        == """version: 2
models:
- name: same_col_desc_3
  columns:
  - name: test1
    description: test
  - name: test2
    description: test
"""
    )
