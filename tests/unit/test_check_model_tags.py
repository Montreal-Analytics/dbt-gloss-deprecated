import pytest

from dbt_gloss.check_model_tags import main


TESTS = (
    (["aa/bb/with_tags.sql", "--tags", "foo", "bar",'--is_test'], True, 0),
    (["aa/bb/with_tags_foo.sql", "--tags", "foo", "bar",'--is_test'], True, 0),
    (["aa/bb/with_tags_foo.sql", "--tags", "bar",'--is_test'], True, 1),
    (["aa/bb/with_tags_empty.sql", "--tags", "bar",'--is_test'], True, 0),
    (["aa/bb/without_tags.sql", "--tags", "foo", "bar",'--is_test'], True, 0),
    (["aa/bb/without_tags.sql", "--tags", "foo", "bar",'--is_test'], False, 1),
)


@pytest.mark.parametrize(
    ("input_args", "valid_manifest", "expected_status_code"), TESTS
)
def test_check_model_tags(
    input_args, valid_manifest, expected_status_code, manifest_path_str
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    status_code = main(input_args)
    assert status_code == expected_status_code


def test_check_model_tags_in_changed(tmpdir, manifest_path_str):
    schema_yml = """
version: 2

models:
-   name: in_schema_tags
    tags:
        - foo
        - bar
-   name: xxx
    """
    yml_file = tmpdir.join("schema.yml")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "in_schema_tags.sql",
            str(yml_file),
            "--tags",
            "foo",
            "bar",
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 0
