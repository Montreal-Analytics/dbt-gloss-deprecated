import pytest

from dbt_gloss.check_model_has_description import main

# Input args, valid manifest, valid_config, expected return value
TESTS = (
    (["aa/bb/with_description.sql", "--is_test"], True, True, 0),
    (["bb/bb/with_description.sql", "--is_test"], False, True, 1),
    (["cc/bb/without_description.sql", "--is_test"], True, True, 1),
    (["dd/bb/with_description.sql", "--is_test"], True, False, 0),
)


@pytest.mark.parametrize(
    ("input_args", "valid_manifest", "valid_config", "expected_status_code"), TESTS
)
def test_check_model_description(
    input_args,
    valid_manifest,
    valid_config,
    expected_status_code,
    manifest_path_str,
    config_path_str,
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])
    status_code = main(input_args)
    assert status_code == expected_status_code


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_model_description_in_changed(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2

models:
-   name: in_schema_desc
    description: blabla
-   name: xxx
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "in_schema_desc.sql",
            str(yml_file),
            "--is_test",
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 0
