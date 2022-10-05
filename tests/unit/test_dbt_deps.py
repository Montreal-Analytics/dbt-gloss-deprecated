from unittest.mock import patch

from dbt_gloss.dbt_deps import main
from dbt_gloss.dbt_deps import prepare_cmd


def test_dbt_deps():
    with patch("dbt_gloss.utils.subprocess.Popen") as mock_popen:
        mock_popen.return_value.communicate.return_value = (
            b"stdout",
            b"stderr",
        )
        mock_popen.return_value.returncode = 0
        result = main()
        assert result == 0


def test_dbt_deps_error():
    with patch("dbt_gloss.utils.subprocess.Popen") as mock_popen:
        mock_popen.return_value.communicate.return_value = (
            b"stdout",
            b"stderr",
        )
        mock_popen.return_value.returncode = 1
        result = main()
        assert result == 1


def test_dbt_deps_cmd():
    result = prepare_cmd()
    assert result == ["dbt", "deps"]
