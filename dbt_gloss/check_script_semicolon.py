import argparse
import os
import time

from typing import IO
from typing import Optional
from typing import Sequence

from dbt_gloss.utils import add_config_args
from dbt_gloss.utils import add_filenames_args
from dbt_gloss.utils import add_manifest_args
from dbt_gloss.utils import add_tracking_args
from dbt_gloss.utils import get_json
from dbt_gloss.utils import JsonOpenError

from dbt_gloss.tracking import dbtGlossTracking


def check_semicolon(file_obj: IO[bytes], replace: bool = False) -> int:
    # Test for newline at end of file
    # Empty files will throw IOError here
    status_code = 0
    try:
        file_obj.seek(-1, os.SEEK_END)
    except OSError:
        return status_code
    last_character = file_obj.read(1)  # pragma: no mutate

    while last_character in {b"\n", b"\r"}:  # pragma: no mutate
        # Deal with the beginning of the file
        if file_obj.tell() == 1:
            return status_code

        # Go back two bytes and read a character
        file_obj.seek(-2, os.SEEK_CUR)
        last_character = file_obj.read(1)  # pragma: no mutate

    # If last character is semicolon
    if last_character == b";":
        if replace:
            file_obj.seek(-1, os.SEEK_CUR)
            file_obj.truncate()
        status_code = 1
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_config_args(parser)
    add_filenames_args(parser)
    add_manifest_args(parser)
    add_tracking_args(parser)

    args = parser.parse_args(argv)
    status_code = 0

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()

    for filename in args.filenames:
        # Read as binary so we can read byte-by-byte
        with open(filename, "rb+") as file_obj:
            status_code_file = check_semicolon(file_obj)
            if status_code_file:
                print(
                    f"{filename}: contains a semicolon at the end. "
                    f"dbt does not support that."
                )
                status_code = status_code_file

    end_time = time.time()
    script_args = vars(args)

    tracker = dbtGlossTracking()
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check the script does not contain a semicolon.",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
        script_args=script_args,
    )

    return status_code


if __name__ == "__main__":
    exit(main())
