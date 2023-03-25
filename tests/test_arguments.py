import pytest
import typing
from glue import arguments
import dataclasses


def mock_glue_resolved_arguments(mocker, return_dict: typing.Dict[str, str]):
    mocker.patch("glue.arguments.parse_arguments_to_dict", return_value=return_dict)


def test_arguments_succeeds(mocker):
    mock_glue_resolved_arguments(mocker, {"custom_option": "value"})

    @dataclasses.dataclass
    class TstArguments(arguments.Arguments):
        custom_option: str

    args = TstArguments.from_glue_arguments()
    assert args.custom_option == "value"


def test_arguments_succeeds_with_builtin_argument(mocker):
    mock_glue_resolved_arguments(mocker, {"JOB_NAME": "name", "custom_option": "value"})

    @dataclasses.dataclass
    class TstArguments(arguments.Arguments):
        JOB_NAME: str
        custom_option: str

    args = TstArguments.from_glue_arguments()
    assert args.custom_option == "value"
    assert args.JOB_NAME == "name"


def test_arguments_fails_if_missing(mocker):
    mock_glue_resolved_arguments(mocker, {"JOB_NAME": "name"})

    @dataclasses.dataclass
    class TstArguments(arguments.Arguments):
        JOB_NAME: str
        non_existent_argument: str

    with pytest.raises(TypeError):
        TstArguments.from_glue_arguments()
