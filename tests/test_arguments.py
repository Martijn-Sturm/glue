import pytest
import typing
from glue_helper_lib import arguments
import dataclasses


def mock_glue_resolved_arguments(mocker, env_args: typing.Dict[str, str]):
    return_dict = {"JOB_NAME": "mocked_job_name", **env_args}
    mocker.patch(
        "glue_helper_lib.arguments.getResolvedOptions",
        return_value=return_dict,
    )


def test_arguments_succeeds(mocker):
    mock_glue_resolved_arguments(mocker, {"custom_option": "value"})

    @dataclasses.dataclass
    class TstArguments(arguments.Arguments):
        custom_option: str

    args = TstArguments.from_glue_arguments()
    assert args.custom_option == "value"


def test_arguments_succeeds_with_builtin_argument(mocker):
    mock_glue_resolved_arguments(
        mocker, {"JOB_NAME": "name", "custom_option": "value"}
    )

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

    with pytest.raises(KeyError):
        TstArguments.from_glue_arguments()


def test_argument_is_parsed_with_parse_fun(mocker):
    test_arg_name = "list_of_strings"
    mock_glue_resolved_arguments(
        mocker, {"JOB_NAME": "name", test_arg_name: "a,b,c"}
    )

    @dataclasses.dataclass
    class TstArguments(arguments.Arguments):
        JOB_NAME: str
        list_of_strings: typing.List[str]

        @classmethod
        def parse_args_funs(cls):
            return {test_arg_name: lambda x: x.split(",")}

    args = TstArguments.from_glue_arguments()
    assert args.list_of_strings == ["a", "b", "c"]
