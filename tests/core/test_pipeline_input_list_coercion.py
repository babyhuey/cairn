"""#197 — a `list` input takes a comma-separated string at the launch door.

`-i report_to=mlflow` was refused for an input declared `type: list`: the value had
to be shell-quoted JSON. The door's typing function (`coerce_declared_inputs`, the
one place the CLI, the console, cron and presets converge) now splits a plain
string on commas, and a value that parses as JSON keeps the JSON rule.
"""
from __future__ import annotations

import pytest

from cairn.core.dispatch.pipeline_launch import InputCoercionError, coerce_declared_inputs
from cairn.core.engine.pipeline import parse_pipeline_def

_DEF = """\
name: tracked
kind: pipeline
description: a list input
inputs:
  report_to: {type: list, default: [mlflow]}
  target_modules: {type: list, default: [q_proj]}
  epochs: {type: int, default: 1}
steps:
  - id: run
    template: echo
"""


@pytest.fixture
def d():
    return parse_pipeline_def(_DEF, pack="scratch")


@pytest.mark.parametrize("raw, typed", [
    ("mlflow", ["mlflow"]),
    ("mlflow,wandb", ["mlflow", "wandb"]),
    (" mlflow , wandb ,, ", ["mlflow", "wandb"]),
    ("", []),
    ('["mlflow"]', ["mlflow"]),               # the JSON form is unchanged
    ('["mlflow", "wandb"]', ["mlflow", "wandb"]),
])
def test_a_list_input_takes_a_comma_separated_string(d, raw, typed):
    assert coerce_declared_inputs(d, {"report_to": raw})["report_to"] == typed


def test_a_value_that_parses_as_json_keeps_the_json_rule(d):
    for raw in ('"mlflow"', "1", "true", "{}"):
        with pytest.raises(InputCoercionError, match="declared list"):
            coerce_declared_inputs(d, {"report_to": raw})


def test_broken_json_is_refused_not_split(d):
    with pytest.raises(InputCoercionError, match="report_to"):
        coerce_declared_inputs(d, {"report_to": '["mlflow",'})


def test_typed_values_and_other_types_are_untouched(d):
    out = coerce_declared_inputs(d, {"report_to": ["wandb"], "epochs": "3",
                                     "target_modules": "q_proj,v_proj"})
    assert out == {"report_to": ["wandb"], "epochs": 3,
                   "target_modules": ["q_proj", "v_proj"]}
