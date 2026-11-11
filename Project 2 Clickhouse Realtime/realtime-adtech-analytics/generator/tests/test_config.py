import pytest

from adgen.config import GeneratorConfig


def test_from_env_and_per_worker_rate():
    cfg = GeneratorConfig.from_env({"EVENTS_PER_SECOND": "10000", "GENERATOR_WORKERS": "4", "CTR": "0.02"})
    assert cfg.events_per_second_per_worker == 2500
    assert cfg.ctr == 0.02


@pytest.mark.parametrize("env", [{"GENERATOR_WORKERS": "0"}, {"WIN_RATE": "1.5"}, {"EVENTS_PER_SECOND": "-1"}])
def test_rejects_invalid(env):
    with pytest.raises(ValueError):
        GeneratorConfig.from_env(env)
