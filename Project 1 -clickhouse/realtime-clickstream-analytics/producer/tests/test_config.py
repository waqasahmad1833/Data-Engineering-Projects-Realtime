import pytest

from clickstream_producer.config import ProducerConfig


def test_defaults_when_env_is_empty():
    config = ProducerConfig.from_env({})
    assert config.topic == "clickstream.events"
    assert config.events_per_second == 50.0


def test_reads_values_from_env():
    config = ProducerConfig.from_env({
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "KAFKA_TOPIC": "events",
        "EVENTS_PER_SECOND": "200",
        "INVALID_EVENT_RATIO": "0.1",
        "LOG_LEVEL": "debug",
    })
    assert config.bootstrap_servers == "kafka:9092"
    assert config.topic == "events"
    assert config.events_per_second == 200.0
    assert config.invalid_event_ratio == 0.1
    assert config.log_level == "DEBUG"


def test_blank_values_fall_back_to_defaults():
    assert ProducerConfig.from_env({"KAFKA_TOPIC": "  "}).topic == "clickstream.events"


@pytest.mark.parametrize("env", [
    {"EVENTS_PER_SECOND": "0"},
    {"INVALID_EVENT_RATIO": "2"},
    {"MAX_LATE_SECONDS": "3"},
])
def test_rejects_invalid_values(env):
    with pytest.raises(ValueError):
        ProducerConfig.from_env(env)
