from unittest import mock

import pytest

from tamarco_kafka.input import KafkaInput
from tamarco_kafka.output import KafkaOutput
from tamarco_kafka.resource import KafkaResource
from tests.utils import AsyncMock, AsyncDict


@pytest.fixture
def kafka_resource():
    return KafkaResource()


def test_add_inputs():
    inputs = [KafkaInput(topic="cats"), KafkaInput(topic="dogs")]
    kafka_resource = KafkaResource(inputs=inputs)
    assert [input_value in inputs for input_value in kafka_resource.inputs]


def test_add_outputs():
    outputs = [KafkaOutput(topic="cats"), KafkaOutput(topic="dogs")]
    kafka_resource = KafkaResource(outputs=outputs)
    assert [output in outputs for output in kafka_resource.outputs]


def test_add_inputs_decorator():
    kafka_resource = KafkaResource()

    @KafkaInput(topic="cats", resource=kafka_resource)
    def handle_cats(message):
        pass

    @KafkaInput(topic="dogs", resource=kafka_resource)
    def handle_dogs(message):
        pass

    assert handle_cats in kafka_resource.inputs.values()
    assert handle_dogs in kafka_resource.inputs.values()


@pytest.mark.asyncio
async def test_get_confluent_kafka_settings():
    kafka_resource = KafkaResource()
    settings = mock.MagicMock()
    settings.get = AsyncMock()
    kafka_resource.settings = settings
    settings_response = await kafka_resource.get_confluent_kafka_settings()
    assert isinstance(settings_response, dict)


@pytest.mark.asyncio
async def test_connect_to_kafka_invalid_settings():
    kafka_resource = KafkaResource()
    invalid_settings = "jfhf"
    with pytest.raises(Exception):
        await kafka_resource.connect_to_kafka(invalid_settings)


@pytest.mark.asyncio
async def test_consumer_offset():
    kafka_resource = KafkaResource(offset_reset=True)
    kafka_resource.settings = AsyncDict()
    kafka_resource.settings["bootstrap_servers"] = "127.0.0.1"
    settings = await kafka_resource.get_confluent_kafka_settings()
    assert "auto.offset.reset" in settings


@pytest.mark.asyncio
async def test_consumer_offset_false():
    kafka_resource = KafkaResource(offset_reset=False)
    kafka_resource.settings = AsyncDict()
    kafka_resource.settings["bootstrap_servers"] = "127.0.0.1"
    settings = await kafka_resource.get_confluent_kafka_settings()
    assert "auto.offset.reset" not in settings
