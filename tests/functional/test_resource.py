import pytest
from tamarco.resources.basic.status.status_codes import StatusCodes

from tamarco_kafka.input import KafkaInput
from tests.functional.conftest import bootstrap_servers


@pytest.mark.asyncio
async def test_start_and_stop(kafka_resource):
    async def settings_method():
        return {"bootstrap_servers": bootstrap_servers}

    @KafkaInput(topic="start_and_stop", resource=kafka_resource)
    async def consume_cats(message):
        pass

    kafka_resource.get_confluent_kafka_settings = settings_method

    await kafka_resource.start()
    await kafka_resource.post_start()


@pytest.mark.asyncio
async def test_status_code_pre_start(kafka_resource):
    status = await kafka_resource.status()
    assert isinstance(status, dict)
    assert status == {"status": StatusCodes.NOT_STARTED}


@pytest.mark.asyncio
async def test_status_code_start(kafka_resource):
    async def settings_method():
        return {"bootstrap_servers": bootstrap_servers}

    @KafkaInput(topic="status_code_start", resource=kafka_resource)
    async def consume_cats(message):
        pass

    kafka_resource.get_confluent_kafka_settings = settings_method

    await kafka_resource.start()
    status = await kafka_resource.status()

    assert isinstance(status, dict)
    assert status["status"] == StatusCodes.STARTED


@pytest.mark.asyncio
async def test_status_code_stop(kafka_resource):
    async def settings_method():
        return {"bootstrap_servers": bootstrap_servers}

    @KafkaInput(topic="status_code_stop", resource=kafka_resource)
    async def consume_cats(message):
        pass

    kafka_resource.get_confluent_kafka_settings = settings_method

    await kafka_resource.start()
    await kafka_resource.stop()
    status = await kafka_resource.status()

    assert isinstance(status, dict)
    assert status["status"] == StatusCodes.STOPPED
