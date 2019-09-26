import asyncio

import pytest
from tamarco.codecs.json import JsonCodec

from tamarco_kafka.input import KafkaInput
from tamarco_kafka.output import KafkaOutput, KafkaGenericOutput


@pytest.mark.asyncio
async def test_kafka_output(kafka_connection, task_manager):
    result_fut = asyncio.Future()

    @KafkaInput(topic="test_output", codec=JsonCodec)
    async def cat_server(message):
        result_fut.set_result(message)

    cat_client = KafkaOutput(topic="test_output", codec=JsonCodec)
    await kafka_connection.register(cat_server, cat_client)

    task_manager.start_task("callback_trigger", cat_server.callback_trigger(task_manager))

    test_message = {"test": "message"}
    await cat_client.push(test_message)

    result = await result_fut

    assert result == test_message


@pytest.mark.skip(reason="no way of currently testing this")
@pytest.mark.asyncio
async def test_kafka_generic_output(event_loop, kafka_connection, task_manager):
    result_fut = asyncio.Future(loop=event_loop)

    @KafkaInput(topic="dog", codec=JsonCodec)
    async def cat_server(message):
        result_fut.set_result(message)

    cat_client = KafkaGenericOutput(codec=JsonCodec)
    await kafka_connection.register(cat_server, cat_client)

    task_manager.start_task("callback_trigger", cat_server.callback_trigger(task_manager))

    test_message = {"test": "message"}
    await cat_client.push(test_message, topic="dog")

    assert await asyncio.wait_for(result_fut, 20) == test_message
