import asyncio

import pytest

from tamarco_kafka.input import KafkaInput


@pytest.mark.asyncio
async def test_input_async_iteration(task_manager, kafka_connection, kafka_output, event_loop):
    topic = "test"

    kafka_input = KafkaInput(topic=topic)
    await kafka_connection.register(kafka_input)

    received_message = asyncio.Future(loop=event_loop)

    async def async_iteration():
        async for message in kafka_input:
            received_message.set_result(message)

    task_manager.start_task("async_iteration", async_iteration())

    message = "wof wof"
    await kafka_output.push(message, topic)

    result = await received_message

    assert result == message


@pytest.mark.asyncio
async def test_input_decorator(task_manager, kafka_connection, kafka_output):
    turtles = []
    fut = asyncio.Future()

    @KafkaInput(topic="turtles")
    async def turtles_consumer(message):
        turtles.append(message)
        fut.set_result(message)

    await kafka_connection.register(turtles_consumer)
    task_manager.start_task("turtles_consumer", turtles_consumer.callback_trigger(task_manager))

    message = "boba"

    await kafka_output.push(message, "turtles")

    result = await fut

    assert len(turtles) == 1
    assert turtles[0] == message
    assert result == message
