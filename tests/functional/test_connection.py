import asyncio
import time
from unittest import mock

import pytest

from tamarco_kafka import settings
from tamarco_kafka.exceptions import KafkaConnectionError
from tamarco_kafka.input import KafkaInput
from tamarco_kafka.output import KafkaOutput


class ConnectionAsyncMock(mock.MagicMock):
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


def sleep_and_raise_socket_error(*args, **kargs):
    time.sleep(settings.CONNECTION_TIMEOUT_SECONDS)
    raise KafkaConnectionError


@pytest.mark.asyncio
async def test_kafka_reconnection_success(conn, event_loop):
    await conn.connect(reconnect=True)
    assert conn.is_connected()
    await conn.close()


@pytest.mark.asyncio
async def test_connection(conn):
    await conn.connect()
    assert conn.is_connected()
    await conn.close()
    assert not conn.is_connected()


@pytest.mark.asyncio
async def test_publish_without_connecting():
    client = KafkaOutput(topic="test")

    with pytest.raises(Exception):
        await client.push("boba", "turtles")


@pytest.mark.asyncio
async def test_bad_register(kafka_connection):
    def bad_consumer():
        pass

    with pytest.raises(Exception):
        await kafka_connection.register(bad_consumer)


@pytest.mark.asyncio
async def test_register_without_connecting(conn, task_manager, event_loop):
    result_fut = asyncio.Future(loop=event_loop)

    @KafkaInput(topic="cats")
    async def cat_input(message):
        result_fut.set_result("meow meow")

    cat_output = KafkaOutput(topic="cats")

    await conn.register(cat_input, cat_output)
    await conn.connect()

    task_manager.start_task("consumer", cat_input.callback_trigger(task_manager))

    await cat_output.push("test")
    result = await result_fut

    assert result == "meow meow"


@pytest.mark.asyncio
async def test_unregister_without_connecting(conn):
    @KafkaInput("cat")
    async def cat_consumer(message):
        pass

    await conn.register(cat_consumer)
    await conn.unregister(cat_consumer)
    assert cat_consumer not in conn.inputs


@pytest.mark.asyncio
async def test_unregister_with_connecting(conn):
    @KafkaInput("cat")
    async def cat_consumer(message):
        pass

    await conn.connect()
    await conn.register(cat_consumer)

    await conn.unregister(cat_consumer)

    assert cat_consumer not in conn.inputs


@pytest.mark.asyncio
async def test_publish_without_connecting2(conn):
    with pytest.raises(Exception):
        await conn._publish(None, None)


@pytest.mark.asyncio
async def test_register_invalid(kafka_connection):
    with pytest.raises(Exception):
        await kafka_connection.register(1)


@pytest.mark.asyncio
async def test_start_producer_twice(conn):
    await conn._start_producer()
    with pytest.raises(Exception):
        await conn._start_producer()


@pytest.mark.asyncio
async def test_stop_producer(conn):
    await conn._start_producer()
    await conn._stop_producer()


@pytest.mark.asyncio
async def test_connect_twice(kafka_connection):
    with pytest.raises(Exception):
        await kafka_connection.connect()


@pytest.mark.asyncio
async def test_connect_without_reconnecting(conn):
    await conn.connect(reconnect=False)
    await conn.close()


@pytest.mark.asyncio
async def test_connect_to_kafka_broker_twice(conn):
    await conn._connect_to_kafka_broker()
    await conn._connect_to_kafka_broker()
    await conn.close()


@pytest.mark.asyncio
async def test_close_without_connecting(conn):
    with pytest.raises(Exception):
        await conn.close()
        await conn.close()


@pytest.mark.asyncio
async def test_kafka_producer_thread_start_twice(producer_thread):
    await producer_thread.start()
    with pytest.raises(Exception):
        await producer_thread.start()


@pytest.mark.asyncio
async def test_kafka_producer_thread_stop_without_starting(producer_thread):
    with pytest.raises(Exception):
        await producer_thread.stop()
