from unittest import mock

import pytest

from tamarco_kafka.input import KafkaInput
from tests.utils import AsyncMock


@pytest.mark.asyncio
async def test_decorator_bad_call():
    with pytest.raises(Exception):

        @KafkaInput()
        def turtles_consumer(self, **kwargs):
            pass


@pytest.mark.asyncio
async def test_input_without_topic():
    with pytest.raises(TypeError):
        KafkaInput()


@pytest.mark.asyncio
async def test_input_decorator_without_topic():
    with pytest.raises(TypeError):

        @KafkaInput()
        def kafka_input(message):
            pass


def test_group_id():
    group_id = "cats_group"

    kafka_input = KafkaInput(topic="cats")
    assert kafka_input.group_id == "default"

    kafka_input = KafkaInput(topic="cats", group_id=group_id)
    assert kafka_input.group_id == "cats_group"

    resource = mock.MagicMock()
    resource.microservice = mock.MagicMock()
    resource.microservice.name = group_id
    kafka_input = KafkaInput(topic="cats", resource=resource)
    assert kafka_input.group_id == "cats_group"


def test_input_as_decorator_without_queue():

    queue = "cats"

    @KafkaInput(topic=queue)
    def handle_message_from_cats(message):
        pass

    assert callable(handle_message_from_cats.on_message_callback)
    assert handle_message_from_cats.on_message_callback.__name__ == "handle_message_from_cats"


@pytest.fixture
def consumer_thread():
    from tamarco_kafka.input import ConsumerThread

    queue = "cats"
    group_id = "default"
    consummer_thread = ConsumerThread(topic=queue, group_id=group_id)
    return consummer_thread


def test_set_connection(consumer_thread):
    connection = mock.MagicMock()
    consumer_thread.set_connection(connection)
    assert consumer_thread.connection == connection


def test_get_conf(consumer_thread):
    connection = mock.MagicMock()
    consumer_thread.connection = connection
    configuration = consumer_thread.get_conf()
    assert isinstance(configuration, dict)


@pytest.mark.asyncio
async def test_start(consumer_thread):
    connection = mock.MagicMock()
    consumer_thread.set_connection(connection)
    with mock.patch("threading.Thread.start") as Thread:
        consumer_thread.start_event = AsyncMock()
        await consumer_thread.start()
        Thread.assert_called_once()


@pytest.mark.asyncio
async def test_start_without_connection(consumer_thread):
    with mock.patch("threading.Thread"):
        with pytest.raises(AssertionError):
            await consumer_thread.start()


@pytest.mark.asyncio
async def test_start_twice(consumer_thread):
    connection = mock.MagicMock()
    consumer_thread.set_connection(connection)
    with mock.patch("threading.Thread.start"):
        consumer_thread.start_event = AsyncMock()
        await consumer_thread.start()
        with pytest.raises(Exception):
            await consumer_thread.start()


@pytest.mark.asyncio
async def test_stop(consumer_thread):
    connection = mock.MagicMock()
    consumer_thread.set_connection(connection)
    consumer_thread._started = True
    consumer_thread.stop_event = AsyncMock()
    await consumer_thread.stop()
    assert not consumer_thread._started
    assert consumer_thread._stop.is_set()


@pytest.mark.asyncio
async def test_stop_without_connection(consumer_thread):
    with mock.patch("threading.Thread"):
        with pytest.raises(AssertionError):
            await consumer_thread.stop()


@pytest.mark.asyncio
async def test_stop_twice(consumer_thread):
    connection = mock.MagicMock()
    consumer_thread.set_connection(connection)
    consumer_thread.stop_event = AsyncMock()
    consumer_thread._started = True
    await consumer_thread.stop()
    with pytest.raises(Exception):
        await consumer_thread.stop()


def test_setup_kafka_consumer(consumer_thread):
    consumer_thread.get_conf = mock.MagicMock()
    consumer_thread.queue = mock.MagicMock()
    with mock.patch("tamarco_kafka.input.Consumer"):
        consumer = consumer_thread._build_kafka_consumer()
        assert consumer == consumer
