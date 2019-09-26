import asyncio
import subprocess
from time import sleep

import pytest
from tamarco.core.tasks import TasksManager

from tamarco_kafka.connection import KafkaConnection, ProducerThread
from tamarco_kafka.input import KafkaInput
from tamarco_kafka.output import KafkaGenericOutput
from tamarco_kafka.resource import KafkaResource

bootstrap_servers = "127.0.0.1:9092"
batch_num_messages = 1


@pytest.fixture()
def kafka_config(event_loop):
    return {"bootstrap_servers": bootstrap_servers, "batch.num.messages": batch_num_messages, "loop": event_loop}


@pytest.fixture
def kafka_connection(kafka_config, event_loop):
    conn = KafkaConnection(**kafka_config)
    event_loop.run_until_complete(conn.connect())

    yield conn

    if conn.is_connected():
        event_loop.run_until_complete(conn.close())
    if conn.producer_thread_started:
        event_loop.run_until_complete(conn._stop_producer())


@pytest.fixture
def conn(kafka_config, event_loop):
    conn = KafkaConnection(**kafka_config)

    yield conn

    if conn.is_connected():
        event_loop.run_until_complete(conn.close())
    if conn.producer_thread_started:
        event_loop.run_until_complete(conn._stop_producer())


@pytest.fixture
def producer_thread(kafka_config, event_loop):
    producer_thread = ProducerThread(conf={}, loop=event_loop)

    yield producer_thread

    if producer_thread._started:
        event_loop.run_until_complete(producer_thread.stop())


@pytest.fixture
def task_manager(event_loop):
    task_manager = TasksManager()
    task_manager.set_loop(event_loop)

    yield task_manager

    task_manager.stop_all()


@pytest.fixture
def kafka_resource(event_loop):
    kafka_resource = KafkaResource()
    kafka_resource.loop = event_loop

    yield kafka_resource

    event_loop.run_until_complete(kafka_resource.stop())


@pytest.fixture
def kafka_output(kafka_connection, event_loop):
    kafka_output = KafkaGenericOutput()
    event_loop.run_until_complete(kafka_connection.register(kafka_output))
    return kafka_output


@pytest.fixture
def kafka_input_queues():
    return ["test"]


@pytest.fixture
def kafka_input_messages(task_manager, kafka_input_queues, kafka_connection, event_loop):
    messages = {queue: asyncio.Queue(loop=event_loop) for queue in kafka_input_queues}

    for kafka_queue_name, async_queue in messages.items():

        @KafkaInput(queue=kafka_queue_name)
        async def kafka_input(message):
            await async_queue.put(message)

        event_loop.run_until_complete(kafka_connection.register(kafka_input))
        task_manager.start_task(f"callback_trigger_{kafka_queue_name}", kafka_input.callback_trigger(task_manager))

    return messages


def local_command(command):
    print(f"\nLaunching command: {command}")
    process = subprocess.Popen(command, shell=True)
    return_code = process.wait()
    return process, return_code


def docker_compose_up():
    print("Bringing up the docker compose")
    command = f"docker-compose up -d"
    _, return_code = local_command(command)
    if return_code != 0:
        pytest.fail(msg="Failed setting up the containers.")


def docker_compose_down():
    print("Removing all containers")
    command = f"docker-compose kill && docker-compose down"
    _, return_code = local_command(command)
    if return_code != 0:
        print("Warning: Error stopping all the containers.")
    else:
        print("Removed docker containers.")


@pytest.fixture(scope="session", autouse=True)
def docker_compose():
    docker_compose_up()
    sleep(5)
    yield
    docker_compose_down()
