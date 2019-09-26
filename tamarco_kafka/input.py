from tamarco.core.utils import set_thread_name

try:
    from confluent_kafka import Consumer, KafkaError, KafkaException
except ImportError:  # pragma: no cover
    import logging

    logging.getLogger("tamarco.init").error(
        "Tamarco don't have support for kafka, please reinstall tamarco with support for it"
        "   > pip install tamarco[kafka]"
    )
    raise

import asyncio  # noqa: I100
import logging
import uuid
from queue import Empty, Full, Queue
from threading import Event, Thread

from tamarco.resources.bases import InputBase
from tamarco_kafka.helpers import AsyncEvent
from tamarco_kafka.settings import THREADING_QUEUE_SIZE

logger = logging.getLogger("tamarco.kafka")


class ConsumerThread:
    def __init__(self, topic, group_id, on_error_callback=None):
        self.topic = topic
        self.group_id = group_id
        self.connection = None
        self.thread = None
        self._stop = Event()
        self.exception = None
        self._started = False
        self.start_event = AsyncEvent()
        self.stop_event = None
        self.thread_supervisor_task = None
        self._error_event = AsyncEvent()
        self.on_error_callback = on_error_callback
        self.loop = asyncio.get_event_loop()
        self.thread_communication_queue = Queue(THREADING_QUEUE_SIZE)

    def set_connection(self, connection):
        """Set the KafkaConnection that the Thread should use for consume messages. Build method.

        Args:
            connection (KafkaConnection): Connection to use in the consumer thread.
        """
        self.connection = connection

    def set_loop(self, loop):
        self.loop = loop

    def get_conf(self):
        """Returns the configuration of the consumer."""
        assert self.connection, "Set the connection is needed before get_configuration"
        configuration = {
            "group.id": self.group_id,
            "default.topic.config": {"auto.offset.reset": "smallest"},
            "bootstrap.servers": self.connection.bootstrap_servers,
            "api.version.request": True,
            "client.id": self.connection.client_id,
            "auto.commit.interval.ms": 1_000,
            "queue.buffering.max.ms": 1_000,
            "batch.num.messages": 100_000,
            "queue.buffering.max.messages": 1_000_000,
        }
        if self.on_error_callback:
            configuration.update({"error_cb": self.on_error_callback})
        return configuration

    def _confluent_kafka_error_cb(self, kafka_error):
        """Callback called in case of error.

        Args:
            kafka_error:
        """
        self.exception = kafka_error
        self._error_event.set()

    async def start(self):
        """Starts consumer thread."""
        logger.debug("Starting Kafka consumer")
        assert self.connection is not None, "Set the connection is needed before the start"
        if not self._started:
            self.stop_event = AsyncEvent()
            self.thread = Thread(target=self._consumer_thread, name=f"KC_{self.topic}")
            self.thread.start()
            await self.start_event.wait()
            self._started = True
            self.start_thread_supervisor_task()
            logger.debug(f"Started thread and task of Kafka handler: {self}")
        else:
            raise Exception("Called ProducerThread start method twice without calling stop method")

    async def thread_supervisor(self):
        await self._error_event.wait()
        logger.warning(
            "Kafka thread supervisor detected a error. Calling on_error_callback. Possible exception: "
            f"{self.exception}"
        )
        self.on_error_callback(self.exception)

    def start_thread_supervisor_task(self):
        if self.on_error_callback:
            logger.debug(
                f"Starting thread supervisor in Kafka consumer. Topic: {self.topic}. Group_id: " f"{self.group_id}"
            )
            self.thread_supervisor_task = asyncio.ensure_future(self.thread_supervisor())

    async def stop(self):
        """Stop consumer Thread."""
        logger.debug("Stopping Kafka consumer")
        assert self.connection is not None, "Set the connection is needed before the stop"
        if self._started:
            self._stop.set()
            await self.stop_event.wait()
            logger.debug(f"Closed Kafka thread of consumer {self}. Topic: {self.topic}. Group_id: {self.group_id}")
            self.start_event = AsyncEvent()
            self._started = False
            if self.thread_supervisor_task:
                self.thread_supervisor_task.cancel()
                logger.debug(
                    f"Cancelled thread supervisor task in consumer Kafka thread: {self}. Topic: {self.topic}. "
                    f"Group_id: {self.group_id}"
                )
        else:
            raise Exception("Called ProducerThread stop method twice without calling start method")

    def _build_kafka_consumer(self):
        """Setup of the kafka consumer."""
        try:
            consumer = Consumer(self.get_conf())
            consumer.subscribe([self.topic])
            consumer.assignment()
        except KafkaException:
            logger.warning(f"Error connecting to the Kafka consumer thread: {self}")
            raise
        else:
            return consumer

    def _consumer_thread(self):
        """Thread that consumes from the broker and enqueue messages to the thread communication Queue."""
        set_thread_name()
        consumer = self._build_kafka_consumer()
        self.start_event.set()

        try:
            while not self._stop.is_set():
                msg = consumer.poll(timeout=0.1)
                if msg is not None:  # None == timeout
                    if not msg.error():
                        msg_value = msg.value()
                        logger.debug(
                            "Received message in Kafka input thread. Sending it to the main thread via " "queue.Queue()"
                        )
                        try:
                            self.thread_communication_queue.put(msg_value, timeout=0.1)
                        except Full:
                            self.retry_put_message(msg_value)
                    elif msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.warning(msg.error())
        except Exception as e:
            self.exception = e
            logger.warning(f"Unexpected exception in Kafka consumer thread: {self}")
        finally:
            consumer.close()
            self.stop_event.set()
            logger.debug(f"Closed Kafka consumer thread: {self}")

    def retry_put_message(self, msg_value):
        while True:
            try:
                self.thread_communication_queue.put(msg_value, timeout=0.1)
                break
            except Full:
                if self._stop.is_set():
                    raise Exception("Close thread")


class KafkaInput(InputBase):
    """Kafka stream of messages.
    It is an asynchronous iterator, each iteration this object provides a new message.
    It also can be used opening a new task for each new message, with the callback trigger.
    """

    def __init__(self, topic: str, group_id: str = None, *args, **kwargs):
        self.topic = topic
        self._group_id = group_id
        if "name" not in kwargs:
            kwargs["name"] = topic
        self.connection = None
        self.consumer_thread = None
        self.on_error_callback = None
        self.loop = asyncio.get_event_loop()
        super().__init__(*args, **kwargs)

    @property
    def group_id(self):
        """Kafka Group id used for consume messages."""
        if self._group_id:
            return self._group_id
        elif self.resource and self.resource.microservice:
            return self.resource.microservice.name
        else:
            return "default"

    def set_connection(self, connection):
        self.connection = connection

    def set_loop(self, loop):
        self.loop = loop

    def set_on_error_callback(self, error_callback):
        self.on_error_callback = error_callback

    def __call__(self, *args, **kwargs):
        """Allow the input to work as a decorator."""
        super().__call__(*args, **kwargs)
        if not self.resource:
            logger.warning("Used KafkaInput as a decorator without specifying the resource")
        return self

    async def __aiter__(self):
        """Allow the input to work as an asynchronous iterator."""
        if self.consumer_thread:
            while not self.consumer_thread._stop.is_set():
                try:
                    message = await self.consume_message_from_thread()
                except Empty:
                    await asyncio.sleep(0.1)
                else:
                    logger.debug(f"Received message from topic: {self.topic}. Message: {message}")
                    try:
                        yield self.decode_message(message)
                    except Exception:
                        logger.warning(f"Rejected message {message} because it cannot be decoded", exc_info=True)

    async def consume_message_from_thread(self):
        return self.consumer_thread.thread_communication_queue.get_nowait()

    def decode_message(self, message):
        if self.codec:
            return self.codec.decode(message)
        else:
            return message.decode("utf-8")

    async def _start_consumer_thread(self):
        self.consumer_thread = ConsumerThread(
            topic=self.topic, group_id=self.group_id, on_error_callback=self.on_error_callback
        )
        self.consumer_thread.set_connection(self.connection)
        self.consumer_thread.set_loop(self.loop)
        await self.consumer_thread.start()

    async def start(self):
        await self._start_consumer_thread()

    async def stop(self):
        await self.consumer_thread.stop()

    async def callback_trigger(self, task_manager):
        async for message in self:
            task_name = "message_handler_" + str(uuid.uuid4())
            if self.resource:
                await task_manager.wait_for_start_task(
                    task_name, self.on_message_callback(self.resource.microservice, message)
                )
            else:
                await task_manager.wait_for_start_task(task_name, self.on_message_callback(message))

    def __repr__(self):
        return f"KafkaInput. Topic: {self.topic}. Group_id: {self.group_id}."
