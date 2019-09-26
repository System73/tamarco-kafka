from tamarco.core.utils import set_thread_name

try:
    from confluent_kafka import Producer
except ImportError:  # pragma: no cover
    import logging

    logging.getLogger("tamarco.init").error(
        "Tamarco don't have support for kafka, please reinstall tamarco with support for it"
        "   > pip install tamarco[kafka]"
    )
    raise

import asyncio  # noqa: I100
import logging
import time
import uuid
from queue import Empty, Full, Queue
from threading import Event, Thread

from tamarco_kafka.exceptions import KafkaConnectionError
from tamarco_kafka.helpers import AsyncEvent
from tamarco_kafka.settings import CONNECTION_TIMEOUT_SECONDS, KAFKA_POLL_TRIGGER, RECONNECTION_TIMEOUT_SECONDS
from tamarco_kafka.settings import THREADING_QUEUE_SIZE

logger = logging.getLogger("tamarco.kafka")


class ProducerThread:
    def __init__(self, conf, loop, on_error_callback=None):
        super().__init__()
        self.loop = loop
        self.conf = conf
        self._stop = Event()
        self.start_event = AsyncEvent()
        self.stop_event = None
        self._error_event = AsyncEvent()
        self.thread = None
        self.exception = None
        self._started = False
        self.poll_counter = 0
        self.thread_supervisor_task = None
        self.on_error_callback = on_error_callback
        self.thread_communication_queue = Queue(THREADING_QUEUE_SIZE)
        if self.on_error_callback:
            self.conf.update({"error_cb": self._confluent_kafka_error_callback})

    def _confluent_kafka_error_callback(self, kafka_error):
        self.exception = kafka_error
        self._error_event.set()

    def run_thread(self):
        set_thread_name()
        kafka_producer = Producer(self.conf)
        self.start_event.set()
        while not self._stop.is_set():
            try:
                topic, message = self.thread_communication_queue.get(timeout=0.1)
                kafka_producer.produce(topic, message)
                self.poll(kafka_producer)
            except Empty:
                pass
            except Exception:
                logger.warning(
                    "Unexpected exception consuming messages from the main process in Kafka producer", exc_info=True
                )
        else:
            self.stop_event.set()
            logger.info("Closed kafka producer thread")

    def poll(self, producer):
        self.poll_counter += 1
        if self.poll_counter > KAFKA_POLL_TRIGGER:
            producer.poll(0)
            self.poll_counter = 0

    async def thread_supervisor(self):
        await self._error_event.wait()
        logger.warning(
            f"Kafka thread supervisor task detected a error in Kafka. Calling on_error_callback. Possible "
            f"exception: {self.exception}"
        )
        self.on_error_callback(self.exception)

    def start_thread_supervisor_task(self):
        if self.on_error_callback:
            logger.info("Starting thread supervisor task in Kafka producer")
            self.thread_supervisor_task = asyncio.ensure_future(self.thread_supervisor(), loop=self.loop)

    async def stop(self):
        """Stops the producer Thread."""
        logger.debug(f"Stopping thread and task of producer: {self}")
        if self._started:
            self._stop.set()
            await self.stop_event.wait()
            self.start_event = AsyncEvent()
            self._started = False
            if self.thread_supervisor_task:
                self.thread_supervisor_task.cancel()
            logger.debug(f"Stopped thread and task of Kafka producer: {self}")
        else:
            raise Exception("Called ProducerThread stop method twice without calling start method")

    async def start(self):
        """Starts the producer Thread with some initialization."""
        logger.debug(f"Starting thread and task of producer {self}")
        if not self._started:
            self.stop_event = AsyncEvent()
            self._stop = Event()
            self.thread = Thread(target=self.run_thread, name="KafkaProducer")
            self.thread.start()
            await self.start_event.wait()
            self._started = True
            self.start_thread_supervisor_task()
            logger.debug(f"Started thread and task of Kafka producer: {self}")
        else:
            raise Exception("Called ProducerThread start method twice without calling stop method")


class KafkaConnection:
    """Class that handles the connection with the kafka broker."""

    def __init__(self, bootstrap_servers=None, client_id=None, on_error_callback=None, loop=None, **kwargs):
        """
        Initialization of the parameters of the connection.

        Args:
            bootstrap_servers: Kafka servers to connect.
            loop: Asyncio loop if needed, the default is asyncio current loop.
        """
        self.loop = loop if loop else asyncio.get_event_loop()
        self.bootstrap_servers = bootstrap_servers if bootstrap_servers else "127.0.0.1"
        self.confluent_conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "api.version.request": True,
            "queue.buffering.max.ms": 100,
            "batch.num.messages": 10_000,
            "queue.buffering.max.messages": 10_000,
        }
        self.confluent_conf.update(kwargs)
        self.client_id = client_id if client_id else str(uuid.uuid4())
        self._connected = False
        self.inputs = []
        self.producer_thread = None
        self.producer_thread_started = False
        self.on_error_callback = on_error_callback

    def is_connected(self):
        """Returns true if the connection is open, else False."""
        return self._connected

    async def register(self, *io_streams):
        """Register as consumers the Inputs, once registered they start to consume messages.

        Args:
            *io_streams: Handlers to register.
        """
        from tamarco_kafka.input import KafkaInput
        from tamarco_kafka.output import KafkaOutput, KafkaGenericOutput

        for io_stream in io_streams:
            logger.info(f"Registering IO stream: {io_stream}")
            if isinstance(io_stream, KafkaInput):
                io_stream.set_connection(self)
                io_stream.set_loop(self.loop)
                io_stream.set_on_error_callback(self.on_error_callback)
                self.inputs.append(io_stream)
                if self._connected:
                    await io_stream.start()
            elif isinstance(io_stream, (KafkaOutput, KafkaGenericOutput)):
                io_stream.set_connection(self)
            else:
                error_message = f"Invalid Kafka input: {io_stream}"
                logger.warning(error_message)
                raise Exception(error_message)

    async def unregister(self, *inputs):
        """Unregister the inputs, once unregistered they stop consuming messages.

        Args:
            *inputs: Inputs to unregister
        """
        for input_element in inputs:
            logger.info(f"Unregistering Kafka handler: {input_element}")
            self.inputs.remove(input_element)
            if self._connected:
                await input_element.stop()

    async def _publish(self, topic, msg):
        """Handles the publishing of messages to the broker.
        Basic interface of AMQP for publishing.

        Args:
            topic: Topic to publish.
            msg: Message to publish in topic.
        """
        if self._connected:
            logger.debug(f"Publishing to topic {topic} the message: {msg}")
            try:
                self.producer_thread.thread_communication_queue.put_nowait((topic, msg))
            except Full:
                await self.wait_and_retry_publish(topic, msg, wait_time=0.1)
        else:
            raise Exception("Cannot publish a message because there is no connection")

    async def wait_and_retry_publish(self, topic, msg, wait_time):
        while True:
            await asyncio.sleep(wait_time)
            try:
                self.producer_thread.thread_communication_queue.put_nowait((topic, msg))
            except Full:
                logger.warning(
                    f"Kafka producer queue for thread communication is full, waiting {wait_time} seconds " f"to retry"
                )
            else:
                break

    async def close(self):
        """Close the connection with the broker.
        The coroutine waits until the connection is closed.
        """
        logger.debug("Closing connection.")
        if self._connected:
            await self._stop_producer()
            for handler in self.inputs:
                await handler.stop()
            self._connected = False
            logger.debug("Closed Kafka connection.")
        else:
            raise Exception("Trying to close a Kafka connection that is not open")

    async def connect(self, reconnect=True, reconnect_timeout=RECONNECTION_TIMEOUT_SECONDS):
        """Connects to the broker.

        Args:
            reconnect (bool): Try to reconnect when a connection fails.
            reconnect_timeout (int): Timeout of the reconnect.

        Raises:
            ConnectionError: The connection can't be stabilized.
        """
        if not self._connected:
            if reconnect:
                await self._reconnect(reconnect_timeout)
            else:
                await self._connect_to_kafka_broker()
        else:
            raise Exception("Trying to connect in an already open connection.")

    async def _connect_to_kafka_broker(self):
        """Connects to kafka starting all the consumers Threads registered and the producer Thread."""
        if not self._connected:
            logger.debug(f"Connecting to Kafka broker: {self.bootstrap_servers}")
            try:

                async def connect():
                    await self._start_producer()
                    for input_element in self.inputs:
                        await input_element.start()
                    self._connected = True
                    logger.info(f"Connected to Kafka broker: {self.bootstrap_servers}")

                await asyncio.wait_for(connect(), CONNECTION_TIMEOUT_SECONDS)
            except Exception:
                logger.exception("Error connecting to Kafka broker")
                raise KafkaConnectionError
        else:
            logger.warning("Trying to connect to an already used connection")

    async def _reconnect(self, reconnect_timeout):
        """When the library can't connect to kafka it tries to reconnect.

        Args:
            reconnect_timeout: Maximum time that the library tries to reconnect.

        Raises:
            ConnectionError: The connection can't be stabilized.
        """
        connection_open = False
        begin_time = time.time()
        while (time.time() - begin_time) < reconnect_timeout:
            try:
                logger.debug(f"Connecting to the Kafka broker with host: {self.bootstrap_servers}")
                await self._connect_to_kafka_broker()
                logger.info("Successful connection to the Kafka broker")
                connection_open = True
                break

            except KafkaConnectionError:
                logger.critical(
                    f"Connection to the resource Kafka broker failed. Timeout: {reconnect_timeout}. "
                    f"Time elapsed: {time.time() - begin_time}"
                )
        if not connection_open:
            logger.critical("Maximum of connection attempts with the Kafka broker reached. Closing server")
            raise KafkaConnectionError

    async def _start_producer(self):
        """Starts producer Thread and waits until it have started."""
        logger.debug("Starting producer thread")
        if not self.producer_thread_started:
            self.producer_thread = ProducerThread(
                conf=self.confluent_conf, loop=self.loop, on_error_callback=self.on_error_callback
            )
            await self.producer_thread.start()
            await self.producer_thread.start_event.wait()
            if self.producer_thread.exception is not None:
                raise KafkaConnectionError
            self.producer_thread_started = True
            logger.debug("Started Kafka producer thread")
        else:
            raise Exception("Error in Kafka producer thread: already started")

    async def _stop_producer(self):
        """Stops producer Thread."""
        await self.producer_thread.stop()
        self.producer_thread_started = False
        logger.debug("Stopped Kafka producer thread")
