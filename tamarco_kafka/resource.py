import asyncio
import logging
from itertools import chain

from tamarco.core.tasks import TasksManager
from tamarco.resources.bases import IOResource
from tamarco.resources.bases import StatusCodes

from tamarco_kafka.connection import KafkaConnection
from tamarco_kafka.settings import CONSUMER_PARALLEL_TASK_LIMIT


class KafkaResource(IOResource):
    depends_on = []
    loggers_names = ["tamarco.kafka", "confluent-kafka"]

    def __init__(self, *args, **kwargs):
        self.offset_reset = kwargs.pop("offset_reset", False)
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger("tamarco.kafka")
        self.kafka_connection = None
        self.kafka_client = None
        self.task_manager = TasksManager(task_limit=CONSUMER_PARALLEL_TASK_LIMIT)
        self.loop = asyncio.get_event_loop()

    async def bind(self, *args, **kwargs):
        await super().bind(*args, **kwargs)
        self.loop = self.microservice.loop
        self.task_manager.set_loop(self.loop)

    async def get_confluent_kafka_settings(self):
        bootstrap_servers = await self.settings.get("bootstrap_servers")
        confluent_kafka_settings = {"bootstrap_servers": bootstrap_servers}
        if self.offset_reset:
            confluent_kafka_settings.update({"auto.offset.reset": self.offset_reset})
        return confluent_kafka_settings

    async def connect_to_kafka(self, confluent_kafka_settings):
        try:
            self._status = StatusCodes.CONNECTING
            self.kafka_connection = KafkaConnection(
                on_error_callback=self.on_disconnect_callback, loop=self.loop, **confluent_kafka_settings
            )
        except Exception:
            self.logger.exception("Invalid Kafka resource settings")
            raise
        else:
            await self.kafka_connection.connect()

    def on_disconnect_callback(self, exception):
        # Workaround for avoid false positives disconnections is active, Disconnect don't trigger a fail status.
        # When the broker is really down usually confluent-kafka trigger two callbacks, 'Disconnected' and other
        # reporting 'ALL_BROKER_DOWN' so the second one should shut down the microservice in the real world.
        if "Receive failed: Disconnected" not in str(exception):
            self.logger.error(f"Error in kafka resource. Reporting failed status. Exception: {exception}")
            self._status = StatusCodes.FAILED
        else:
            self.logger.warning(
                "Workaround to avoid false disconnections is active. The exception 'Receive failed: "
                "Disconnected' do not trigger a failed status code in the kafka resource"
            )

    @property
    def io_streams(self):
        return tuple(chain(self.outputs.values(), self.inputs.values()))

    async def start(self):
        confluent_kafka_settings = await self.get_confluent_kafka_settings()
        try:
            await self.connect_to_kafka(confluent_kafka_settings)
        except ConnectionError:
            self.logger.critical("Kafka resource cannot connect to Kafka broker. Reporting failed status")
            self._status = StatusCodes.FAILED
        else:
            try:
                await self.kafka_connection.register(*self.io_streams)
            except Exception:
                self.logger.critical(
                    "Unexpected exception registering handlers in Kafka resource. Reporting failed " "status",
                    exc_info=True,
                )
                self._status = StatusCodes.FAILED
            else:
                for input_element in self.inputs.values():
                    self.register_input_callback_trigger_task(input_element)
                if self._status != StatusCodes.FAILED:
                    self._status = StatusCodes.STARTED

    async def post_start(self):
        self.task_manager.start_all()
        await super().post_start()

    def register_input_callback_trigger_task(self, new_input):
        input_name = new_input.name
        task_name = f"callback_trigger_{input_name}"
        if new_input.on_message_callback:
            self.task_manager.register_task(task_name, new_input.callback_trigger(self.task_manager))

    async def stop(self):
        self.logger.info(f"Stopping Kafka resource: {self.name}")
        if self._status != StatusCodes.NOT_STARTED or self._status != StatusCodes.STOPPED:
            self._status = StatusCodes.STOPPING
            try:
                await self.kafka_connection.close()
            except Exception:
                self.logger.warning("Unexpected exception stopping the Kafka connection", exc_info=True)
        self.task_manager.stop_all()
        await super().stop()

    async def status(self):
        status = {"status": self._status}
        if self._status == StatusCodes.STARTED:
            kafka_config = self.kafka_connection.confluent_conf
            inputs = [str(kafka_input) for kafka_input in self.inputs]
            outputs = [str(kafka_output) for kafka_output in self.outputs]
            status.update({"kafka_config": kafka_config, "inputs": inputs, "outputs": outputs})
        return status
