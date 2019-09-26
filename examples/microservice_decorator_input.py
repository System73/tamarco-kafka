import asyncio

from tamarco.codecs.json import JsonCodec
from tamarco.core.microservice import Microservice, task
from tamarco.resources.io.kafka.input import KafkaInput
from tamarco.resources.io.kafka.output import KafkaOutput
from tamarco.resources.io.kafka.resource import KafkaResource


class KafkaMicroservice(Microservice):

    name = "kafka_example"

    def __init__(self):
        super().__init__()
        self.settings.update_internal(
            {
                "system": {
                    "deploy_name": "test",
                    "logging": {"profile": "PRODUCTION"},
                    "resources": {"kafka": {"bootstrap_servers": "127.0.0.1"}},
                }
            }
        )

    kafka = KafkaResource(outputs=[KafkaOutput(topic="metrics", codec=JsonCodec)])

    @task
    async def metrics_producer(self):
        metrics_output = self.kafka.outputs["metrics"]
        while True:
            await asyncio.sleep(1)
            metrics_message = {"metrics": {"cow": "MOOOO"}}
            await metrics_output.push(metrics_message)
            self.logger.info(f"Produce message {metrics_message} to metrics topic")

    @KafkaInput(resource=kafka, topic="metrics", codec=JsonCodec)
    async def pull(self, message):
        self.logger.info(f"Consumed message from metrics topic: {message}")


def main():
    ms = KafkaMicroservice()
    ms.run()


if __name__ == "__main__":
    main()
