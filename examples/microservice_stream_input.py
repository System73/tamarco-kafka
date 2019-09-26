from tamarco.codecs.json import JsonCodec
from tamarco.core.microservice import Microservice, task, task_timer
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

    metrics_input = KafkaInput(topic="metrics", codec=JsonCodec)
    metrics_output = KafkaOutput(topic="metrics", codec=JsonCodec)

    kafka = KafkaResource(inputs=[metrics_input], outputs=[metrics_output])

    @task_timer(interval=1000, autostart=True)
    async def metrics_producer(self):
        metrics_message = {"metrics": {"cat": "MEOW"}}
        await self.metrics_output.push(metrics_message)
        self.logger.info(f"Produce message {metrics_message} to metrics topic")

    @task
    async def metrics_consumer(self):
        async for metric in self.metrics_input:
            self.logger.info(f"Consumed message from metrics topic: {metric}")


def main():
    ms = KafkaMicroservice()
    ms.run()


if __name__ == "__main__":
    main()
