# tamarco-kafka

[![Build Status](https://travis-ci.com/System73/tamarco-kafka.svg?branch=master)](https://travis-ci.com/System73/tamarco-kafka)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=System73_tamarco-kafka&metric=coverage)](https://sonarcloud.io/dashboard?id=System73_tamarco-kafka)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=System73_tamarco-kafka&metric=alert_status)](https://sonarcloud.io/dashboard?id=System73_tamarco-kafka)

Kafka resource for Tamarco microservice framework. It runs a confluent-kafka client in a thread.

This repository is a plugin for Tamarco, for more information go to [Tamarco main repository](https://github.com/System73/tamarco).

## Settings

This resource depends on the following configuration:

```yaml
    system:
        resources:
            kafka:
                bootstrap_servers: kafka:9092
```

The bootstrap servers are the address of the members of a kafka cluster separated by coma.

## Input and outputs

The inputs and outputs need to be declared in the resource.

### Input

The input can be used with two different patterns, as decorator and as async stream.

This resource only supports balanced consumer groups with auto commit.

#### Async stream

This usage case uses the input as asynchronous iterator to consume the metric stream.

```python3
class MyMicroservice(Microservice):
    name = "input_example"

    metrics_input = KafkaInput(topic='metrics', codec=JsonCodec)
    kafka = KafkaResource(inputs=[metrics_input])

    @task
    async def metrics_consumer(self):
        async for metric in self.metrics_input:
            self.logger.info(f'Consumed message from metrics topic: {metric}')
```

### Decorator

This usage case declares a function as handler of the messages, and the resource is going to open automatically a
coroutine to consume each message. 

```python3
class MyMicroservice(Microservice):
    name = "input_example"

    kafka = KafkaResource(inputs=[metrics_input])

    @KafkaInput(resource=kafka, topic='metrics', codec=JsonCodec)
    async def metrics_handler(self, message):
        self.logger.info(f'Consumed message from metrics topic: {message}')
```

## Output

It is a Kafka producer very simple to use.

```python3
class MyMicroservice(Microservice):
    name = "output_example"
    metrics_output = KafkaOutput(topic='metrics', codec=JsonCodec)
    kafka = KafkaResource(outputs=[metrics_output])

    @task_timer(interval=1000, autostart=True)
    async def metrics_producer(self):
        metrics_message = {'metrics': {'cat': 'MEOW'}}
        await self.metrics_output.push(metrics_message)
        self.logger.info(f'Produced message {metrics_message} to metrics topic')
```

## How to run the examples

To run them you just need to launch the docker-compose, install the requirements and run it.

```python3
pip install -r examples/requirements.txt
docker-compose up -d
python examples/microservice_stream_input.py
```
