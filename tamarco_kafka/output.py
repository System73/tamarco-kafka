import logging

from tamarco.resources.bases import OutputBase

from tamarco_kafka.connection import KafkaConnection

logger = logging.getLogger("tamarco.kafka")


class ClientPatternsMixin:
    """Implements all the clients patterns.
    The **Client** and the **Handlers** inherit from it so they can use the client patterns.
    """

    def __init__(self, *args, **kwargs):
        """Initializes the client connection and default topic.

        Args:
            connection: Connection to be used by the client.
            topic: Default topic of the client.
        """
        self.connection = None
        super().__init__(*args, **kwargs)

    def set_connection(self, connection: KafkaConnection):
        self.connection = connection

    async def push(self, message, topic, codec=None):
        """Implementation of the push pattern.

        Args:
            message: Message to push
            topic: Topic name where the message should be pushed.
            codec: Codec of the message.
        """
        assert self.connection, "Kafka client needs to be registered in a connection."
        encoded_message = self.encode_message(message, codec=codec)
        await self.connection._publish(topic=topic, msg=encoded_message)

    def encode_message(self, message, codec=None):
        if codec:
            codec_to_use = codec
        elif self.codec:
            codec_to_use = self.codec
        else:
            return message
        try:
            return codec_to_use.encode(message)
        except Exception:
            logger.warning(f"Unexpected exception encoding the message {message} in Kafka output", exc_info=True)
            raise

    def __repr__(self):
        topic = getattr(self, "topic", None)
        return f"KafkaOutput. Topic: {topic}."


class KafkaOutput(ClientPatternsMixin, OutputBase):
    def __init__(self, topic=None, *args, **kwargs):
        self.topic = topic
        if topic and ("name" not in kwargs):
            kwargs["name"] = topic
        super().__init__(*args, **kwargs)

    async def push(self, message):
        await super().push(message, topic=self.topic)


class KafkaGenericOutput(ClientPatternsMixin, OutputBase):
    def __init__(self, *args, **kwargs):
        kwargs["name"] = "generic"
        super().__init__(*args, **kwargs)
