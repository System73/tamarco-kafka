class KafkaConnectionError(Exception):
    """Exception that raises when the library can't establish the connection with the broker."""

    pass


class DeliveryError(Exception):
    """Exception that raises when the library can't deliver the message to the broker."""

    pass
