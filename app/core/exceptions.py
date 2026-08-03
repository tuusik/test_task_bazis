class IdempotencyConflictError(Exception):
    pass


class PaymentProcessingError(RuntimeError):
    pass


class PermanentPaymentProcessingError(PaymentProcessingError):
    pass


class TransientPaymentProcessingError(PaymentProcessingError):
    pass


class PaymentNotFoundError(PermanentPaymentProcessingError):
    pass


class InvalidPaymentStateError(PermanentPaymentProcessingError):
    pass


class PermanentWebhookDeliveryError(PermanentPaymentProcessingError):
    pass


class WebhookDeliveryError(TransientPaymentProcessingError):
    pass


class WebhookDeliveryInProgressError(TransientPaymentProcessingError):
    def __init__(self, retry_after_seconds: float) -> None:
        self.retry_after_seconds = retry_after_seconds
        super().__init__("Webhook delivery is already in progress")
