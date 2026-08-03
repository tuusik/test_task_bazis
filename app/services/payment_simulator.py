import asyncio
import random

from app.core.config import Settings
from app.domain.enums import PaymentStatus


class PaymentSimulator:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def simulate(self) -> PaymentStatus:
        processing_delay = random.uniform(*self.settings.consumer_processing_time)

        await asyncio.sleep(processing_delay)

        return random.choices(
            population=[
                PaymentStatus.SUCCEEDED,
                PaymentStatus.FAILED,
            ],
            weights=self.settings.consumer_process_chance,
            k=1,
        )[0]
