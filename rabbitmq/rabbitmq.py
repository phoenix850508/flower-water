import aio_pika
from collections.abc import Awaitable, Callable
import json
from typing import Any, Iterable
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)

MessageHandler = Callable[[Any, str], Awaitable[None]]


class RabbitMQ:

    def __init__(
        self,
        url: str,
        exchange_type: aio_pika.ExchangeType = aio_pika.ExchangeType.TOPIC,
        prefetch: int = 10,
    ) -> None:
        self.url = url
        self._conn: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None
        self.prefetch = prefetch
        self.in_queue: AbstractQueue | None = None
        self.out_queue: AbstractQueue | None = None
        self._exchanges: dict[str, AbstractExchange] = {}
        self.exchange_type = exchange_type
        self._consumers: list[tuple[AbstractQueue, str]] = []

    # ---- Lifecycle ----
    async def connect(self) -> None:
        conn = await aio_pika.connect_robust(self.url)
        channel = await conn.channel()
        await self.channel.set_qos(prefetch_count=self.prefetch)
        self._conn, self._channel = conn, channel

    async def close(self) -> None:
        for queue, tag in self._consumers:
            await queue.cancel(tag)
        self._consumers.clear()
        self._exchanges.clear()
        if self._conn:
            await self._conn.close()

    async def __aenter__(self) -> "RabbitMQ":
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    @property
    def channel(self) -> AbstractChannel:
        if self._channel is None:
            raise RuntimeError("RabbitMQ 尚未 connect()")
        return self._channel

    async def get_exchange(self, name: str) -> AbstractExchange:
        if name not in self._exchanges:
            self._exchanges[name] = await self.channel.declare_exchange(
                name, self.exchange_type, durable=True
            )
        return self._exchanges[name]

    # ---- publish ----
    async def publish(self, exchange: str, routing_key: str, payload: Any) -> None:
        ex = await self.get_exchange(exchange)
        msg = aio_pika.Message(
            body=json.dumps(payload).encode(), content_type="application/json"
        )
        await ex.publish(msg, routing_key=routing_key)

    # ---- subscribe ----
    async def subscribe(
        self,
        bindings: Iterable[tuple[str, str]],
        handler: MessageHandler,
        queue_name: str | None = None,
    ) -> str:
        """
        bindings: [(exchange, routing_key), ...]，一個 queue 可以綁多個 exchange
        queue_name=None  → 臨時 queue（exclusive，斷線就刪除）
        queue_name="xxx" → 持久 queue（重啟後會收到離線期間的訊息；同名的多個實例會分攤訊息）
        """

        queue = await self.channel.declare_queue(
            queue_name or "",
            durable=queue_name is not None,
            exclusive=queue_name is not None,
        )

        for exchange, key in bindings:
            await queue.bind(exchange, routing_key=key)

        async def on_message(msg: aio_pika.abc.AbstractIncomingMessage) -> None:
            async with msg.process(requeue=False):
                await handler(
                    json.loads(msg.body),
                    msg.routing_key or "",
                )

        tag = await queue.consume(on_message)
        self._consumers.append((queue, tag))
        return tag

    async def unsubscribe(self, tag: str) -> None:
        for item in self._consumers:
            queue, t = item
        if t == tag:
            await queue.cancel(t)
            self._consumers.remove(item)
            return
