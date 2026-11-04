import aio_pika

class RabbitMQ:
    def __init__(self, url: str) -> None:
        self.url = url
        self._conn: aio_pika.abc.AbstractRobustConnection | None = None
        self.channel: aio_pika.abc.AbstractChannel | None = None
        self.in_queue: aio_pika.abc.AbstractQueue | None = None
        self.out_queue: aio_pika.abc.AbstractQueue | None = None
        self.exchange: aio_pika.abc.AbstractExchange | None = None

    async def connect(self) -> None:
        self.conn = await aio_pika.connect_robust(url)
        self.channel = await self.conn.channel()
        await self.channel.declare_queue(self.in_queue, durable=False)
        await self.channel.declare_queue(self.out_queue, durable=False)

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()

    # ---- publish ----
    async def publish(self, routing_key: str, payload: Any) -> None:
        msg = aio_pika.Message(body=json.dump(payload).encode(), content_type='application/json')
        await self.exahange.publish(msg, routing_key=routing_key)

    # ---- subscribe ----
    async def subscribe(self, binding_key: str, handler: MessageHandler, queue_name: str | None = None) -> str:
        assert self._channel is not None
        queue = await self.channel.declare_queue(queue_name, durable=queue_name is not None)

        await queue.bind(self.exahange, routing_key=binding_key)

        async def on_message(msg: AbstractIncomingMessage) -> None:
            async with msg.process(requeue=False):
                await handler(json.loads(msg.body), msg.routing_key or "")

        tag = await queue.consume(on_message)
        self.consumer.append(queue, tag)
        return tag

    async def unsubscribe(self, tag: str) -> None:
        for queue, t in self._consumers:
            if t == tag:
                await queue.cancel(t)
    self._consumers = [(q, t) for q, t in self._consumers if t != tag]