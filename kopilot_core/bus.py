import json
import logging
import re
from typing import Optional, List, Callable, Tuple

import anyio
import nats
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import ConsumerConfig, StreamConfig, AckPolicy, RetentionPolicy, StorageType
from nats.js.errors import NotFoundError
from anyio.abc import TaskGroup
from anyio import Event

logger = logging.getLogger("nats")

class NATSServer:
    def __init__(self):
        self._connection: Optional[nats.NATS] = None
        self._cfg: dict = {}
        self._handlers: List[Tuple[str, Callable]] = []
        self._responders: List[Tuple[str, Callable]] = []
        # Durable (JetStream) lane — parallel to the core lane above.
        self._durables: List[Tuple[str, Callable, dict]] = []
        self._js = None
        self._ensured_streams: set = set()
        self._task_group: Optional[TaskGroup] = None
        self._shutdown_event: Optional[Event] = None

    def configure(self, **cfg):
        self._cfg.update(cfg)
    
    async def serve(self, task_group: TaskGroup, shutdown_event: Event):
        self._task_group = task_group
        self._shutdown_event = shutdown_event
        try:
            await self.connect()
            await self._shutdown_event.wait()
        finally:
            await self.close()

    async def connect(self):
        if self._connection and self._connection.is_connected:
            return
        try:
            self._connection = await nats.connect(
                **self._cfg,
                disconnected_cb=self._on_disconnected,
                reconnected_cb=self._on_reconnected,
                error_cb=self._on_error,
                closed_cb=self._on_closed,
            )
            logger.info("Connected to NATS server")
            await self._register_handlers()
            if self._durables:
                await self._setup_durables()
        except Exception as e:
            logger.error(f"Failed to connect to NATS: {e}")
            raise

    async def close(self):
        if self._connection and self._connection.is_connected:
            await self._connection.close()
            self._connection = None
            logger.info("NATS connection closed")

    async def _on_disconnected(self):
        logger.warning("NATS disconnected")

    async def _on_reconnected(self):
        logger.warning("NATS reconnected")

    async def _on_error(self, e):
        logger.error(f"NATS async error: {e}", exc_info=True)

    async def _on_closed(self):
        logger.error("NATS gave up: max_reconnect_attempts exhausted")

    async def _register_handlers(self):
        for subject, handler in self._handlers:
            async def wrapper(msg, h=handler, subj=subject):
                logger.info(f"RCV {subj} {len(msg.data)}b")

                async def handle_safely(msg, h, subj):
                    try:
                        data = json.loads(msg.data.decode()) if msg.data else {}
                        await h(data)
                    except Exception as e:
                        logger.error(f"Error in {subj}: {e}", exc_info=True)

                self._task_group.start_soon(handle_safely, msg, h, subj)

            await self._connection.subscribe(subject, cb=wrapper)
            logger.info(f"Registered subscription: {subject}")

        for subject, handler in self._responders:
            async def wrapper(msg, h=handler, subj=subject):
                logger.info(f"REQ {subj} {len(msg.data)}b")

                async def handle_and_respond(msg, h, subj):
                    try:
                        data = json.loads(msg.data.decode()) if msg.data else {}
                        result = await h(data)
                        await msg.respond(json.dumps(result).encode())
                    except Exception as e:
                        logger.error(f"Error handling {subj}: {e}", exc_info=True)
                        await msg.respond(json.dumps({"error": str(e)}).encode())

                self._task_group.start_soon(handle_and_respond, msg, h, subj)

            await self._connection.subscribe(subject, cb=wrapper)
            logger.info(f"Registered responder: {subject}")

    def sub(self, subject: str):
        def decorator(func: Callable):
            self._handlers.append((subject, func))
            return func
        return decorator

    def reply(self, subject: str):
        def decorator(func: Callable):
            self._responders.append((subject, func))
            return func
        return decorator

    # ── Durable lane (JetStream) ─────────────────────────────────────────
    # Opt-in, guaranteed-delivery counterpart to @nc.sub / nc.pub. A message
    # published with jpub() is persisted to a WorkQueue stream and held until
    # exactly one durable consumer acks it, then deleted. Survives restarts;
    # redelivers on crash/timeout; one-of-N across competing instances. The
    # core lane above is untouched — use this only where dropping a message is
    # unacceptable. At-least-once: keep handlers idempotent.

    @staticmethod
    def _stream_name(subject: str) -> str:
        # Stream/consumer names can't contain '.', '*', '>' — sanitize.
        return re.sub(r"[^a-zA-Z0-9_]", "_", subject)

    def durable(self, subject: str, *, ack_wait: int = 30, max_deliver: int = 5,
                batch: int = 1, fetch_timeout: int = 5):
        """Register a guaranteed-delivery handler backed by a WorkQueue stream.

        ack_wait: seconds before an un-acked message is redelivered.
        max_deliver: attempts before a poison message is TERM'd (dropped + logged).
        """
        def decorator(func: Callable):
            self._durables.append((subject, func, {
                "ack_wait": ack_wait, "max_deliver": max_deliver,
                "batch": batch, "fetch_timeout": fetch_timeout,
            }))
            return func
        return decorator

    async def _ensure_stream(self, subject: str):
        name = self._stream_name(subject)
        if name in self._ensured_streams:
            return
        try:
            await self._js.stream_info(name)
        except NotFoundError:
            await self._js.add_stream(StreamConfig(
                name=name,
                subjects=[subject],
                retention=RetentionPolicy.WORK_QUEUE,
                storage=StorageType.FILE,
            ))
            logger.info(f"Created WorkQueue stream {name} for {subject}")
        self._ensured_streams.add(name)

    async def _setup_durables(self):
        self._js = self._connection.jetstream()
        for subject, handler, opts in self._durables:
            await self._ensure_stream(subject)
            self._task_group.start_soon(self._run_durable, subject, handler, opts)
            logger.info(f"Registered durable: {subject}")

    async def _run_durable(self, subject: str, handler: Callable, opts: dict):
        name = self._stream_name(subject)
        psub = await self._js.pull_subscribe(
            subject,
            durable=name,
            config=ConsumerConfig(
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=opts["ack_wait"],
                max_deliver=opts["max_deliver"],
            ),
        )
        while not self._shutdown_event.is_set():
            try:
                msgs = await psub.fetch(opts["batch"], timeout=opts["fetch_timeout"])
            except NatsTimeoutError:
                continue  # no messages this window — normal
            except Exception as e:
                logger.error(f"Durable fetch error on {subject}: {e}", exc_info=True)
                await anyio.sleep(1)
                continue
            for msg in msgs:
                await self._handle_durable(msg, handler, subject, opts)

    async def _handle_durable(self, msg, handler: Callable, subject: str, opts: dict):
        delivered = msg.metadata.num_delivered
        logger.info(f"JRCV {subject} {len(msg.data)}b (attempt {delivered})")
        try:
            data = json.loads(msg.data.decode()) if msg.data else {}
            await handler(data)
            await msg.ack()
            logger.info(f"ACK {subject}")
        except Exception as e:
            if delivered >= opts["max_deliver"]:
                await msg.term()  # drop from the WorkQueue so it can't clog it
                logger.error(f"TERM {subject} after {delivered} attempts (dead): {e}", exc_info=True)
            else:
                await msg.nak()
                logger.error(f"NAK {subject} (attempt {delivered}): {e}", exc_info=True)

    def _ensure_connected(self):
        if self._connection is None:
            raise RuntimeError("NATS not connected — call `await nc.connect()` first.")

    async def jpub(self, subject: str, data: dict, msg_id: Optional[str] = None):
        """Publish to the durable lane. Pass msg_id for publisher-side dedup."""
        self._ensure_connected()
        if self._js is None:
            self._js = self._connection.jetstream()
        await self._ensure_stream(subject)
        message = json.dumps(data).encode()
        headers = {"Nats-Msg-Id": msg_id} if msg_id else None
        logger.info(f"JPUB {subject} {len(message)}b")
        await self._js.publish(subject, message, headers=headers)

    async def pub(self, subject: str, data: dict):
        self._ensure_connected()
        message = json.dumps(data).encode()
        logger.info(f"PUB {subject} {len(message)}b")
        await self._connection.publish(subject, message)

    async def request(self, subject: str, data: dict, timeout: int = 5):
        self._ensure_connected()
        message = json.dumps(data).encode()
        logger.info(f"REQ-OUT {subject} {len(message)}b")
        response = await self._connection.request(subject, message, timeout=timeout)
        return json.loads(response.data.decode()) if response.data else None


nc = NATSServer()