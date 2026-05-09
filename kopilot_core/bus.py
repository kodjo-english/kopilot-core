import json
import logging
from typing import Optional, List, Callable, Tuple

import nats
from anyio.abc import TaskGroup
from anyio import Event

logger = logging.getLogger("nats")

class NATSServer:
    def __init__(self):
        self._connection: Optional[nats.NATS] = None
        self._cfg: dict = {}
        self._handlers: List[Tuple[str, Callable]] = []
        self._responders: List[Tuple[str, Callable]] = []
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

    async def pub(self, subject: str, data: dict):
        message = json.dumps(data).encode()
        logger.info(f"PUB {subject} {len(message)}b")
        await self._connection.publish(subject, message)

    async def request(self, subject: str, data: dict, timeout: int = 5):
        message = json.dumps(data).encode()
        logger.info(f"REQ-OUT {subject} {len(message)}b")
        response = await self._connection.request(subject, message, timeout=timeout)
        return json.loads(response.data.decode()) if response.data else None


nc = NATSServer()