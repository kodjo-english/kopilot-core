import logging
import signal

import anyio
from anyio import Event, open_signal_receiver, create_task_group
from anyio.abc import CancelScope

from kopilot_core.bus import nc
from kopilot_core.scheduler import sch

logger = logging.getLogger(__name__)


class Service:
    def __init__(self):
        self.shutdown_event = Event()
        self._shutdown_initiated = False

    async def start(self):
        async with create_task_group() as tg:
            tg.start_soon(nc.serve, tg, self.shutdown_event)

            if not sch.running:
                sch.start()
            logger.info("Scheduler started")

            await self.shutdown_event.wait()
            tg.cancel_scope.cancel()

    async def stop(self):
        if self._shutdown_initiated:
            return
        self._shutdown_initiated = True
        logger.info("Shutdown initiated")

        self.shutdown_event.set()

        if sch.running:
            try:
                sch.shutdown(wait=True)
                logger.info("Scheduler stopped")
            except Exception as e:
                logger.error(f"Error stopping scheduler: {e}")

    async def _signals(self, scope: CancelScope):
        with open_signal_receiver(signal.SIGTERM, signal.SIGINT) as signals:
            async for signum in signals:
                logger.info(f"Received signal {signum}")
                await self.stop()
                scope.cancel()
                return


async def _main():
    service = Service()
    try:
        async with create_task_group() as tg:
            tg.start_soon(service._signals, tg.cancel_scope)
            tg.start_soon(service.start)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt")
    except anyio.get_cancelled_exc_class():
        logger.info("Service cancelled")
    finally:
        await service.stop()
        logger.info("Service shutdown complete")


def run():
    logger.info("Starting kopilot service")
    anyio.run(_main)