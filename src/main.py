from __future__ import annotations

import signal
import asyncio

from src.config import load_config
from src.database import Database

from src.manager import main_loop, MainManager

async def main(shutdown_event: asyncio.Event) -> None:
    settings = load_config()
    database = Database()
    main_manager = MainManager(settings=settings, database=database)
    main_promise = asyncio.create_task(coro=main_loop(manager=main_manager))

    shutdown_task = asyncio.create_task(shutdown_event.wait())
    (completed_tasks, _) = await asyncio.wait(
        fs=[shutdown_task], return_when=asyncio.FIRST_COMPLETED,
    )
    if shutdown_task in completed_tasks:
        main_promise.cancel()
        await main_manager.shutdown()
        await asyncio.gather(main_promise, return_exceptions=True)

    database.close()

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    shutdown = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)

    try:
        loop.run_until_complete(main(shutdown))
    finally:
        loop.close()
        print("🔌 Service has been stopped.")