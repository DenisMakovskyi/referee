import pytest
import asyncio

from unittest.mock import AsyncMock, patch

from src.manager import MainManager, main_loop

@pytest.mark.asyncio
async def test_main_loop_runs_multiple_times():
    manager = AsyncMock(spec=MainManager)

    call_counter = 0

    async def mock_run():
        nonlocal call_counter
        call_counter += 1
        if call_counter >= 3:
            raise asyncio.CancelledError()

    manager.run.side_effect = mock_run

    with patch("asyncio.sleep", new=AsyncMock()) as fake_sleep:
        with pytest.raises(asyncio.CancelledError):
            await main_loop(manager=manager, interval_seconds=5)

    assert call_counter >= 3
    assert fake_sleep.call_count >= 2