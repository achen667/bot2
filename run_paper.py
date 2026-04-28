from __future__ import annotations

import asyncio

from .config import BotConfig
from .main import run_bot


if __name__ == "__main__":
    asyncio.run(run_bot(BotConfig(paper_trading=True)))
