from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def load_private_key_from_keystore(keystore_path: str, password: Optional[str] = None) -> str:
    import getpass
    from eth_account import Account

    with open(keystore_path, "r", encoding="utf-8") as file:
        keystore = json.load(file)
    if password is None:
        password = getpass.getpass("Enter keystore password: ")
    priv_bytes = Account.decrypt(keystore, password)
    return "0x" + priv_bytes.hex()


class ClobClientWrapper:
    def __init__(self, keystore_path: str, gnosis_safe_address: str, clob_host: str, chain_id: int, password: Optional[str] = None, response_log_path: Optional[Path] = None):
        from py_clob_client.client import ClobClient

        private_key = load_private_key_from_keystore(keystore_path, password)
        tmp = ClobClient(host=clob_host, key=private_key, chain_id=chain_id, signature_type=2, funder=gnosis_safe_address)
        self.client = ClobClient(host=clob_host, key=private_key, creds=tmp.create_or_derive_api_creds(), chain_id=chain_id, signature_type=2, funder=gnosis_safe_address)

    async def submit_order(self, token_id: str, price: float, size: float, side: str) -> dict:
        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY, SELL

        clob_side = BUY if side.upper() == "BUY" else SELL
        args = MarketOrderArgs(token_id=token_id, amount=size, side=clob_side)
        loop = asyncio.get_event_loop()
        signed = await loop.run_in_executor(None, self.client.create_market_order, args)
        return await loop.run_in_executor(None, self.client.post_order, signed, OrderType.FOK)
