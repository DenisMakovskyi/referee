from typing import List, Dict

from dataclasses import dataclass

@dataclass
class Bubble:
    id: str
    symbol: str
    market_cap: float
    listed_exc: List[str]
    performance: Dict[str, float]