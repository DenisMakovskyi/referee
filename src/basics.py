from typing import List, Dict, Callable

StreamCallbacks = Dict[str, Callable[[float, int], None]]

def chunked(seq: List[str], size: int) -> List[List[str]]:
    return [seq[i:i + size] for i in range(0, len(seq), size)]
