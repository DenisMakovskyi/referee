from typing import TypeVar, List, Dict, Iterator, Iterable, Callable
from itertools import islice

T = TypeVar('T')

StreamCallbacks = Dict[str, Callable[[float, int], None]]

def chunked(iterable: Iterable[T], size: int) -> Iterator[List[T]]:
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk

def matches_number(f: List[T], s: List[T]) -> int:
    return len(set(f) & set(s))

def are_contents_the_same(f: List[T], s: List[T]) -> bool:
    return set(f) == set(s)