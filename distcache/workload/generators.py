"""
Workload Generators
-------------------
uniform_workload  - keys drawn with equal probability
skewed_workload   - one hot key absorbs most traffic (celebrity/trending item)
"""

import random


def uniform_workload(n: int, key_space: int = 1000,
                     seed: int | None = None) -> list[str]:
    """n keys drawn uniformly from [key:0 .. key:{key_space-1}]."""
    rng = random.Random(seed)
    return [f"key:{rng.randint(0, key_space - 1)}" for _ in range(n)]


def skewed_workload(n: int,
                    hot_key: str = "key:hot",
                    hot_fraction: float = 0.8,
                    key_space: int = 1000,
                    seed: int | None = None) -> list[str]:
    """
    n keys where *hot_fraction* of requests hit a single hot key.
    Models a celebrity item / trending hashtag cache hotspot.
    """
    rng = random.Random(seed)
    keys = []
    for _ in range(n):
        if rng.random() < hot_fraction:
            keys.append(hot_key)
        else:
            keys.append(f"key:{rng.randint(0, key_space - 1)}")
    return keys
