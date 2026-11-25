"""Deterministic summary and tag extraction.

Deterministic on purpose: the content cache is only valid if the same content always
produces the same result.
"""

import re
from collections import Counter

MAX_SUMMARY_WORDS = 40
TAG_LIMIT = 5
_IGNORED_WORDS = frozenset(
    """about above after again against also because been before being below between both could
    does doing down during each from further have having here into itself just more most other
    over same should some such than that their theirs them then there these they this those
    through under until very were what when where which while will with would your yours""".split()
)


def build_summary(text: str) -> str:
    tokens = text.split()
    result = " ".join(tokens[:MAX_SUMMARY_WORDS])
    return result + (" ..." if len(tokens) > MAX_SUMMARY_WORDS else "")


def top_keywords(summary: str) -> list[str]:
    candidates = [w for w in re.findall(r"[a-z][a-z0-9-]{3,}", summary.lower()) if w not in _IGNORED_WORDS]
    freq = Counter(candidates)
    # most frequent first; ties broken by first appearance (Counter preserves insertion order)
    return [word for word, _ in sorted(freq.items(), key=lambda kv: -kv[1])[:TAG_LIMIT]]
