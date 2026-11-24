"""Deterministic stand-ins for the two model calls.

Deterministic on purpose: the content cache is only valid if the same content always
produces the same result.
"""

import re
from collections import Counter

SUMMARY_WORDS = 40
MAX_TAGS = 5
_STOPWORDS = frozenset(
    """about above after again against also because been before being below between both could
    does doing down during each from further have having here into itself just more most other
    over same should some such than that their theirs them then there these they this those
    through under until very were what when where which while will with would your yours""".split()
)


def summarize(content: str) -> str:
    words = content.split()
    summary = " ".join(words[:SUMMARY_WORDS])
    return summary + (" ..." if len(words) > SUMMARY_WORDS else "")


def extract_tags(summary: str) -> list[str]:
    words = [w for w in re.findall(r"[a-z][a-z0-9-]{3,}", summary.lower()) if w not in _STOPWORDS]
    counts = Counter(words)
    # most frequent first; ties broken by first appearance (Counter preserves insertion order)
    return [word for word, _ in sorted(counts.items(), key=lambda kv: -kv[1])[:MAX_TAGS]]
