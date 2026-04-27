"""Source scrapers — cada source implementa search(query, **kwargs) → list[Article]."""

from . import pubmed, scielo, scholar

REGISTRY = {
    "pubmed": pubmed,
    "scielo": scielo,
    "scholar": scholar,
}
