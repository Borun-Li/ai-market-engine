from dataclasses import dataclass, field


@dataclass # auto-generates __init__, __repr__, and __eq__ from annotated attributes
class ScraperReport:
    """Accumulates and summarizes the results of a single scraper run.

    Populated incrementally as each feed is processed, then printed at the
    end of every run to make pipeline output auditable.

    Attributes:
        sources_scraped: Names of feeds that were successfully fetched.
        articles_found: Total articles returned across all feeds before keyword filtering.
        articles_filtered: Articles remaining after keyword filtering.
        errors: Human-readable error strings for any feed that failed.
        runtime_seconds: Wall-clock duration of the full scraper run.
    """

    sources_scraped: list[str] = field(default_factory=list)
    articles_found: int = 0
    articles_filtered: int = 0
    errors: list[str] = field(default_factory=list)
    runtime_seconds: float = 0.0

    def summary(self) -> str:
        """Return a single-line human-readable summary of the run.

        Returns:
            A pipe-delimited string reporting source count, article counts,
            error count, and runtime. Example:
            'Sources: 4 | Found: 312 | Filtered: 135 | Errors: 0 | Runtime: 8.42s'
        """
        return (
            f"Sources: {len(self.sources_scraped)} | "
            f"Found: {self.articles_found} | "
            f"Filtered: {self.articles_filtered} | "
            f"Errors: {len(self.errors)} | "
            f"Runtime: {self.runtime_seconds:.2f}s"
        )