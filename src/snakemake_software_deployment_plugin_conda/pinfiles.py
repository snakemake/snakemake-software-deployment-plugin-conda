from pathlib import Path
from typing import Generator
from rattler.match_spec import MatchSpec


def get_match_specs_from_conda_pinfile(path: Path) -> Generator[MatchSpec, None, None]:
    """Open given conda pinfile and yield its entries as rattler match spec strings."""
    with open(path, "r") as f:
        header = True
        for record in f:
            if header:
                if record.strip() == "@EXPLICIT":
                    header = False
            else:
                yield MatchSpec.from_url(record.strip())
