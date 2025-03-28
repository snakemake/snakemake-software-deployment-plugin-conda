from pathlib import Path
from typing import Generator
from urllib.parse import urlparse


def get_match_specs_from_conda_pinfile(path: Path) -> Generator[str, None, None]:
    """Open given conda pinfile and yield its entries as rattler match spec strings."""
    with open(path, "r") as f:
        header = True
        for record in f:
            if header:
                if record.strip() == "@EXPLICIT":
                    header = False
            else:
                parsed = urlparse(record.strip())
                package_components = Path(parsed.path).name.rsplit("-", 3)
                name = "-".join(package_components[:-2])
                md5 = parsed.fragment
                url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                yield f"{name}[url='{url}', md5='{md5}']"
