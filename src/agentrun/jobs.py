"""Built-in CLI jobs. All model calls use the local mock provider."""

from .example import JOBS as EXAMPLE_JOBS
from .hiring import HN_HIRING


JOBS = {**EXAMPLE_JOBS, "hn_hiring": HN_HIRING}
