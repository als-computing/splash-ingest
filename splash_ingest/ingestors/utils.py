from dataclasses import dataclass
from enum import Enum
from typing import Optional, Union


class Severity(str, Enum):
    warning = "warning"
    error = "error"


@dataclass
class Issue:
    severity: Severity
    msg: str
    exception: Optional[Union[str, None]] = None
