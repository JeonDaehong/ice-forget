"""IceForget: a right-to-be-forgotten compliance engine for Apache Iceberg.

IceForget orchestrates the industry-standard ``DELETE -> compact -> expire``
erasure pipeline against Iceberg tables, then proves the data is physically
gone by re-scanning every reachable snapshot and issuing a signed erasure
certificate.

The public surface most callers need:

    from iceforget import ErasureCoordinator, load_policy

    coordinator = ErasureCoordinator.from_policy(load_policy("policy.yaml"))
    result = coordinator.erase("db.users", {"user_id": 42})
"""

__version__ = "0.0.1"

from iceforget.coordinator import ErasureCoordinator
from iceforget.models import (
    ErasureCertificate,
    ErasureRequest,
    ErasureResult,
    FileMatch,
    IndexReport,
    VerifyReport,
)
from iceforget.policy import Policy, TablePolicy, load_policy

__all__ = [
    "ErasureCoordinator",
    "ErasureCertificate",
    "ErasureRequest",
    "ErasureResult",
    "FileMatch",
    "IndexReport",
    "VerifyReport",
    "Policy",
    "TablePolicy",
    "load_policy",
    "__version__",
]
