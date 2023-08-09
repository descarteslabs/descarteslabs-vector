import dataclasses
import enum
from typing import List, Optional

import descarteslabs.vector as dlv


class Role(enum.Enum):
    """Constants for the various types of roles"""

    OWNER = "owner"
    READER = "reader"
    WRITER = "writer"

    @property
    def attr(self) -> str:
        """Gets the corresponding attribute name"""
        return self.value + "s"

    @classmethod
    def values(cls) -> List[str]:
        """Gets the available values"""
        return [val.value for key, val in cls.__members__.items()]


def grantable_principals(
    *,
    orgs: Optional[List[str]] = None,
    users: Optional[List[str]] = None,
    groups: Optional[List[str]] = None,
    emails: Optional[List[str]] = None,
) -> List[str]:
    """Formats the given entities into shareable identifiers"""
    return (
        [f"org:{i}" for i in orgs or []]
        + [f"user:{i}" for i in users or []]
        + [f"group:{i}" for i in groups or []]
        + [f"email:{i}" for i in emails or []]
    )


@dataclasses.dataclass
class UnshareResult:
    """Result of the unshare operation"""

    unknown_principals: List[str] = dataclasses.field(default_factory=list)


def share_product(
    product: dlv.Table,
    *,
    role: Role,
    orgs: Optional[List[str]] = None,
    users: Optional[List[str]] = None,
    groups: Optional[List[str]] = None,
    emails: Optional[List[str]] = None,
) -> None:
    """Grants the given role to the given principals on the given product"""
    attr = role.attr

    # Existing list of entities in the given role
    existing = product.parameters.get(attr, [])

    principals = grantable_principals(
        orgs=orgs, users=users, groups=groups, emails=emails
    )

    for principal in principals:
        if principal not in existing:
            existing.append(principal)
    product.update(**{attr: existing})


def unshare_product(
    product: dlv.Table,
    *,
    role: Role,
    orgs: Optional[List[str]] = None,
    users: Optional[List[str]] = None,
    groups: Optional[List[str]] = None,
    emails: Optional[List[str]] = None,
) -> UnshareResult:
    """Revokes the given role from the given principals on the given product"""

    result = UnshareResult()

    attr = role.attr

    # Existing list of entities in the given role
    existing = product.parameters.get(attr, [])

    principals = grantable_principals(
        orgs=orgs, users=users, groups=groups, emails=emails
    )
    for principal in principals:
        if principal not in existing:
            result.unknown_principals.append(principal)
            continue

        existing.remove(principal)

    product.update(**{attr: existing})

    return result
