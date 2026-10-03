from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GraphFeatures:
    cycle_len: int | None = None   # length (edges) of a flow-conserving cycle closed by this txn
    up_chain_len: int = 0          # longest flow-conserving upstream chain ending at the sender (edges)


@dataclass(frozen=True)
class AccountProfile:
    kind: str = "unknown"          # person | organization | unknown
    addr_peers: int = 0            # other customers sharing the owner's registered address
    dev_peers: int = 0             # other customers sharing the owner's device
