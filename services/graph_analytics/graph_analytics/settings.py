from aml_common.config import Settings


class AnalyticsSettings(Settings):
    metrics_port: int = 8003
    net_engine: str = "auto"             # gds | networkx | auto (try GDS, fall back to networkx)
    net_interval_s: float = 60.0
    net_window_s: float = 1800.0         # recent-flow horizon (graph/event time)
    net_min_amount: float = 5000.0       # only material flows enter the analytics graph
    net_ttl_s: int = 240                 # Redis TTL of per-account network flags
    edge_retention_s: float = 3600.0     # TRANSFERRED_TO edges older than this are pruned
    prune_batch: int = 5000
    cluster_min_size: int = 3            # materialise :Cluster nodes for clusters at least this big
    # network flag thresholds
    net_tight_size: int = 4
    net_tight_ratio: float = 0.8
    net_scc_min: int = 3
    net_hub_factor: float = 2.0
    net_hub_min_indegree: int = 3
    net_max_cluster: int = 40            # components larger than this are split with Louvain
