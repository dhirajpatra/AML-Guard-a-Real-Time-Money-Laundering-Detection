from aml_common.config import Settings


class ProcSettings(Settings):
    proc_group: str = "stream-processor"
    proc_batch: int = 50                 # max decisions per Postgres flush
    proc_flush_interval_s: float = 0.25
    proc_offset_reset: str = "earliest"
    review_threshold: float = 0.50
    block_threshold: float = 0.80
    flow_window_s: float = 300.0         # how far back flow-tracing looks
    flow_min_amount: float = 5000.0      # graph flow queries only run for material amounts
    profile_ttl_s: float = 300.0
    stats_interval_s: float = 10.0
