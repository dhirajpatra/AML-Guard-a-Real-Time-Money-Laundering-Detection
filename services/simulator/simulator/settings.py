from aml_common.config import Settings


class SimSettings(Settings):
    sim_tps: float = 10.0               # normal transactions per second
    sim_customers: int = 2000
    sim_scenarios_per_min: float = 6.0  # laundering scenarios started per minute
    sim_seed: int = 42
    sim_max_txns: int = 0               # 0 = run forever
    sim_seed_graph: bool = True
    high_risk_countries: str = "IR,KP,MM"

    @property
    def high_risk_list(self) -> list[str]:
        return [c.strip() for c in self.high_risk_countries.split(",") if c.strip()]
