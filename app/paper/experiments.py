from __future__ import annotations

from dataclasses import dataclass

from app.config import AppConfig, PaperTradingExperimentConfig, PaperTradingConfig, RiskConfig


@dataclass(frozen=True)
class ExperimentRuntime:
    name: str
    strategy_name: str
    paper: PaperTradingConfig
    risk: RiskConfig

    def to_record(self) -> dict:
        return {
            "name": self.name,
            "strategy_name": self.strategy_name,
            "status": "active",
            "config_json": {
                "paper_trading": self.paper.model_dump(exclude={"experiments"}),
                "risk": self.risk.model_dump(),
            },
        }


def resolve_experiments(config: AppConfig) -> list[ExperimentRuntime]:
    base_paper = config.paper_trading.model_copy(update={"experiments": []})
    configured = [experiment for experiment in config.paper_trading.experiments if experiment.enabled]
    if not configured:
        configured = [PaperTradingExperimentConfig(name="micro_burst_primary", strategy_name=base_paper.strategy_name)]
    runtimes = []
    for experiment in configured:
        paper = _resolve_paper_config(base_paper, experiment)
        risk = _resolve_risk_config(config.risk, experiment)
        runtimes.append(
            ExperimentRuntime(
                name=experiment.name,
                strategy_name=paper.strategy_name,
                paper=paper,
                risk=risk,
            )
        )
    return runtimes


def _resolve_paper_config(base: PaperTradingConfig, experiment: PaperTradingExperimentConfig) -> PaperTradingConfig:
    overrides = experiment.model_dump(exclude_none=True, exclude={"name", "enabled", "risk"})
    overrides.pop("experiments", None)
    return base.model_copy(update=overrides)


def _resolve_risk_config(base: RiskConfig, experiment: PaperTradingExperimentConfig) -> RiskConfig:
    overrides = experiment.risk.model_dump(exclude_none=True)
    return base.model_copy(update=overrides)
