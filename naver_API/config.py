"""Explicit monthly operating scope; no production theme defaults."""
from dataclasses import dataclass
from pathlib import Path
import math
import yaml


@dataclass(frozen=True)
class SeasonConfig:
    themes: tuple
    ad_daily_calls: int
    trend_daily_calls: int
    trend_monthly_calls: int
    ad_interval_seconds: float
    trend_interval_seconds: float

    def __post_init__(self):
        if len(self.themes) != 3 or len({t[0] for t in self.themes}) != 3:
            raise ValueError('Exactly three distinct event codes required')
        for code, name, month in self.themes:
            if type(code) is not int or code < 1 or not isinstance(name, str) or not name.strip() or type(month) is not int or not 1 <= month <= 12:
                raise ValueError('Invalid theme code/name/month')
        for value in (self.ad_daily_calls, self.trend_daily_calls, self.trend_monthly_calls):
            if type(value) is not int or value < 1:
                raise ValueError('Positive explicit call budgets required')
        for value in (self.ad_interval_seconds, self.trend_interval_seconds):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError('Positive finite API intervals required')

    def scope(self):
        return {'themes': [list(t) for t in self.themes], 'top': 10, 'calculation': 'mean28_v1'}


def load_config(path, day):
    data = yaml.safe_load(Path(path).read_text(encoding='utf8'))
    themes = data['months'][day.strftime('%Y-%m')]
    return SeasonConfig(tuple((t['event'], t['name'], t['month']) for t in themes), **data['limits'])
