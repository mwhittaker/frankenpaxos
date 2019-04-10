from typing import Callable, Dict, NamedTuple, List, Tuple
import matplotlib.pyplot as plt
import pandas as pd
import pylab

def plot_prometheus_data(ax: plt.Axes,
                         df: pd.DataFrame,
                         metrics: List[Tuple[str, str]],
                         instances: List[Tuple[str, str]],
                         f: Callable[[pd.Series], pd.Series]) -> None:
    # Get the total number of lines we'll have to draw.
    n = len([
        None
        for (metric, _) in metrics
        for (instance, _) in instances
        if f'{metric}_{instance}' in df
    ])

    if n <= 20:
        # If there are few enough lines, we can give each its own color.
        if n <= 10:
            color_iter = iter([pylab.get_cmap('tab10')(i) for i in range(10)])
        else:
            color_iter = iter([pylab.get_cmap('tab20')(i) for i in range(20)])

        for (metric, metric_abbr) in metrics:
            for (instance, instance_abbr) in instances:
                name = f'{metric}_{instance}'
                if name in df:
                    s = f(df[name].dropna())
                    line = ax.plot_date(
                        s.index,
                        s,
                        label=f'{instance_abbr} {metric_abbr}',
                        fmt='.-',
                        color=next(color_iter),
                        alpha=0.75,
                    )[0]
    else:
        # Otherwise, we have to factor by marker and color.
        markers = [
            '.', 'o', 'v', '^', '<', '>', '1', '2', '3', '4', '8', 's', 'p',
            'P', '*', 'h', 'H', '+', 'x', 'X', 'D', 'd', '|', '_', 0, 1, 2, 3,
            4, 5, 6, 7, 8, 9, 10, 11,
        ]
        assert len(metrics) <= len(markers)

        if len(instances) <= 10:
            colors = [pylab.get_cmap('tab10')(i) for i in range(10)]
        elif len(instances) <= 20:
            colors = [pylab.get_cmap('tab20')(i) for i in range(20)]
        else:
            colors = [pylab.get_cmap('gist_rainbow')(float(i) / len(instances))
                      for i in range(len(instances))]

        for ((metric, metric_abbr), marker) in zip(metrics, markers):
            for ((instance, instance_abbr), color) in zip(instances, colors):
                name = f'{metric}_{instance}'
                if name in df:
                    s = f(df[name].dropna())
                    line = ax.plot_date(
                        s.index,
                        s,
                        label=f'{instance_abbr} {metric_abbr}',
                        marker=marker,
                        color=color,
                        linestyle='-',
                        alpha=0.75,
                    )[0]


