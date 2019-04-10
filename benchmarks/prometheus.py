from typing import Dict, FrozenSet, List, Tuple
import json
import pandas as pd
import requests
import subprocess
import time


def prometheus_config(scrape_interval_ms: int, jobs: Dict[str, List[str]]):
    """
    prometheus_config returns a JSON representation of a prometheus.yml
    configuration file. jobs maps job names to addresses.
    """
    return {
        'global': {
            'scrape_interval': f'{scrape_interval_ms}ms'
        },
        'scrape_configs': [
            {
                'job_name': job_name,
                'static_configs': [{'targets': addressses}],
            }
            for (job_name, addressses) in jobs.items()
        ],
    }


class PrometheusQueryer:
    """A queryable Prometheus server.

    PrometheusQueryer can be used to execute PromQL [1] queries against
    Prometheus data stored in a data directory named `tsdb_path`. For example,
    imagine we have Prometheus data stored in `data/`. We can do the following:

        with PrometheusQueryer('data/') as prometheus:
            df = prometheus.query('up[24h]')

    `df` is the result of running the PromQL query `up[24h]` and might look
    something like this:

                                       <metric_name>
        2019-04-05 18:51:08.917000055  1
        2019-04-05 18:51:09.117000103  1
        2019-04-05 18:51:09.316999912  1
        2019-04-05 18:51:09.516999960  1
        2019-04-05 18:51:09.717000008  1
        2019-04-05 18:51:09.917000055  1

    `df` is a DataFrame, indexed by time, with one column per returned metric.
    Every column is labeled with a frozenset of the corresponding Prometheus
    labels. For example, a metric name might look something like this:

        frozenset({(__name__, up), (job: foo)(instance, 10.0.0.1:8000)})

    [1]: https://prometheus.io/docs/prometheus/latest/querying/basics/
    """

    # popen is a function like Popen.
    def __init__(self, tsdb_path: str, popen = subprocess.Popen) -> None:
        # We launch prometheus with an empty configuration file.
        empty_prometheus_config = '/tmp/empty_prometheus.yml'
        with open(empty_prometheus_config, 'w') as f:
            f.write('')

        # We run prometheus on an arbitrary port.
        self.address = 'localhost:12345'
        cmd = [
            'prometheus',
            f'--config.file={empty_prometheus_config}',
            f'--storage.tsdb.path={tsdb_path}',
            f'--web.listen-address={self.address}',
        ]
        self.proc = popen(cmd)

    def __enter__(self) -> 'PrometheusQueryer':
        return self

    def __exit__(self, cls, exn, traceback) -> None:
        self.proc.terminate()

    def query(self, q: str) -> pd.DataFrame:
        """
        If we query a prometheus server right after we start it, it may not
        be ready to receive HTTP requests yet. So, if we fail to connect, we
        sleep a bit and try again.
        """
        num_retries = 10
        for i in range(num_retries - 1):
            try:
                return self._query_once(q)
            except (ConnectionRefusedError,
                    requests.exceptions.ConnectionError,
                    ValueError):
                time.sleep(i * 0.1)
        return self._query_once(q)

    def _query_once(self, q: str) -> pd.DataFrame:
        """
        See [1] for what Prometheus results look like.

        [1]: https://prometheus.io/docs/prometheus/latest/querying/api/
        """
        r = requests.get(f'http://{self.address}/api/v1/query',
                         params={'query': q, 'timeout': '1000s'})
        if r.status_code == 503:
            # Service is unavailable.
            raise ValueError(f'Query "{q}" resulted in a 503.')
        if r.json()['status'] == 'success':
            pass
        elif r.json()['status'] == 'error':
            raise ValueError(f'Query "{q}" resulted in error: {r["error"]}.')
        else:
            raise ValueError(f'Unknown status {r["status"]}')

        series: Dict[FrozenSet[Tuple[str, str]], pd.Series] = {}
        for stream in r.json()['data']['result']:
            if r.json()['data']['resultType'] == 'matrix':
                values = stream['values']
            else:
                values = [stream['value']]

            timestamps = [t for [t, x] in values]
            timestamps = (pd.to_datetime(timestamps, unit='s', origin='unix')
                            .tz_localize('UTC'))
            values = [x for [t, x] in values]
            s = pd.Series(values, index=timestamps)
            series[frozenset(stream['metric'].items())] = s

        return pd.DataFrame(series)
