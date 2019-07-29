import rbfopt
import numpy as np
import pandas as pd
from .. import benchmark, workload
#from ..fastmultipaxos import *
#from ..fastmultipaxos.fastmultipaxos import FastMultiPaxosSuite, Input, Output, ThriftySystemType, RoundSystemType, AcceptorOptions, LeaderOptions, ClientOptions
from ..fastmultipaxos.fastmultipaxos import *
from typing import Any, Callable, Dict, List, NamedTuple, Collection
import io
from contextlib import redirect_stdout
import ast
import yaml
import json

round_dict = {'CLASSIC_ROUND_ROBIN': 0, 'MIXED_ROUND_ROBIN': 1}
thrifty_dict = {'NotThrifty': 0, 'Closest': 1, 'Random': 2}
user_param_to_index_dict = {'num_client_procs': 0, 'num_clients_per_proc': 1, 'duration_seconds': 2,
        'client_lag_seconds': 3, 'state_machine': 4, 'workload': 5, 'profiled': 6, 'monitored': 7,
        'prometheus_scrape_interval_ms': 8, 'leader_log_level': 9}
protocol_param_to_index_dict = {'f': 0, 'round_system_type': 1,
        'timeout_seconds': 2,
        'wait_period_ms': 3, 'wait_stagger_ms': 4,
        'thrifty_system': 5, 'resend_phase1as_timer_period_ms': 6, 'resend_phase2as_timer_period_ms': 7,
        'phase2a_max_buffer_size': 8, 'phase2a_buffer_flush_period_ms': 9,
        'value_chosen_max_buffer_size': 10, 'value_chosen_buffer_flush_period_ms': 11,
        'repropose_period_ms': 12}

user_param_list = [1, 1000, 60, 2, 'KeyValueStore',
        workload.UniformSingleKeyWorkload(num_keys = 100, size_mean = 1, size_std = 0), False, False,
        200, 'debug']

def parse_bb_input(param_list, user_param_list):
    num_runs = 1
    rev_round_dict = {0: RoundSystemType.CLASSIC_ROUND_ROBIN, 1: RoundSystemType.ROUND_ZERO_FAST,
            2: RoundSystemType.MIXED_ROUND_ROBIN}
    state_machine_dict = {0: 'Register', 1: 'KeyValue'}

    thrifty_dict = {0: ThriftySystemType.NOT_THRIFTY,
            1: ThriftySystemType.CLOSEST, 2: ThriftySystemType.RANDOM}

    input_tuple = Input(f = param_list[protocol_param_to_index_dict['f']],
            num_client_procs = user_param_list[user_param_to_index_dict['num_client_procs']],
            num_clients_per_proc = user_param_list[user_param_to_index_dict['num_clients_per_proc']],
            round_system_type = rev_round_dict[param_list[protocol_param_to_index_dict['round_system_type']]],
            duration_seconds = user_param_list[user_param_to_index_dict['duration_seconds']],
            timeout_seconds = param_list[protocol_param_to_index_dict['timeout_seconds']],
            client_lag_seconds = user_param_list[user_param_to_index_dict['client_lag_seconds']],
            state_machine = user_param_list[user_param_to_index_dict['state_machine']],
            workload = user_param_list[user_param_to_index_dict['workload']],
            profiled = user_param_list[user_param_to_index_dict['profiled']],
            monitored = user_param_list[user_param_to_index_dict['monitored']],
            prometheus_scrape_interval_ms = user_param_list[user_param_to_index_dict['prometheus_scrape_interval_ms']],
            acceptor = AcceptorOptions()._replace(
                wait_period_ms = param_list[protocol_param_to_index_dict['wait_period_ms']],
                wait_stagger_ms = param_list[protocol_param_to_index_dict['wait_stagger_ms']]),
            leader = LeaderOptions()._replace(
                thrifty_system = thrifty_dict[param_list[protocol_param_to_index_dict['thrifty_system']]],
                resend_phase1as_timer_period_ms = param_list[protocol_param_to_index_dict['resend_phase1as_timer_period_ms']],
                resend_phase2as_timer_period_ms = param_list[protocol_param_to_index_dict['resend_phase2as_timer_period_ms']],
                phase2a_max_buffer_size = param_list[protocol_param_to_index_dict['phase2a_max_buffer_size']],
                phase2a_buffer_flush_period_ms = param_list[protocol_param_to_index_dict['phase2a_buffer_flush_period_ms']],
                value_chosen_max_buffer_size = param_list[protocol_param_to_index_dict['value_chosen_max_buffer_size']],
                value_chosen_buffer_flush_period_ms = param_list[protocol_param_to_index_dict['value_chosen_buffer_flush_period_ms']]
            ),
            leader_log_level = user_param_list[user_param_to_index_dict['leader_log_level']],
            client = ClientOptions()._replace(
                repropose_period_ms = param_list[protocol_param_to_index_dict['repropose_period_ms']])
        )
    return [input_tuple] * num_runs

def get_lower_bounds(user_params):
    lower_bounds_dict = {'f': 1, 'round_system_type': 0,
            'timeout_seconds': 60, 'wait_period_ms': 0, 'wait_stagger_ms': 0,
            'thrifty_system': 0, 'resend_phase1as_timer_period_ms': 1, 'resend_phase2as_timer_period_ms': 1,
            'phase2a_max_buffer_size': 1, 'phase2a_buffer_flush_period_ms': 1,
            'value_chosen_max_buffer_size': 1, 'value_chosen_buffer_flush_period_ms': 1,
            'repropose_period_ms': 1}
    lower_bounds = []
    for key in protocol_param_to_index_dict.keys():
        lower_bounds.insert(protocol_param_to_index_dict[key], lower_bounds_dict[key])
    return lower_bounds

def get_upper_bounds(user_params):
    upper_bounds_dict = {'f': 5, 'round_system_type': 2,
            'timeout_seconds': user_params[2],
            'wait_period_ms': user_params[2],
            'wait_stagger_ms': user_params[2],
            'thrifty_system': 2, 'resend_phase1as_timer_period_ms': user_params[2],
            'resend_phase2as_timer_period_ms': user_params[2],
            'phase2a_max_buffer_size': user_params[0] * user_params[1],
            'phase2a_buffer_flush_period_ms': user_params[2],
            'value_chosen_max_buffer_size': 10000000,
            'value_chosen_buffer_flush_period_ms': user_params[2],
            'repropose_period_ms': user_params[2]}
    upper_bounds = []
    for key in protocol_param_to_index_dict:
        upper_bounds.insert(protocol_param_to_index_dict[key], upper_bounds_dict[key])
    return upper_bounds

class BBFastMultiPaxosSuite(FastMultiPaxosSuite):
    def __init__(self, user_params, param_list):
        self.user_params = user_params
        self.param_list = param_list
        FastMultiPaxosSuite.__init__(self)

    def args(self) -> Dict[Any, Any]:
        return vars(get_parser().parse_args())

    def inputs(self) -> Collection[Input]:
        return parse_bb_input(self.user_params, self.param_list)

    def summary(self, input: Input, output: Output) -> str:
        return str({
            'latency.median_ms': f'{output.latency.median_ms:.6}',
            'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}'})

def run_objective_function(x):
    new_x = [int(element) for element in x]
    suite = BBFastMultiPaxosSuite(new_x, user_param_list)
    print('Selected parameters are ')
    print(new_x)
    f = io.StringIO()
    with benchmark.SuiteDirectory(suite.args()['suite_directory'], 'fastmultipaxos_blackbox') as dir:
        with redirect_stdout(f):
            suite.run_suite(dir)
        out = f.getvalue()
        out_dict_str = (out[out.find('{'):out.find('}')+1]).strip()
        out_dict = ast.literal_eval(str(out_dict_str))
        print('Output is ' + out_dict['stop_throughput_1s.p90'])
        return float(out_dict['stop_throughput_1s.p90'])


#print(run_objective_function(get_lower_bounds(user_param_list)))
lower_bounds = get_lower_bounds(user_param_list)
upper_bounds = get_upper_bounds(user_param_list)
types = ['I' for i in range(len(lower_bounds))]
#types.append('R')
#types.append('R')
#types = np.array(types)
dimension = len(lower_bounds)
#print(objective_function(lower_bounds))
#print(dimension, len(types))

bb = rbfopt.RbfoptUserBlackBox(dimension, lower_bounds, upper_bounds, types, run_objective_function)
settings = rbfopt.RbfoptSettings(max_evaluations=50)
alg = rbfopt.RbfoptAlgorithm(settings, bb)
print(alg.optimize())
#alg.optimize()
