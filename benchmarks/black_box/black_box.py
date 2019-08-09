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
import shutil
from skopt.space import Real, Integer
from skopt import gp_minimize, dummy_minimize

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
        'ping_period_ms': 12, 'no_ping_timeout_min_ms': 13, 'no_ping_timeout_max_ms': 14,
        'not_enough_votes_timeout_min_ms': 15, 'not_enough_votes_timeout_max_ms': 16,
        'fail_period_ms': 17, 'success_period_ms': 18, 'num_retries': 19, 'network_delay_alpha': 21,
        'repropose_period_ms': 20}

user_param_list = [1, 1000, 30, 2, 'KeyValueStore',
        workload.UniformSingleKeyWorkload(num_keys = 100, size_mean = 1, size_std = 0), False, False,
        200, 'debug']
analysis = []
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
                value_chosen_buffer_flush_period_ms = param_list[protocol_param_to_index_dict['value_chosen_buffer_flush_period_ms']],
                election = ElectionOptions()._replace(
                    ping_period_ms = param_list[protocol_param_to_index_dict['ping_period_ms']],
                    no_ping_timeout_min_ms = param_list[protocol_param_to_index_dict['no_ping_timeout_min_ms']],
                    no_ping_timeout_max_ms = param_list[protocol_param_to_index_dict['no_ping_timeout_max_ms']],
                    not_enough_votes_timeout_min_ms = param_list[protocol_param_to_index_dict['not_enough_votes_timeout_min_ms']],
                    not_enough_votes_timeout_max_ms = param_list[protocol_param_to_index_dict['not_enough_votes_timeout_max_ms']]
                ),
                heartbeat = HeartbeatOptions()._replace(
                    fail_period_ms = param_list[protocol_param_to_index_dict['fail_period_ms']],
                    success_period_ms = param_list[protocol_param_to_index_dict['success_period_ms']],
                    num_retries = param_list[protocol_param_to_index_dict['num_retries']],
                    network_delay_alpha = param_list[protocol_param_to_index_dict['network_delay_alpha']]
                )
            ),
            leader_log_level = user_param_list[user_param_to_index_dict['leader_log_level']],
            client = ClientOptions()._replace(
                repropose_period_ms = param_list[protocol_param_to_index_dict['repropose_period_ms']])
        )
    return [input_tuple] * num_runs

def get_lower_bounds(user_params):
    lower_bounds_dict = {'f': 1, 'round_system_type': 0,
            'timeout_seconds': user_params[user_param_to_index_dict['duration_seconds']] + 30, 'wait_period_ms': 0, 'wait_stagger_ms': 0,
            'thrifty_system': 0, 'resend_phase1as_timer_period_ms': 1, 'resend_phase2as_timer_period_ms': 1,
            'phase2a_max_buffer_size': 1, 'phase2a_buffer_flush_period_ms': 1,
            'value_chosen_max_buffer_size': 1, 'value_chosen_buffer_flush_period_ms': 1,
            'repropose_period_ms': 1, 'ping_period_ms': 1, 'no_ping_timeout_min_ms': 1,
            'no_ping_timeout_max_ms': 1, 'not_enough_votes_timeout_min_ms': 1, 'not_enough_votes_timeout_max_ms': 1, 'fail_period_ms': 1, 'success_period_ms': 1, 'num_retries': 1,
            'network_delay_alpha': 0.01}
    lower_bounds = []
    for key in protocol_param_to_index_dict.keys():
        lower_bounds.insert(protocol_param_to_index_dict[key], lower_bounds_dict[key])
    return lower_bounds

def get_upper_bounds(user_params):
    upper_bounds_dict = {'f': 5, 'round_system_type': 1,
            'timeout_seconds': user_params[user_param_to_index_dict['duration_seconds']] + 31,
            'wait_period_ms': user_params[user_param_to_index_dict['duration_seconds']]*1000,
            'wait_stagger_ms': user_params[user_param_to_index_dict['duration_seconds']]*1000,
            'thrifty_system': 2,
            'resend_phase1as_timer_period_ms': user_params[user_param_to_index_dict['duration_seconds']]*1000,
            'resend_phase2as_timer_period_ms': user_params[user_param_to_index_dict['duration_seconds']]*1000,
            'phase2a_max_buffer_size': user_params[user_param_to_index_dict['num_client_procs']] * user_params[user_param_to_index_dict['num_clients_per_proc']],
            'phase2a_buffer_flush_period_ms': user_params[user_param_to_index_dict['duration_seconds']]*1000,
            'value_chosen_max_buffer_size': 10000000,
            'value_chosen_buffer_flush_period_ms':user_params[user_param_to_index_dict['duration_seconds']],
            'repropose_period_ms': user_params[user_param_to_index_dict['duration_seconds']]*1000,
            'ping_period_ms': user_params[user_param_to_index_dict['duration_seconds']]*1000,
            'no_ping_timeout_min_ms': user_params[user_param_to_index_dict['duration_seconds']]*1000,
            'no_ping_timeout_max_ms': user_params[user_param_to_index_dict['duration_seconds']]*1000,
            'not_enough_votes_timeout_min_ms': user_params[user_param_to_index_dict['duration_seconds']]*1000,
            'not_enough_votes_timeout_max_ms': user_params[user_param_to_index_dict['duration_seconds']]*1000,
            'fail_period_ms': user_params[user_param_to_index_dict['duration_seconds']]*1000,
            'success_period_ms': user_params[user_param_to_index_dict['duration_seconds']]*1000,
            'num_retries': 3,
            'network_delay_alpha': 0.99
            }
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
    new_x = []
    for i in range(len(x)):
        if i == protocol_param_to_index_dict['network_delay_alpha']:
            new_x.append(float(x[i]))
        else:
            new_x.append(int(x[i]))

    min_ping = new_x[protocol_param_to_index_dict['no_ping_timeout_min_ms']]
    max_ping = new_x[protocol_param_to_index_dict['no_ping_timeout_max_ms']]
    min_votes = new_x[protocol_param_to_index_dict['not_enough_votes_timeout_min_ms']]
    max_votes = new_x[protocol_param_to_index_dict['not_enough_votes_timeout_max_ms']]

    if min_ping > max_ping:
        new_x[protocol_param_to_index_dict['no_ping_timeout_min_ms']] = max_ping
        new_x[protocol_param_to_index_dict['no_ping_timeout_max_ms']] = min_ping
    if min_votes > max_votes:
        new_x[protocol_param_to_index_dict['not_enough_votes_timeout_min_ms']] = max_votes
        new_x[protocol_param_to_index_dict['not_enough_votes_timeout_max_ms']] = min_votes

    suite = BBFastMultiPaxosSuite(new_x, user_param_list)
    print('Selected parameters are ')
    print_parameters(new_x)
    f = io.StringIO()
    objective_value = 0.0
    with benchmark.SuiteDirectory(suite.args()['suite_directory'], 'fastmultipaxos_blackbox') as dir:
        print('Directory is: ' + str(dir.path))
        with redirect_stdout(f):
            # Call run_benchmark directly instead or in run_suite change return value
            suite.run_suite(dir)
        out = f.getvalue()
        out_dict_str = (out[out.find('{'):out.find('}')+1]).strip()
        out_dict = ast.literal_eval(str(out_dict_str))

        print('P90 throughput 1 second windows is ' + out_dict['stop_throughput_1s.p90'])
        objective_value = float(out_dict['stop_throughput_1s.p90'])
    shutil.rmtree(dir.path)
    analysis.append(new_x + [objective_value])
    return -objective_value

def print_parameters(input_tuple):
    parameters = ''
    reversed_param_dict = {v: k for k, v in protocol_param_to_index_dict.items()}
    for i in range(len(input_tuple)):
        parameters = parameters + reversed_param_dict[i] + ': ' + str(input_tuple[i]) + '\n'
    print(parameters)

def print_user_parameters():
    parameters = ''
    for key, value in user_param_to_index_dict.items():
        parameters = parameters + key + ': ' + str(user_param_list[value]) + '\n'
    print(parameters)

def get_scikit_optimize_dimensions():
    lower_bounds = get_lower_bounds(user_param_list)
    upper_bounds = get_upper_bounds(user_param_list)
    dimensions = []

    for i in range(len(lower_bounds)):
        if i != protocol_param_to_index_dict['network_delay_alpha']:
            dimensions.insert(i, Integer(lower_bounds[i], upper_bounds[i]))
        else:
            dimensions.insert(i, Real(lower_bounds[i], upper_bounds[i]))
    return dimensions

def write_data_frame():
    reversed_param_dict = {v: k for k, v in protocol_param_to_index_dict.items()}
    column_list = []
    for i in range(len(protocol_param_to_index_dict)):
        column_list.append(reversed_param_dict[i])
    column_list.append('p90 throughput 1 second windows')
    df = pd.DataFrame(analysis, columns = column_list)
    df.to_csv('frankenpaxos.csv', sep='\t')



#print(run_objective_function(get_lower_bounds(user_param_list)))
print_user_parameters()
lower_bounds = get_lower_bounds(user_param_list)
upper_bounds = get_upper_bounds(user_param_list)
types = ['I' for i in range(len(lower_bounds) - 1)]
types.append('R')
#types.append('R')
#types = np.array(types)
dimension = len(lower_bounds)
#print(objective_function(lower_bounds))
#print(dimension, len(types))

#bb = rbfopt.RbfoptUserBlackBox(dimension, lower_bounds, upper_bounds, types, run_objective_function)
#settings = rbfopt.RbfoptSettings(max_evaluations=1)
#alg = rbfopt.RbfoptAlgorithm(settings, bb)
#print(alg.optimize())
res = dummy_minimize(run_objective_function, get_scikit_optimize_dimensions(), n_calls=20)
print(res)
write_data_frame()
#alg.optimize()
