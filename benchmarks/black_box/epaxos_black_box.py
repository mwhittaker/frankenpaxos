import rbfopt
import numpy as np
import pandas as pd
from .. import benchmark, workload
#from ..fastmultipaxos import *
#from ..fastmultipaxos.fastmultipaxos import FastMultiPaxosSuite, Input, Output, ThriftySystemType, RoundSystemType, AcceptorOptions, LeaderOptions, ClientOptions
from ..epaxos.epaxos import *
from typing import Any, Callable, Dict, List, NamedTuple, Collection
import io
from contextlib import redirect_stdout
import ast
import yaml
import json
import shutil
from skopt.space import Real, Integer
from skopt import gp_minimize, dummy_minimize
import datetime

thrifty_dict = {'NotThrifty': 0, 'Closest': 1, 'Random': 2}
user_param_to_index_dict = {'num_client_procs': 0, 'num_warmup_clients_per_proc': 1,
        'num_clients_per_proc': 2, 'warmup_duration': 3, 'warmup_timeout': 4, 'warmup_sleep': 5,
        'duration': 6, 'timeout': 7,
        'client_lag_seconds': 8, 'state_machine': 9, 'workload': 10, 'profiled': 11, 'monitored': 12,
        'prometheus_scrape_interval': 13, 'replica_log_level': 14, 'client_log_level': 15,
        'unsafe_skip_graph_execution': 16}
protocol_param_to_index_dict = {'f': 0, 'thrifty_system': 1,
        'resend_pre_accepts_timer_period': 2,
        'default_to_slow_path_timer_period': 3, 'resend_accepts_timer_period': 4,
        'resend_prepares_timer_period': 5, 'recover_instance_timer_min_period': 6,
        'recover_instance_timer_max_period': 7,
        'execute_graph_batch_size': 8,
        'execute_graph_timer_period': 9, 'repropose_period': 10,
        'replica_dependency_graph': 11}

user_param_list = [1, 1, 1000, 5, 15, 2, 30, 60, 1, 'KeyValueStore',
        workload.UniformSingleKeyWorkload(num_keys = 100, size_mean = 1, size_std = 0), False, False,
        200, 'debug', 'debug', False]
analysis = []
def parse_bb_input(param_list, user_param_list):
    num_runs = 1
    thrifty_dict = {0: 'Not_Thrifty', 1: 'Closest', 2: 'Random'}
    dep_graph_dict = {0: 'Jgrapht', 1: 'Tarjan', 2: 'IncrementalTarjan'}
    print(user_param_to_index_dict['client_log_level'], len(user_param_list))

    input_tuple = Input(f = param_list[protocol_param_to_index_dict['f']],
            num_client_procs = user_param_list[user_param_to_index_dict['num_client_procs']],
            num_warmup_clients_per_proc = user_param_list[user_param_to_index_dict['num_warmup_clients_per_proc']],
            num_clients_per_proc = user_param_list[user_param_to_index_dict['num_clients_per_proc']],
            warmup_duration = datetime.timedelta(seconds=user_param_list[user_param_to_index_dict['warmup_duration']]),
            warmup_timeout = datetime.timedelta(seconds=user_param_list[user_param_to_index_dict['timeout']]),
            warmup_sleep = datetime.timedelta(seconds=user_param_list[user_param_to_index_dict['warmup_sleep']]),
            duration = datetime.timedelta(seconds=user_param_list[user_param_to_index_dict['duration']]),
            timeout = datetime.timedelta(seconds=user_param_list[user_param_to_index_dict['timeout']]),
            client_lag = datetime.timedelta(seconds=user_param_list[user_param_to_index_dict['client_lag_seconds']]),
            state_machine = user_param_list[user_param_to_index_dict['state_machine']],
            workload = user_param_list[user_param_to_index_dict['workload']],
            profiled = user_param_list[user_param_to_index_dict['profiled']],
            monitored = user_param_list[user_param_to_index_dict['monitored']],
            prometheus_scrape_interval = datetime.timedelta(milliseconds=user_param_list[user_param_to_index_dict['prometheus_scrape_interval']]),
            replica_options = ReplicaOptions()._replace(
                thrifty_system = thrifty_dict[param_list[protocol_param_to_index_dict['thrifty_system']]],
                resend_pre_accepts_timer_period = datetime.timedelta(milliseconds=param_list[protocol_param_to_index_dict['resend_pre_accepts_timer_period']]),
                default_to_slow_path_timer_period = datetime.timedelta(milliseconds=param_list[protocol_param_to_index_dict['default_to_slow_path_timer_period']]),
                resend_accepts_timer_period = datetime.timedelta(milliseconds=param_list[protocol_param_to_index_dict['resend_prepares_timer_period']]),
                resend_prepares_timer_period = datetime.timedelta(milliseconds=param_list[protocol_param_to_index_dict['resend_prepares_timer_period']]),
                recover_instance_timer_min_period = datetime.timedelta(milliseconds=param_list[protocol_param_to_index_dict['recover_instance_timer_min_period']]),
                recover_instance_timer_max_period = datetime.timedelta(milliseconds=param_list[protocol_param_to_index_dict['recover_instance_timer_max_period']]),
                unsafe_skip_graph_execution = user_param_list[user_param_to_index_dict['unsafe_skip_graph_execution']],
                execute_graph_batch_size = param_list[protocol_param_to_index_dict['execute_graph_batch_size']],
                execute_graph_timer_period = datetime.timedelta(milliseconds=param_list[protocol_param_to_index_dict['execute_graph_timer_period']])),
            replica_log_level = user_param_list[user_param_to_index_dict['replica_log_level']],
            replica_dependency_graph = dep_graph_dict[param_list[protocol_param_to_index_dict['replica_dependency_graph']]],
            client_options = ClientOptions()._replace(
                repropose_period = datetime.timedelta(milliseconds=param_list[protocol_param_to_index_dict['repropose_period']])),
            client_log_level = user_param_list[user_param_to_index_dict['client_log_level']]
        )
    return [input_tuple] * num_runs

def get_lower_bounds(user_params):
    lower_bounds_dict = {'f': 1, 'thrifty_system': 0,
            'resend_pre_accepts_timer_period': 1,
            'default_to_slow_path_timer_period': 1,
            'resend_accepts_timer_period': 1,
            'resend_prepares_timer_period': 1,
            'recover_instance_timer_min_period': 1,
            'recover_instance_timer_max_period': 1,
            'execute_graph_batch_size': 1,
            'execute_graph_timer_period': 1,
            'repropose_period': 1,
            'replica_dependency_graph': 0
            }
    lower_bounds = []
    for key in protocol_param_to_index_dict.keys():
        lower_bounds.insert(protocol_param_to_index_dict[key], lower_bounds_dict[key])
    return lower_bounds

def get_upper_bounds(user_params):
    upper_bounds_dict = {'f': 5, 'thrifty_system': 2,
            'resend_pre_accepts_timer_period': user_params[user_param_to_index_dict['duration']]*1000,
            'default_to_slow_path_timer_period': user_params[user_param_to_index_dict['duration']]*1000,
            'resend_accepts_timer_period': user_params[user_param_to_index_dict['duration']]*1000,
            'resend_prepares_timer_period': user_params[user_param_to_index_dict['duration']]*1000,
            'recover_instance_timer_min_period': user_params[user_param_to_index_dict['duration']]*1000,
            'recover_instance_timer_max_period': user_params[user_param_to_index_dict['duration']]*1000,
            'execute_graph_batch_size': 1000000,
            'execute_graph_timer_period': user_params[user_param_to_index_dict['duration']]*1000,
            'repropose_period': user_params[user_param_to_index_dict['duration']],
            'replica_dependency_graph': 2
            }
    upper_bounds = []
    for key in protocol_param_to_index_dict:
        upper_bounds.insert(protocol_param_to_index_dict[key], upper_bounds_dict[key])
    return upper_bounds

class BBEPaxosSuite(EPaxosSuite):
    def __init__(self, user_params, param_list):
        self.user_params = user_params
        self.param_list = param_list
        #EPaxosSuite.__init__(self)

    def args(self) -> Dict[Any, Any]:
        return vars(get_parser().parse_args())

    def inputs(self) -> Collection[Input]:
        print(self.param_list, self.user_params)
        return parse_bb_input(self.param_list, self.user_params)

    def summary(self, input: Input, output: Output) -> str:
        return str({
            'latency.median_ms': f'{output.latency.median_ms:.6}',
            'stop_throughput_1s.p90': f'{output.stop_throughput_1s.p90:.6}'})

def run_objective_function(x):
    new_x = []
    for i in range(len(x)):
        new_x.append(int(x[i]))

    min_recover = new_x[protocol_param_to_index_dict['recover_instance_timer_min_period']]
    max_recover = new_x[protocol_param_to_index_dict['recover_instance_timer_max_period']]

    if min_recover > max_recover:
        new_x[protocol_param_to_index_dict['recover_instance_timer_min_period']] = max_recover
        new_x[protocol_param_to_index_dict['recover_instance_timer_max_period']] = min_recover

    suite = BBEPaxosSuite(user_param_list, new_x)
    print('Selected parameters are ')
    print_parameters(new_x)
    f = io.StringIO()
    objective_value = 0.0
    with benchmark.SuiteDirectory(suite.args()['suite_directory'], 'epaxos_blackbox') as dir:
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
        dimensions.insert(i, Integer(lower_bounds[i], upper_bounds[i]))
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
print(user_param_list[user_param_to_index_dict['client_log_level']])
print_user_parameters()
lower_bounds = get_lower_bounds(user_param_list)
upper_bounds = get_upper_bounds(user_param_list)
types = ['I' for i in range(len(lower_bounds))]
#types.append('R')
#types.append('R')
#types = np.array(types)
dimension = len(lower_bounds)
#print(objective_function(lower_bounds))
#print(dimension, len(types))

#bb = rbfopt.RbfoptUserBlackBox(dimension, lower_bounds, upper_bounds, types, run_objective_function)
#settings = rbfopt.RbfoptSettings(max_evaluations=1)
#alg = rbfopt.RbfoptAlgorithm(settings, bb)
#print(alg.optimize())
res = gp_minimize(run_objective_function, get_scikit_optimize_dimensions(), n_calls=20)
print(res)
write_data_frame()
#alg.optimize()
