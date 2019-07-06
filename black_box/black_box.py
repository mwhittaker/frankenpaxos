import rbfopt
import numpy as np
import pandas as pd

data_path = 'results_YOQVYWDJEU.csv'
columns_to_drop = ['net_name', 'command_size_bytes_stddev', 'command_sleep_time_nanos_mean',
        'command_sleep_time_nanos_stddev', 'profiled', 'monitored', 'prometheus_scrape_interval_ms',
        'leader_log_level']
optimization_columns = ['mean_latency_ms', 'median_latency_ms', 'p90_latency_ms', 'p95_latency_ms',
        'p99_latency_ms', 'mean_1_second_throughput', 'median_1_second_throughput', 'p90_1_second_throughput',
        'p95_1_second_throughput', 'p99_1_second_throughput', 'mean_2_second_throughput',
        'median_2_second_throughput', 'p90_2_second_throughput', 'p95_2_second_throughput',
        'p99_2_second_throughput', 'mean_5_second_throughput', 'median_5_second_throughput',
        'p90_5_second_throughput', 'p95_5_second_throughput', 'p99_5_second_throughput']
round_dict = {'CLASSIC_ROUND_ROBIN': 0, 'MIXED_ROUND_ROBIN': 1}
thrifty_dict = {'NotThrifty': 0, 'Closest': 1, 'Random': 2}
data = pd.read_csv(data_path)
data = data.dropna()
data = data.drop(columns_to_drop, axis=1)
data = data.replace({'round_system_type': round_dict, 'leader.thrifty_system': thrifty_dict})
specific_optimization_index = 0

def get_lower_bounds(data):
    decision_df = data.drop(optimization_columns, axis=1)
    lower_bounds = []
    for i in range(len(decision_df.columns)):
        lower_bound = decision_df.iloc[:,i].min()
        lower_bounds.append(lower_bound)
    return np.array(lower_bounds)

def get_upper_bounds(data):
    decision_df = data.drop(optimization_columns, axis=1)
    upper_bounds = []
    for i in range(len(decision_df.columns)):
        upper_bound = decision_df.iloc[:,i].max()
        upper_bounds.append(upper_bound)
    return np.array(upper_bounds)

def objective_function(x):
    filtered_data = data
    #x = filtered_data.iloc[0].tolist()
    #print(x)
    for i in range(len(x)):
        condition = filtered_data[filtered_data.columns[i]] == x[i]
        filtered_data = filtered_data[condition]
    if filtered_data.empty:
        return sum(x)
    return filtered_data[optimization_columns[specific_optimization_index]].tolist()[0]

lower_bounds = get_lower_bounds(data)
upper_bounds = get_upper_bounds(data)
types = ['I' for i in range(len(lower_bounds) - 2)]
types.append('R')
types.append('R')
types = np.array(types)
dimension = len(lower_bounds)
#print(objective_function(lower_bounds))
print(dimension, len(types))

bb = rbfopt.RbfoptUserBlackBox(dimension, lower_bounds, upper_bounds, types, objective_function)
settings = rbfopt.RbfoptSettings(max_evaluations=50)
alg = rbfopt.RbfoptAlgorithm(settings, bb)
#print(alg.optimize())
alg.optimize()
