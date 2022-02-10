import os
import warnings

import pandas as pd
from tqdm import tqdm

from utils.threads import pool_executor_wrapper

warnings.filterwarnings('ignore')


class MapReduceEngine:
	def execute(self, input_data, map_function, reduce_function, sql_conn):
		"""
		Execute the mapreduce steps: map, shuffle and sort, reduce
		Parallelize the map and reduce steps by creating threads to run the
		map and reduce function in parallel on the partitioned data

		Parameters:
		input_data (dict): Dictionary to apply map_function on
		map_function: Map function to apply on input_data
		reduce_function: Reduce function on the shuffled and sorted output of
						 map_function

		Returns:
		str: Completion status message
		"""

		# Start a thread for each key in input_data and run map_function on it
		num_of_threads = len(input_data)
		mapreducetemp_path = "output/mapreducetemp"

		# Define a progress bar to visualize progress of threads
		with tqdm(total=num_of_threads, desc='Map Progress Bar') as pbar:
			map_status = pool_executor_wrapper(map_function, input_data, pbar, num_of_threads, mapreducetemp_path)

		if map_status is not None:
			return map_status

		# Load content of all CSV files into temp_results table
		for file in os.listdir(mapreducetemp_path):
			if file.endswith(".csv"):
				data = pd.read_csv(os.path.join(mapreducetemp_path, file))
				data.to_sql('temp_results', sql_conn, if_exists='append', index=False)

		# SQL statement to generate a sorted list by key
		sql_generate_sorted_list = """
                                   SELECT key, GROUP_CONCAT(value)
                                   FROM temp_results
                                   GROUP BY key
                                   ORDER BY key;
                                   """
		sorted_list = sql_conn.execute(sql_generate_sorted_list).fetchall()

		# Start a thread for each value from the generated list
		num_of_threads = len(sorted_list)

		# Define a progress bar to visualize progress of threads
		with tqdm(total=num_of_threads, desc='Reduce Progress Bar') as pbar:
			reduce_status = pool_executor_wrapper(reduce_function, sorted_list, pbar, num_of_threads, 'output/mapreducefinal')

		if reduce_status is not None:
			return reduce_status

		return "MapReduce Completed"
