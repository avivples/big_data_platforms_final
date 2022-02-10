import os
import sys
import warnings
from time import time

import numpy as np

from utils.files import create_files, merge_files
from map_reduce.map_reduce_engine import MapReduceEngine
from map_reduce.map_reduce_functions import inverted_map, inverted_reduce
from utils.sql import create_connection, create_table

warnings.filterwarnings('ignore')


def main(argv):
	# Create CSV files
	num_of_files, min_num_of_rows, max_num_of_rows = int(argv[0]), int(argv[1]), int(argv[2])
	create_files(num_of_files, min_num_of_rows, max_num_of_rows)

	os.makedirs("output/mapreducetemp", exist_ok=True)
	os.makedirs("output/mapreducefinal", exist_ok=True)

	# Merge CSV files using Worst Fit Algorithm
	merge_files()

	sql_create_table = """ CREATE TABLE IF NOT EXISTS temp_results (
	                           key TEXT,
							   value TEXT
	                           ); """

	# create a database connection
	sql_conn = create_connection("output/mydb.db")

	# create table
	create_table(sql_conn, sql_create_table)

	mapreduce = MapReduceEngine()

	# Run MapReduce on the merged files and large files
	large_files_start = time()
	large_files_data = [f"output/large_files/{_}" for _ in os.listdir('output/large_files') if _.endswith('.csv')]
	large_files_status = mapreduce.execute(large_files_data, inverted_map, inverted_reduce, sql_conn)
	large_files_elapsed_time = np.round(time() - large_files_start, 3)
	print(large_files_status)
	print(f"The MapReduce took {large_files_elapsed_time} seconds on the merged and large files")

	# Run MapReduce on the original small files
	small_files_start = time()
	small_files_data = [f"output/{_}" for _ in os.listdir('output') if _.endswith('.csv')]
	small_files_status = mapreduce.execute(small_files_data, inverted_map, inverted_reduce, sql_conn)
	small_files_elapsed_time = np.round(time() - small_files_start, 3)
	print(small_files_status)
	print(f"The MapReduce took {small_files_elapsed_time} seconds on the small files")

	# delete all temporary data from mapreducetemp folder and delete SQLite database
	mapreducetemp_path = "output/mapreducetemp"
	for file in os.listdir(mapreducetemp_path):
		os.remove(os.path.join(mapreducetemp_path, file))
	del sql_conn
	os.remove("output/mydb.db")


if __name__ == '__main__':
	main(sys.argv[1:])
