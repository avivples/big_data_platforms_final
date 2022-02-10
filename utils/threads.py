import os
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from threading import current_thread

import pandas as pd

from utils import logger

warnings.filterwarnings('ignore')


def thread_wrapper(function, save_path, value):
	"""
	A wrapper to run the threads with logging capability. If the thread fails
	raise the exception.

	Parameters:
	function: function to run by thread
	save_path (str): path to save the csv
	value (tuple): value to run the function on and thread_id
	"""
	data, thread_id = value
	current_thread().name = f"{function.__name__} thread {thread_id}"
	logger.info('Started')
	try:
		# If tuple then it means it's the reduce function
		if type(data) is tuple:
			res = function(*data)
			df = pd.DataFrame(res)
			df.to_csv(f'{save_path}/part-{thread_id}-final.csv', index=False)
		# The map function
		else:
			res = function(data)
			df = pd.DataFrame(res, columns=['key', 'value'])
			df.to_csv(os.path.join(save_path, f'part-tmp-{thread_id}.csv'), index=False)
	except Exception as e:
		logger.error(e)
		raise e

	logger.info('Ended')


def pool_executor_wrapper(function, data, pbar, num_of_threads, save_path):
	"""
	A wrapper to execute the pool of threads

	Parameters:
	function: function to let the thread run
	data: data to run the function on
	pbar (tqdm): the progress bar we want to update as threads complete
	num_of_threads (int): the max number of threads we want to run
	save_path (str): path where we want to save our results

	Returns:
	str: if a thread fails returns a fail message otherwise returns nothing
	"""
	# Define the pool executor and start the threads

	with ProcessPoolExecutor() as executor:
		futures = list(map(lambda x: executor.submit(thread_wrapper, function, save_path, x),
						   list(zip(data, range(1, num_of_threads + 1)))))

		# Go over the list of threads and check the completion status of each
		for future in as_completed(futures):
			try:
				future.result()
			except Exception as e:
				return "MapReduce Failed"

			pbar.update(1)
