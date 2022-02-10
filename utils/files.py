import os
import random
import shutil
import string

import numpy as np
import pandas as pd
from tqdm import tqdm


def generate_random_name():
    """
    Generate a random name: First character is uppercase and
    alphabetic and the rest is series of lower case characters of length
    1 - 12. Giving us an output string of a length 2 - 13

    Returns:
    str: Randomly generated name
    """
    return random.choice(string.ascii_uppercase) + \
           ''.join(random.choices(string.ascii_lowercase, k=random.choice(range(1, 12))))


def generate_csvs(number_of_csvs, firstname, secondname, city, min_num_of_rows, max_num_of_rows):
    """
    Generate number_of_csvs csv files with a random number, 10-10,000
    of randomly generated rows for the defined columns by randomly picking
    a value from the lists of possible values for each column.

    Parameters:
    number_of_csvs (int): number of csv files to generate
    """
    random.seed(123)

    for i in tqdm(range(1, number_of_csvs+1), desc='Generating CSV'):
        df = pd.DataFrame(columns=['firstname', 'secondname', 'city'])
        n = np.random.randint(min_num_of_rows, max_num_of_rows + 1)
        df['firstname'] = np.random.choice(firstname, n)
        df['secondname'] = np.random.choice(secondname, n)
        df['city'] = np.random.choice(city, n)
        df.to_csv(f'output/myCSV{i}.csv', index=False)


def create_files(num_of_files, min_num_of_rows, max_num_of_rows):
    firstname = ['John', 'Dana', 'Scott', 'Marc', 'Steven', 'Michael', 'Albert', 'Johanna']
    city = ['NewYork', 'Haifa', 'Munchen', 'London', 'PaloAlto', 'TelAviv', 'Kiel', 'Hamburg']
    # Use list comprehension in order to create a list of 8 randomly generated names
    secondname = [generate_random_name() for _ in range(8)]  # please use some version of random

    generate_csvs(num_of_files, firstname, secondname, city, min_num_of_rows, max_num_of_rows)


def worst_fit(files, merge_limit):
	"""
	Using Worst Fit Strategy we sort the small files into queues that don't go over the merge limit

	Parameters
	files: list of files to merge
	merge_limit:

	Returns
	queues_files (list): list of lists of small files to be merged
	queues_size (list): list of the file sizes of the small files to be merged
	"""
	file_sizes = [os.path.getsize(file) for file in files]
	queues_file = []
	queues_size = []

	# Find the best queue that can accommodate file_size
	for i in tqdm(range(len(file_sizes)), desc='Worst fit algorithm'):
		# Initialize maximum space left and index of worst queue
		mx, wi = -1, 0

		for j in range(len(queues_size)):
			if (queues_size[j] >= file_sizes[i]) and (queues_size[j] - file_sizes[i] > mx):
				wi = j
				mx = queues_size[j] - file_sizes[i]

		# If no queue could accomodate file_size create a new queue
		if mx == -1:
			queues_size.append(merge_limit - file_sizes[i])
			queues_file.append([files[i]])
		else:
			queues_size[wi] -= file_sizes[i]
			queues_file[wi].append(files[i])

	return queues_file, queues_size


def merge_files():
	"""
	We take all the created files, segregate the large files from the small files, and merge the small files

	"""
	# Define the merge limit to 210 KB since the max file size is 230 KB in our case
	merge_limit = 210 * 1024

	# Create folder to save the large files and merged files
	os.makedirs("output/large_files", exist_ok=True)

	# Segregate large files from small files
	large_files = [f"output/{_}" for _ in os.listdir('output') if _.endswith('.csv') and os.path.getsize(f"output/{_}") >= merge_limit]

	# Copy large files into the new folder
	# In our case we copy so we can time MapReduce on the small files
	# as well as the large and merged files
	for large_file in large_files:
		shutil.copyfile(large_file, f"output/large_files/{os.path.split(large_file)[-1]}")

	small_files = [f"output/{_}" for _ in os.listdir('output') if _.endswith('.csv') and os.path.getsize(f"output/{_}") < merge_limit]

	merged_files, merged_files_size = worst_fit(small_files, merge_limit)

	# Merge the CSV files into one file
	for i, to_merge in tqdm(enumerate(merged_files), total=len(merged_files), desc='Merging CSVs'):
		df = pd.concat(map(pd.read_csv, to_merge), ignore_index=True)
		df.to_csv(f'output/large_files/myMergedCSV{i}.csv', index=False)