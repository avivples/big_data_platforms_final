import logging

import warnings
warnings.filterwarnings('ignore')


def setup_logger():
	for handler in logging.root.handlers[:]:
		logging.root.removeHandler(handler)

	logging.basicConfig(filename='output/mapreduce.log', filemode='a',
						format='%(asctime)s | %(threadName)s | %(levelname)s | %(message)s',
						level=logging.INFO)

	return logging.getLogger()
