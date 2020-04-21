import unittest
import pandas as pd
import numpy as np
import calc_mean_and_median
import statistics
import argparse
import os

class mean_and_median_test(unittest.TestCase):
	
	def test_results_with_python_functions(self):
		"""
		Tests if mean and median obtained using python functions
		is the same as the mean and median calculated using the
		code in calc_mean_and_median.py.

		For the sake of testing, I am assuming that the total data
		fits on 1 machine.
		"""
		parser = argparse.ArgumentParser()
		parser.add_argument("--f",
							default="",
							type=str,
							help="Folder name for data")
		args = parser.parse_args()

		file_first_quarter_path = os.path.join(args.f, 'file1.csv')
		file_second_quarter_path = os.path.join(args.f, 'file2.csv')
		file_third_quarter_path = os.path.join(args.f, 'file3.csv')
		file_forth_quarter_path = os.path.join(args.f, 'file4.csv')
		
		data1 = pd.read_csv(file_first_quarter_path)
		values = list(data1['Values'])
		data2 = pd.read_csv(file_second_quarter_path)
		values.extend(list(data2['Values']))
		data3 = pd.read_csv(file_third_quarter_path)
		values.extend(list(data3['Values']))
		data4 = pd.read_csv(file_forth_quarter_path)
		values.extend(list(data4['Values']))

		python_mean = statistics.mean(values)
		python_median = statistics.median(values)

		func_mean, func_median = calc_mean_and_median.calc_mean_median(parser = parser)

		self.assertAlmostEqual(python_mean, func_mean)
		self.assertAlmostEqual(python_median, func_median)

if __name__ == "__main__":
	unittest.main(argv=['first-arg-is-ignored'], exit=False)