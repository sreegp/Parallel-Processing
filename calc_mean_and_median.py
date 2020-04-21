import math
import random
import sys
import time
import os
import multiprocessing as mp
import numpy as np
import pandas as pd
import argparse

def count_values_pivot(file_path, queue_choose_pivot, queue_sum_len, queue_use_pivot, queue_pivot_count, queue_discard, queue_checklen): 
	"""
	Every process runs this function. 

	It reads in data, provides a pivot, gathers count less than or equal to pivot,
	and more than pivot. It discards data based on the instructions from the 5th
	process. It ends the process if there is no data left, and sends a flag to the
	5th process.

	"""
	# 1) Read in the data from a csv file, convert it to a list.
	data = pd.read_csv(file_path)
	values = list(data['Values'])
	
	# 2) Put the length and sum of values into a queue which will by
	# read in by the 5th process.
	length = len(values)
	sum_val = sum(values)
	
	queue_sum_len.put([sum_val, length])

	while True:
	# 3) Provide a random value for the pivot. 
		random_value_for_pivot = random.choice(values)
		queue_choose_pivot.put(random_value_for_pivot) 

		pivot = queue_use_pivot.get()

	# 4) Calculate count less than or equal to pivot and count more than pivot,
	# putting values less than or equal to pivot on 1 list and putting values 
	# more than the pivot on another list. Put counts in queue for 5th process
	# to aggregate.

		less_than_equal_pivot = []
		more_than_pivot = []
		for i in range(len(values)):
			if values[i] <= pivot:
				less_than_equal_pivot.append(values[i])
			elif values[i] > pivot:
				more_than_pivot.append(values[i])

		less_than_equal_pivot_count = len(less_than_equal_pivot)
		more_than_pivot_count = len(more_than_pivot)

		queue_pivot_count.put([less_than_equal_pivot_count, more_than_pivot_count])

	# 5) Get instructions from the 5th process on which data to discard,
	# either less or equal to pivot, or more than pivot.

		discard_flag = queue_discard.get()
		if discard_flag == 'discard_lower':
			del less_than_equal_pivot[:]
			values = more_than_pivot
		elif discard_flag == 'discard_upper':
			del more_than_pivot[:]
			values = less_than_equal_pivot

	# 6) If there are no values left in process, put 'not_ok' string in
	# queue and return 1. Process ends. 5th process will stop using this
	# process for future iterations.
   
		if len(values) == 0:
			queue_checklen.put('not_ok')
			return 1
		else:
			queue_checklen.put('ok')

def maxval_less_than_pivot(file_path, pivot, return_queue):
	"""
	This function is run when the length of the total data is even and
	a second value of the median has to be obtained for averaging.

	Based on the assumptions detailed in the report, 
	this second value is the largest value less than the larger median.
	"""

	# 1) Read in the data from a csv file, convert it to a list.
	data = pd.read_csv(file_path)
	values = list(data['Values'])
	max_less_than_pivot = min(values)
	# 2) Put largest value found that is less than the pivot into the
	# queue which is read by the 5th process.
	if (max_less_than_pivot == pivot) or (max_less_than_pivot > pivot):
		max_less_than_pivot = -float('inf')
		return_queue.put(max_less_than_pivot)
	else: 
		for i in range(0, len(values)):
			if (values[i] < pivot) and (values[i] > max_less_than_pivot):
				max_less_than_pivot = values[i]
	return_queue.put(max_less_than_pivot) 

def calc_mean_median(parser = None):
	# Create queue for choosing the pivot
	queue_choose_pivot = mp.Queue()
	# Create queues for getting sum and length of the data in each process
	queue_sum_len_1 = mp.Queue()
	queue_sum_len_2 = mp.Queue()
	queue_sum_len_3 = mp.Queue()
	queue_sum_len_4 = mp.Queue()
	# Create queue for send chosen pivot back to processes
	queue_use_pivot = mp.Queue()
	# Create queue to receive count less than or equal to pivot and 
	# more than pivot
	queue_pivot_count = mp.Queue()
	# Create queues to tell processes to discard parts of their data
	queue_discard_1 = mp.Queue()
	queue_discard_2 = mp.Queue()
	queue_discard_3 = mp.Queue()
	queue_discard_4 = mp.Queue()
	# Create queues to check the length of the data in each process
	# after every iteration.
	queue_checklen_p1 = mp.Queue()
	queue_checklen_p2 = mp.Queue()
	queue_checklen_p3 = mp.Queue()
	queue_checklen_p4 = mp.Queue()
	# Create queues to get max value less than pivot
	# This is for scenarios where length of the total data is even.
	return_queues = [mp.Queue() for _ in range(4)]

	# Change file path as its stored on the processes.
	if parser == None:
		parser = argparse.ArgumentParser()
		parser.add_argument("--f",
							default="",
							type=str,
							help="Folder name for data")
		args = parser.parse_args()
	else:
		args = parser.parse_args()

	file_first_quarter_path = os.path.join(args.f, 'file1.csv')
	file_second_quarter_path = os.path.join(args.f, 'file2.csv')
	file_third_quarter_path = os.path.join(args.f, 'file3.csv')
	file_forth_quarter_path = os.path.join(args.f, 'file4.csv')

	# Create 4 processes and map them to the count_values_pivot() function with different queues.

	p1 = mp.Process(target = count_values_pivot, args = (file_first_quarter_path, queue_choose_pivot, queue_sum_len_1, queue_use_pivot, queue_pivot_count, queue_discard_1, queue_checklen_p1))
	p2 = mp.Process(target = count_values_pivot, args = (file_second_quarter_path, queue_choose_pivot, queue_sum_len_2, queue_use_pivot, queue_pivot_count, queue_discard_2, queue_checklen_p2))
	p3 = mp.Process(target = count_values_pivot, args = (file_third_quarter_path, queue_choose_pivot, queue_sum_len_3, queue_use_pivot, queue_pivot_count, queue_discard_3, queue_checklen_p3))
	p4 = mp.Process(target = count_values_pivot, args = (file_forth_quarter_path, queue_choose_pivot, queue_sum_len_4, queue_use_pivot, queue_pivot_count, queue_discard_4, queue_checklen_p4))


	# Keep track of live processes. Initialize by assuming all processes are alive in the beginning.
	live_p={"p1":(p1,queue_checklen_p1),"p2":(p2,queue_checklen_p2),"p3":(p3,queue_checklen_p3),"p4":(p4,queue_checklen_p4)}

	# start all processes
	p1.start()
	p2.start()
	p3.start()
	p4.start()
	
	# Get sum and length of data from each of the processes
	sum1, length1 = queue_sum_len_1.get()
	sum2, length2 = queue_sum_len_2.get()
	sum3, length3 = queue_sum_len_3.get()
	sum4, length4 = queue_sum_len_4.get()

	# Calculate average by dividing total length by total sum.
	total_length = length1 + length2 + length3 + length4
	total_sum = sum1 + sum2 + sum3 + sum4
	average = total_sum / total_length 

	# Initialize the position of the median to be the middle position.
	# When the total length of the data is even, this is the value in the larger
	# median position. E.g. for a total length of 8, pos_median = 5
	# If the total length of the list is even, another routine below will 
	# obtain the second, smaller median. This code assumes values in the entire
	# data are unique. This is detailed in the report.
	pos_median = total_length // 2 + 1

	while True: 
	# Initialize less than or equal to pivot count as less_equal_count
	# Initialize more than pivot count as more_count
		less_equal_count = 0
		more_count = 0
	
	# Get pivot from processes
		pivot = queue_choose_pivot.get()

	# Drain queue to remove any additional pivot values added but not used as pivot.
		while not queue_choose_pivot.empty():
			queue_choose_pivot.get()

	# Send pivot back to other processes
	# live_p is the dict of live processes
		for i in range(len(live_p)):
			queue_use_pivot.put(pivot)
			
	# Get count of values less than or equal to pivot and more than pivot
	# live_p is the dict of live processes
		for i in range(len(live_p)):
			less_than_equal_pivot_count, more_than_pivot_count = queue_pivot_count.get()
			less_equal_count += less_than_equal_pivot_count
			more_count += more_than_pivot_count

	# Discard values less than or equal to pivot if less_equal_count
	# is less the the position of the median. 
	# Adjust the position of the median.
		if (less_equal_count < pos_median):
			queue_discard_1.put('discard_lower')
			queue_discard_2.put('discard_lower')
			queue_discard_3.put('discard_lower')
			queue_discard_4.put('discard_lower')
			pos_median = pos_median - less_equal_count

	# If less_equal_count is equal to the position of the median
	# the pivot is the median for an odd length less or one of the
	# values for the median calculation for an even length list
	# Break out of the loop here.
		elif (less_equal_count == pos_median):
			median = pivot
			break

	# Discard values more than the pivot if less_equal_count
	# is greater the the position of the median
	# The position of the median stays the same.

		else:
			queue_discard_1.put('discard_upper')
			queue_discard_2.put('discard_upper')
			queue_discard_3.put('discard_upper')
			queue_discard_4.put('discard_upper')	

	# Check for processes that are have not been terminated
	# Run future iterations for processes that are alive.
		new_live_p={}
		for pname,(p,q) in live_p.items():
			msg=q.get()
			if msg=="ok":
				new_live_p[pname]=(p,q)
		live_p=new_live_p


	# Terminate remaining processes
	p1.terminate()
	p2.terminate()
	p3.terminate()
	p4.terminate()

	p1.join() 
	p2.join() 
	p3.join() 
	p4.join() 

	# If the total length of the data is even, initialize 4 new processes and 
	# map them to the maxval_less_than_pivot() function with different queues.
	if total_length % 2 == 0:

		p5 = mp.Process(target = maxval_less_than_pivot, args = (file_first_quarter_path, pivot, return_queues[0]))
		p6 = mp.Process(target = maxval_less_than_pivot, args = (file_second_quarter_path, pivot, return_queues[1]))
		p7 = mp.Process(target = maxval_less_than_pivot, args = (file_third_quarter_path, pivot, return_queues[2]))
		p8 = mp.Process(target = maxval_less_than_pivot, args = (file_forth_quarter_path, pivot, return_queues[3]))

	# Start processes
		p5.start()
		p6.start()
		p7.start()
		p8.start()

	# Get highest value less than pivot from all processes
		high_vals=[rq.get() for rq in return_queues]
		smaller_median_value = max(high_vals)

		median = (median + smaller_median_value)/2

	# Terminate processes
		p5.terminate()
		p6.terminate()
		p7.terminate()
		p8.terminate()

		p5.join() 
		p6.join() 
		p7.join() 
		p8.join() 

	# print("average = ", average, "median =", median)
	
	# # Create function to return average and median
	# def return_mean_and_median(average, median):
	return average, median
	
	# # Return average and median
	# return_mean_and_median(average, median)


if __name__ == "__main__":
	average, median = calc_mean_median()	
	print('average', average)
	print('median', median)

	

	
	

	