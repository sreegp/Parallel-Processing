# Parallel-Processing
I implemented median calculation for large datasets located across machines with minimal data transfer overhead.

I used Python's multiprocessing package.

Problem Definition:
Log file of floats or ints is too big to be consumed by 1 machine. It is located on 4 machines. The goal of this problem is to calculate the exact average and median in a distributed manner without putting all the data on 1 machine.

Overall Assumptions:
1) Unsorted data is split approximately equally into 4 files before the start of the program. Each machine reads one file.
2) 4 machines are simulated using 4 processes.
3) There is a 5th process in main that communicates with the 4 parallel processes to calculate the exact average and median.

Median Calculation:

For the median calculation, a pivot value is randomly selected from data in one of the 4 processes. This pivot value is shared with all 4 processes using a queue. Each process does a count of values less than or equal to the pivot and a count of values more than the pivot and returns this to the 5th process. The 5th process sums the values less than or equal to the pivot and the values more than the pivot.

Based on the total length of the data (used to calculate average), the 5th process determines an initial position for the median (total_length // 2 + 1). If the sum of values less than or equal to the pivot equals the position of the median, we exit out of the loop.

The pivot is equal to the median if the length of the total data is odd, or the pivot is equal to the larger of the 2 medians if the length of the total data is even.
If the sum of values less than or equal to the pivot is less than the position of the median, the 5th process tells the other processes to discard values lower than or equal to the median. The median is one of the values greater than the pivot. The position of the median is reduced by the total number of values discarded.
On the other hand, if the sum of values less than or equal to the pivot is greater than the position of the median, the 5th process tells the other processes to discard values greater than the median. The median is one of the values less than or equal to the pivot.

A new pivot is randomly selected from data in one of the 4 processes. Each process once again does a count of values less than or equal to the pivot and a count of values more than the pivot and returns this to the 5th process, to aggregate the sums and compare with the new position of the median. This routine repeats until the sum of values less than or equal to the pivot equals the position of the median.

Odd vs Even Length Total Data:

If the total length of the data is odd, we have found the median (the pivot) and we are done. However, if the total length of the data is even, we have found the larger of the 2 medians and need to find the smaller median and average the two.
In order to find the smaller median, the 4 current processes are ended and 4 new processes are started. This is because parts of the total dataset have been discarded in different iterations. These discarded values might contain the smaller median. These 4 new processes find the largest value in their data that is smaller than the median. The 5th process receives these values and selects the largest of them. This is the smaller median. The 5th process averages the smaller and larger median values to obtain the median for the total dataset.

Processes Running out of Data:

If any one of the processes run out of the data, it is terminated and removed from the dictionary of processes. It is no longer used. The rest of the processes continue to find the median.

Assumption specific to approach:

1) Values in the data across the 4 processes are ​unique and not repeating​. The
approach will work for cases whereby the randomly chosen pivots are unique but other elements are repeating.

Data:

It is assumed that data is provided as a csv file. The values from the csv file are converted to lists for median calculation. Multiple datasets are used for testing.
