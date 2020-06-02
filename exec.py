from maincontroller import createCommand
import multiprocessing as mp

# Calculate number of processes
NUMBER_OF_PROCESSES = 12

p = mp.Pool(NUMBER_OF_PROCESSES)
p.map(createCommand, [0])#range(NUMBER_OF_PROCESSES))
