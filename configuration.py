import count_words_test
import max_grade_test

# Directory for the input data file
DATA_FILE = "text.txt"

# Directory for the output files
RESULT_FILE = "result.txt"

# Number for the map processes (any positive number higher than one)
N_MAPPERS = 4

# Number for the reduce processes (any positive number higher than one)
N_REDUCERS = 4

# Custom map function for Your data
MAP_FUNCTION = count_words_test.words_count_map

# Custom reduce function for Your data
REDUCE_FUNCTION = count_words_test.words_count_reduce

# Make combine step while data processing (True or False)
DO_COMBINE = True

# Clean temporary files for map and reduce processes (True or False)
CLEAN_TEMP_FILES = True
