# Run computations using map/reduce framework
from map_reduce import MapReduce


if __name__ == '__main__':
    count_words = MapReduce()
    count_words.run()

    # To run max grade finding task change configuration values to these ones:
    # 'grades.csv', 'result.csv', 4, 4, max_grade_test.max_grade_map, max_grade_test.max_grade_reduce, False, True
    # max_grade = MapReduce()
    # max_grade.run()
