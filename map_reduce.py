"""

    Copyright 2018. Oleksandr Pryhoda

    Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
    documentation files (the "Software"), to deal in the Software without restriction, including without limitation
    the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
    and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all copies or substantial
    portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
    TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
    THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
    CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
    IN THE SOFTWARE.

    Map/Reduce framework implementation using Python 3.6
    @license http://www.opensource.org/licenses/mit-license.html  MIT License
    @author Oleksandr Pryhoda <pryhoda@ucu.edu.ua>

"""

from configuration import DATA_FILE, RESULT_FILE, N_MAPPERS, N_REDUCERS,\
    MAP_FUNCTION, REDUCE_FUNCTION, DO_COMBINE, CLEAN_TEMP_FILES

import multiprocessing
from multiprocessing import Pool
import os
import csv


class MapReduce(object):
    """MapReduce class representing the map/reduce model"""

    def __init__(self, data_file=DATA_FILE, result_file=RESULT_FILE, n_mappers=N_MAPPERS, n_reducers=N_REDUCERS,
                 map_function=MAP_FUNCTION, reduce_function=REDUCE_FUNCTION, combine=DO_COMBINE, clean=CLEAN_TEMP_FILES):
        """
        Default initialization using configuration.py file
        :param data_file: file where the data to be processed is located
        :param result_file: file for results
        :param n_mappers: number of map workers
        :param n_reducers: number of reduce workers
        :param map_function: custon function for mapping process
        :param reduce_function: custom function for reducing process
        :param combine: use of combine step
        :param clean: clean all temporary files created during work process
        """
        self.data_file = data_file
        self.result_file = result_file
        self.n_mappers = n_mappers
        self.n_reducers = n_reducers
        self.map_function = map_function
        self.reduce_function = reduce_function
        self.combine = combine
        self.clean = clean

    def _file_reader(self):
        """
        Read data from file

        :return: data from file
        """
        if self.data_file.endswith('.txt'):
            with open(self.data_file, 'r', encoding='ascii', errors='ignore') as txtfile:
                data = txtfile.readlines()
        elif self.data_file.endswith('.csv'):
            with open(self.data_file, newline='') as csvfile:
                data = list(csv.DictReader(csvfile))
        else:
            print("Wrong format for input data file!")
            return

        return data

    def _file_writer(self, file_name, data, option):
        """
        Write data in file

        :param file_name: name of file to write
        :param data: data to write
        :param option: option for file opening (a - add file, w - write file and etc.)
        """
        with open(file_name, option, encoding='utf-8') as file:
            for key, value in data:
                file.write('{}, {}\n'.format(key, value))

    def _mapper(self, data, first, last, file_index):
        """
        Call map function with part of data

        :param data: list of data
        :param first: start index in list fot specific mapper
        :param last: end index of list for specific mapper
        :param file_index: index of temporary file in which write data
        """
        mapped_data = self.map_function(data, first, last)
        if self.combine:
            mapped_data = self._combiner(mapped_data)

        self._file_writer('mapper_file_' + str(file_index) + '.txt', mapped_data, 'w')

    def _combiner(self, mapped_data):
        """
        Merge values for same keys

        :param mapped_data: data from mapper
        :return: combined data
        """
        comb_dict = {}
        for key, value in mapped_data:
            if key in comb_dict.keys():
                comb_dict[key] += value
            else:
                comb_dict[key] = value

        return comb_dict.items()

    def _shuffler(self):
        """
        Merge data from all mappers in one place and sort it

        :return: merged and sorted data
        """
        mapped_data = []
        for index in range(self.n_mappers):
            file_to_read = 'mapper_file_' + str(index) + '.txt'
            with open(file_to_read) as file:
                for line in file:
                    readed_line = line.strip().split(",")
                    mapped_data.append(readed_line)

            if self.clean:
                os.remove(file_to_read)

        mapped_data.sort(key=lambda x: x[0])
        shuffled_data = []
        current_key = mapped_data[0][0]
        temp_lst = []
        for key, value in mapped_data:
            if key == current_key:
                temp_lst.append([key, value])
            else:
                current_key = key
                shuffled_data.append(temp_lst)
                temp_lst = [[key, value]]

        return shuffled_data

    def _reducer(self, shuffled_data):
        """
        Call reduce function with part of data

        :param shuffled_data: part of merged and sorted data for specific reduce worker
        """
        file_index = shuffled_data[0]
        data = shuffled_data[1]

        for data_for_node in data:
            reduced_data = self.reduce_function(data_for_node)
            self._file_writer('reducer_file_' + str(file_index) + '.txt', reduced_data, 'a')

    def _run_mapper(self, data):
        """
        Distribute data to map along mapper workers

        :param data: data to map
        """
        avg, last = len(data)/self.n_mappers, 0
        map_processes = []
        file_index = 0
        while last < len(data):
            map_processes.append(multiprocessing.Process(target=self._mapper, args=(data, int(last), int(last + avg), file_index)))
            last += avg
            file_index += 1

        for process in map_processes:
            process.start()

        for process in map_processes:
            process.join()

    def _run_reducer(self, shuffled_data):
        """
        Distribute data to reduce along reducer workers

        :param data: data to map
        """
        data_size, file_index = len(shuffled_data), 0
        avg, last = data_size / self.n_mappers, 0

        data_for_reducers = []
        while last < data_size:
            data_for_reducers.append([file_index, shuffled_data[int(last):int(last+avg)]])
            last += avg
            file_index += 1

        with Pool(self.n_reducers) as reduce_pool:
            reduce_pool.map(self._reducer, data_for_reducers)

    def _write_result(self):
        """
        Write results of Map/Reduce framework into file
        """
        reduced_files = [filename for filename in os.listdir(os.getcwd())
                         if filename in ['reducer_file_' + str(i) + '.txt' for i in range(self.n_reducers)]]

        with open(self.result_file, 'w') as outfile:
            for filename in reduced_files:
                with open(filename, 'r') as readfile:
                    if self.result_file.endswith('.csv'):
                        writer_file = csv.writer(outfile)
                        writer_file.writerow([readfile.read()])
                    elif self.result_file.endswith('.txt'):
                        outfile.write(readfile.read() + '\n')
                    else:
                        print("Wrong format of output file!!!")

                if self.clean:
                    os.remove(filename)

    def run(self):
        """
        Perform all work of Map/Reduce framework
        """
        data = self._file_reader()
        self._run_mapper(data)

        shuffled_data = self._shuffler()
        self._run_reducer(shuffled_data)

        self._write_result()
