from random import randint
from threading import Thread

import time

from src.config import INPUT_SIZE, NUMBER_OF_PROCESSES


class ParallelMapReduce(object):
    def __init__(self, input=None, processes = NUMBER_OF_PROCESSES):
        self.__set_params(input, processes)

    def __set_params(self, input, processes):
        self.initial_collection = input if input else self._generate_rand_input(INPUT_SIZE)
        self.number_of_partitions = processes
        self.partition_size = len(self.initial_collection) / self.number_of_partitions
        self.mapped_collection = {}
        self.output_collection = {}

    @staticmethod
    def _generate_rand_input(size):
        return [randint(0, 9) for p in range(0, size)]


    def _map_chunk(self, process_number):
        id_from  = int(process_number * self.partition_size)
        id_to = int(min((process_number + 1) * self.partition_size, len(self.initial_collection)))
        print('{} - {}'.format(id_from, id_to))
        for i in range(id_from, id_to):
            if self.initial_collection[i] in self.mapped_collection:
                self.mapped_collection[self.initial_collection[i]] += '1'
            else:
                self.mapped_collection[self.initial_collection[i]] = '1'

    def map_parallel(self):
        threads = []
        for i in range(0, self.number_of_partitions):
            p = Thread(target=self._map_chunk, args=(i,))
            p.start()
            threads.append(p)

        for thread in threads:
            thread.join()

    @staticmethod
    def __get_partition_sizes(n, number_of_partitions):
        size = int(n / number_of_partitions)
        remainder =  n % number_of_partitions
        chunk_sizes = [size for i in range(0, number_of_partitions)]
        for i in range(0, remainder):
            chunk_sizes[i % number_of_partitions] += 1
        return chunk_sizes

    def _reduce_chunk(self, id_from, id_to):
        for key in list(self.mapped_collection.keys())[id_from:id_to]:
            if key in self.output_collection:
                self.output_collection[key] += len(self.mapped_collection[key])
            else:
                self.output_collection[key] = len(self.mapped_collection[key])

    def reduce_parallel(self):
        chunk_sizes = self.__get_partition_sizes(len(self.mapped_collection), self.number_of_partitions)
        threads = []
        id_to = 0
        for i in range(0, len(chunk_sizes)):
            id_from = id_to
            id_to = min(id_from + chunk_sizes[i], len(self.mapped_collection))
            t = Thread(target=self._reduce_chunk, args=(id_from, id_to))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

    def map_reduce(self):
        #self.__set_params(input_collection, processes)
        start = time.time()
        self.map_parallel()
        print('MapResults: ' + str(self.mapped_collection))
        self.reduce_parallel()
        print('Redused: ' + str(self.output_collection))
        print('Total(for check): ' + str(sum(self.output_collection.values())))
        elapsed = time.time() - start
        print('Elapsed:' + str(elapsed))


if __name__ == '__main__':
    # par_map_reduce = ParallelMapReduce()
    # par_map_reduce.map_parallel()
    # print(par_map_reduce.mapped_collection)
    # par_map_reduce.reduce_parallel()
    # print(par_map_reduce.output_collection)
    # print(sum(par_map_reduce.output_collection.values()))

    par_map_reduce = ParallelMapReduce()
    par_map_reduce.map_reduce()


