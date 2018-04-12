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

    def __reset_results(self):
        """Resets results for multiple runs"""
        self.mapped_collection = {}
        self.output_collection = {}

    @staticmethod
    def _generate_rand_input(size):
        """generates and returns a rand input of a size: size"""
        return [randint(0, 9) for p in range(0, size)]

    @staticmethod
    def __get_partition_sizes(n, number_of_partitions):
        """
        Splits on almost equal partitions
        :param n: size
        :param number_of_partitions: number_of_partitions
        :return: list of partition's sizes
        """
        size = int(n / number_of_partitions)
        remainder =  n % number_of_partitions
        chunk_sizes = [size for i in range(0, number_of_partitions)]
        for i in range(0, remainder):
            chunk_sizes[i % number_of_partitions] += 1
        return chunk_sizes

    def _map_chunk(self, id_from, id_to):
        """
        Maps part of the initial_array
        :param id_from: first index of a chunk [included]
        :param id_to: last index of a chunk [not included]
        :return: void; modifies mapped_collection of a class
        """
        for i in range(id_from, id_to):
            if self.initial_collection[i] in self.mapped_collection:
                self.mapped_collection[self.initial_collection[i]].append(1)
            else:
                self.mapped_collection[self.initial_collection[i]] = [1]

    def map_parallel(self):
        """
        Maps the initial_collection
        :return: void; dict of {key: [1,1,1...]} is stored in class prop mapped_collection; with 1 for each occurrence
        """
        threads = []
        chunk_sizes = self.__get_partition_sizes(len(self.initial_collection), self.number_of_partitions)
        id_to = 0
        for i in range(0, len(chunk_sizes)):
            id_from = id_to
            id_to = min(id_from + chunk_sizes[i], len(self.initial_collection))
            # id_from = int(i * self.partition_size)
            # id_to = int(min((i + 1) * self.partition_size, len(self.initial_collection)))
            p = Thread(target=self._map_chunk, args=(id_from, id_to))
            p.start()
            threads.append(p)

        for thread in threads:
            thread.join()

    def _reduce_chunk(self, id_from, id_to):
        """
        Reduces part of the mapped_array
        :param id_from: first index of a chunk [included]
        :param id_to: last index of a chunk [not included]
        :return: void; modifies output_collection of a class
        """
        for key in list(self.mapped_collection.keys())[id_from:id_to]:
            if key in self.output_collection:
                self.output_collection[key] += len(self.mapped_collection[key])
            else:
                self.output_collection[key] = len(self.mapped_collection[key])

    def reduce_parallel(self):
        """
        Reduces the structure returned by map
        :return: void; dict of {key: number_of_occurrences} is stored in class prop output_collection
        """
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

    def parallel_map_reduce(self):
        """
        Performs parallel map reduce
        :return: elapsed
        """
        self.__reset_results()

        start = time.time()
        self.map_parallel()
        print('MapResults: ' + str(self.mapped_collection)[0:256] + '...')
        self.reduce_parallel()
        print('Redused: ' + str(self.output_collection))
        print('Total(for check): ' + str(sum(self.output_collection.values())))
        elapsed = time.time() - start
        print('Elapsed:' + str(elapsed))
        return elapsed


    def seq_map_reduce(self):
        """
        Simply map and reduce the whole list as a single chunk
        :return: Elapsed time
        """
        self.__reset_results()

        start = time.time()
        id_from = 0
        id_to = len(self.initial_collection)
        self._map_chunk(id_from, id_to)
        print('SeqMapResults: ' + str(self.mapped_collection)[0:256] + '...')
        self._reduce_chunk(id_from, id_to)
        print('Seq Redused: ' + str(self.output_collection))
        print('Seq Total(for check): ' + str(sum(self.output_collection.values())))
        elapsed = time.time() - start
        print('Seq Elapsed:' + str(elapsed))
        return elapsed


if __name__ == '__main__':
    par_map_reduce = ParallelMapReduce()
    par_map_reduce.parallel_map_reduce()
    print()
    par_map_reduce.seq_map_reduce()


