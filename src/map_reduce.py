

class ParallelMapReduce(object):
    def __init__(self):
        pass

    def _split_list(self, list_for_split, number_of_partitions):
        """
        Splits list into approximately equal size sublists
        :param list_for_split: original list
        :param number_of_partitions: number of parts to split list into
        :return:
        """
        k, m = divmod(len(list_for_split), number_of_partitions)
        return (list_for_split[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(number_of_partitions)
                if list_for_split[i * k + min(i, m):(i + 1) * k + min(i + 1, m)])


    def _map_funk(self, chunk):

