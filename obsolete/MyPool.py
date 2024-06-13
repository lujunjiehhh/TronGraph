import multiprocessing
import time

from multiprocessing import Pool
import time


class MyPool:
    task_queue = multiprocessing.JoinableQueue()
    result_queue = multiprocessing.Queue()

    def __init__(self, processes):
        self.pool = []
        for i in range(processes):
            p = multiprocessing.Process(target=self.worker)
            p.daemon = False
            p.start()
            self.pool.append(p)

    def map(self, func, iterable):
        for item in iterable:
            self.task_queue.put((func, item))

        # Add a 'None' task for each worker process
        for _ in range(len(self.pool)):
            self.task_queue.put((None, None))

        result = []
        for i in range(len(iterable)):
            result.append(self.result_queue.get())
            # print(result)
        print(result)
        return result

    def worker(self):
        while True:
            func, item = self.task_queue.get()
            if func is None:
                self.task_queue.task_done()
                # If the task is 'None', it means no more tasks left.
                # Break the loop and the process will exit.
                break
            result = func(item)
            self.result_queue.put(result)
            self.task_queue.task_done()

    def close(self):
        for p in self.pool:
            p.terminate()

    def __del__(self):
        self.close()


def func(x):
    time.sleep(1)
    return x * x


if __name__ == '__main__':
    pool = MyPool(4)
    iterable = range(10)
    print(pool.map(func, iterable))
    pool.close()
    print(pool.map(func, iterable))
    pool.close()
    print('Test Done.')
