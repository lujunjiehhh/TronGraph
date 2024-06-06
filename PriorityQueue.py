import redis


class PriorityQueueClient:
    def __init__(self, name):
        self.redis = redis.Redis()
        self.name = name

    def enqueue(self, item, priority):
        # 将元组转换为字符串进行存储
        item_str = f"{item[0]}:{item[1]}"
        self.redis.zadd(self.name, {item_str: priority})

    def dequeue(self) -> tuple[tuple[str, int], float]:
        """
        Dequeues and returns the highest priority item from the queue.
        :return: A tuple containing the dequeued item and its priority.
                 The item is a tuple of type (str, int), representing the key and value respectively.
                 The priority is an integer.
        """
        items = self.redis.zpopmax(self.name)
        if items:
            item_str, priority = items[0]
            # 将字符串转换回元组
            key, value = item_str.decode("utf-8").split(':')
            item = (key, int(value))
            return item, priority

    def size(self):
        return self.redis.zcard(self.name)

    def is_empty(self):
        return self.size() == 0

    def clear(self):
        self.redis.delete(self.name)


if __name__ == "__main__":
    redis_queue = PriorityQueueClient("priority_queue")
    redis_queue.enqueue(("a", 1), 1.2)
    redis_queue.enqueue(("b", 2), 2.2)
    redis_queue.enqueue(("c", 3), 3.2)
    print(redis_queue.dequeue())
    print(redis_queue.dequeue())
    print(redis_queue.is_empty())
    print(redis_queue.size())
