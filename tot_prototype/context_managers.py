from pymongo import MongoClient 


class MongoDbService():

    def __init__(self, host='localhost', port=27017):
        self.host = host
        self.port = port
    
    def __enter__(self):
        self.client = MongoClient(self.host, self.port)
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def __call__(self, *args, **kwargs):
        """
        TODO, this decorator is unfinished
        """
        service = self.__enter__()
        ret = self.func(*args, service=service, **kwargs)
        self.__exit__()
        return ret
