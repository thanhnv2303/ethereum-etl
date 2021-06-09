from knowledge_graph_etl.exporter.database.singleton import Singleton


class MemoryStorage:
    __instance = None

    @staticmethod
    def getInstance():
        """ Static access method. """
        if Singleton.__instance == None:
            Singleton()
        return Singleton.__instance

    def __init__(self):
        """ Virtually private constructor. """
        if Singleton.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            Singleton.__instance = self
        self.storage = {}

    def add_element(self, key, value):
        self.storage[key] = value

    def get_elemet(self, key):
        return self.storage.get(key)
