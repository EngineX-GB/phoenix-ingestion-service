import abc
from abc import ABC, abstractmethod


class IDataAccess(ABC):

    @abc.abstractmethod
    def load_feed_data(self, feed_files: list):
        pass

    @abc.abstractmethod
    def load_feed_data_by_directory(self, directory_path : str):
        pass