import abc
from abc import ABC, abstractmethod


class IDataAccess(ABC):

    @abc.abstractmethod
    def load_feed_data(self, feed_files: list):
        pass
