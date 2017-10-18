# -*- coding: utf-8 -*-

import abc
from abc import ABCMeta


class BaseProcessor(metaclass=ABCMeta):

    @abc.abstractmethod
    def extract(self, **kwargs):
        return

    @abc.abstractmethod
    def run(self, **kwargs):
        return


class FeedProcessor(BaseProcessor):
    feed = None
    data = []

    def __init__(self, db_warehouse=None, **kwargs):
        if not db_warehouse:
            raise ValueError(
                "Need the database warehouse object to persist the feed data."
            )
        self.db_warehouse = db_warehouse

    def clear(self):
        self._data = []
