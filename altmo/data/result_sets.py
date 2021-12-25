import abc
from typing import Callable, Generator

from psycopg2.extensions import cursor

from altmo.data.utils import page_query
from altmo.data.read import get_residence_amenity_straight_distance_count, get_residence_amenity_straight_distance


class ResultSetContainer(abc.ABC):
    """
    Holds information about the result sets we want to retrieve and the data itself in the `result_sets` property.
    """

    def __init__(self, cur: cursor, study_area_id: int, batch_size: int, query_kwargs: dict = None):
        self.cursor = cur
        self.study_area_id = study_area_id
        self.batch_size = batch_size
        self._count = self._get_result_set_count()
        self.query_kwargs = query_kwargs if query_kwargs else {}

    def __repr__(self):
        return (
            f'<ResultSetContainer'
            'study_area_id={self.study_area_id} '
            'batch_size={self.batch_size} '
            'query_kwargs={self.query_kwargs}>'
        )

    @abc.abstractmethod
    def _get_result_set_count(self) -> int:
        ...

    @abc.abstractmethod
    def _get_result_set_func(self) -> Callable:
        ...

    @property
    def result_sets(self) -> Generator:
        return page_query(
            self._get_result_set_func(), self._count, self.batch_size,
            self.cursor, self.study_area_id, **self.query_kwargs
        )

    @result_sets.setter
    def result_sets(self, value):
        raise TypeError('Results is a read-only value')


class StraightDistanceResultSetContainer(ResultSetContainer):
    """
    Implementation of ResultSetContainer that uses the `get_residence_amenity_straight_distance*` functions
    """
    def _get_result_set_count(self) -> int:
        return get_residence_amenity_straight_distance_count(self.cursor, self.study_area_id)

    def _get_result_set_func(self, *args, **kwargs) -> Callable:
        return get_residence_amenity_straight_distance
