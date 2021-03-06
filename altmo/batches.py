"""
These are a series of "Batch" objects that are meant to be used in async workflows
"""
from __future__ import annotations

import abc
import asyncio
import logging
import sys
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Callable

import aiocsv
import aiofiles
from psycopg2.errors import UniqueViolation

from altmo.api.valhalla import ValhallaAsyncClient, get_matrix_request
from altmo.data.decorators import async_postgres_cursor_method
from altmo.data.types import StraightDistanceRow, Point
from altmo.data.write import add_amenity_residence_distance_async
from altmo.utils import grouper

logger = logging.getLogger("batches")
logging.getLogger("chardet.charsetprober").disabled = True


@dataclass
class BatchConfig:
    costing: str
    out: str
    file_name: str


class ReaderBatchError(Exception):
    pass


class ReaderBatch(abc.ABC, Sequence):
    """
    Creates jobs which read data from various sources
    """
    _producers: list[asyncio.Task]

    @abc.abstractmethod
    def register(self, queue: asyncio.Queue) -> None:
        ...

    def __getitem__(self, item):
        """
        Makes this item behave like a list, using self._producers as a its underlying list
        """
        return self._producers[item]

    def __len__(self):
        """
        Retrieve the length of the
        """
        return len(self._producers)


class WriterBatch(abc.ABC):
    """
    Base writer class
    """
    @abc.abstractmethod
    async def consume(self, queue: asyncio.Queue) -> None:
        ...

    def register(self, queue: asyncio.Queue) -> None:
        """
        Creates a task and then adds our _handle_results callback
        """
        def _handle_result(task_: asyncio.Task) -> None:
            try:
                task_.result()
            except asyncio.exceptions.CancelledError:
                pass
            except Exception as exc:
                raise exc

        task = asyncio.create_task(self.consume(queue))
        task.add_done_callback(_handle_result)


def batch_manager(reader: ReaderBatch, writer: WriterBatch) -> Callable:
    """
    Returns a callable which bootstraps the batch_manager process
    """
    queue = asyncio.Queue()

    async def run():
        """Runs the batch import process, linking `reader` and `writer` together"""
        reader.register(queue)
        writer.register(queue)

        await asyncio.gather(*reader)
        await queue.join()

        # Cancel all remaining tasks
        tasks = [
            t for t in asyncio.all_tasks()
            if t is not asyncio.current_task()
        ]

        tuple(t.cancel() for t in tasks)

    return run


class ValhallaReaderBatch(ReaderBatch):
    """
    Manages reading operations from the Valhalla API
    """

    # Current limit of batch size for this API
    VALHALLA_BATCH_LIMIT: int = 50

    def __init__(
        self, data: list[StraightDistanceRow], client: ValhallaAsyncClient, config: BatchConfig
    ):
        self.data = data
        self.client = client
        self.config = config
        self._producers = []

    def register(self, queue: asyncio.Queue) -> None:
        """
        Sets the producers attribute using the provided queue object
        """
        self._producers = []
        for residence, amenities in self._group_data_by_residence().items():
            group = grouper(amenities, size=self.VALHALLA_BATCH_LIMIT)

            for idx, amt_batch in enumerate(group, start=1):
                task = asyncio.create_task(self.produce(queue, residence, amt_batch))
                self._producers.append(task)
                logger.info(f'Adding Task {idx} for {residence}')

    def _group_data_by_residence(self) -> dict[Point, list[Point]]:
        """
        Groups the `self.data` attribute by residence(id, lat, lng)
        """
        residence_group = defaultdict(list)
        for row in self.data:
            residence_group[
                Point(row.residence_id, row.residence_lat, row.residence_lng)
            ].append(
                Point(row.amenity_id, row.amenity_lat, row.amenity_lng)
            )
        return residence_group

    async def produce(self, queue: asyncio.Queue, residence: Point, amenities: list[Point]):
        """
        Task to retrieve data from Valhalla API using its `sources_to_targets` endpoint
        """
        json_data = get_matrix_request(residence, amenities, costing=self.config.costing)
        data = (
            StraightDistanceRow(residence.id, amenity.id, residence.lat, residence.lng, amenity.lat, amenity.lng)
            for amenity in amenities
        )
        resp = None

        try:
            resp = (await self.client.source_to_targets(json=json_data))
            await queue.put((resp['sources_to_targets'][0], data))
        except (KeyError, IndexError):
            raise ReaderBatchError(f'Malformed response: {resp}')


class StdOutWriterBatch(WriterBatch):
    """
    Writes val_batch data to std out
    """
    def __init__(self, config: BatchConfig):
        self.config = config

    async def consume(self, queue: asyncio.Queue) -> None:
        """
        Consumer that prints the results it receives to stdout using whatever print function was supplied.
        """
        while True:
            http_res, db_res = await queue.get()
            for http_data, db_data in zip(http_res, db_res):
                row = (
                    http_data['distance'], http_data['time'],
                    db_data.amenity_id, db_data.residence_id,
                    self.config.costing
                )
                str_row = (str(fld) for fld in row)
                sys.stdout.write(f'{",".join(str_row)}\n')
            queue.task_done()


class CSVWriterBatch(WriterBatch):
    """
    Writes a ValhallaReaderBatch to a CSV file
    """
    def __init__(
        self,
        file_obj: aiofiles.threadpool.text.AsyncTextIOWrapper,
        config: BatchConfig,
        delimiter: str = ',',
        quote_char: str = '"'
    ):
        self.config = config
        self._file_obj = file_obj
        self._csv_writer = aiocsv.AsyncWriter(self._file_obj, quotechar=quote_char, delimiter=delimiter)

    async def consume(self, queue: asyncio.Queue) -> None:
        """
        Consumer function that writes what it receives to a CSV File
        """
        while True:
            http_res, db_res = await queue.get()
            for http_data, db_data in zip(http_res, db_res):
                row = (
                    http_data['distance'], http_data['time'], db_data.amenity_id,
                    db_data.residence_id, self.config.costing
                )
                await self._csv_writer.writerow(row)
            queue.task_done()


class DBWriterBatch(WriterBatch):
    """
    Saves the records inside of ValhallaReaderBatch to our database
    """

    def __init__(self, config: BatchConfig):
        self.config = config

    async def consume(self, queue: asyncio.Queue) -> None:
        """
        Writes records to PostgreSQL database
        """
        while True:
            http_res, db_res = await queue.get()
            new_records = []
            for http_data, db_data in zip(http_res, db_res):
                row = (
                    http_data['distance'], http_data['time'], db_data.amenity_id,
                    db_data.residence_id, self.config.costing
                )
                logger.info(f'Record to be written: {row}')
                new_records.append(row)

            await self._insert_records(new_records)
            queue.task_done()

    @async_postgres_cursor_method
    async def _insert_records(self, cur, new_records):
        """
        Attempt to insert data into database.
        """
        try:
            await add_amenity_residence_distance_async(cur, new_records)
            logger.info(f'Added {len(new_records)} new records')
        except UniqueViolation as exc:
            logger.error(f'{str(exc)}\n')
