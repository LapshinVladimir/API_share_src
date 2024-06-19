import logging
from enum import Enum
from typing import Any, List

from connectors.db_connector import DatabaseConnector
from connectors.es_connector import ElasticsearchConnector
from service.state_storage import State
from service.transform_film import process_to_es_format
from core.etl_base import ETLBase


class Target(str, Enum):
    FILMS = 'film_work'
    PERSONS = 'person'
    GENRES = 'genre'

    def __str__(self):
        return f'{self.value}'


class ETLFilms(ETLBase):
    def __init__(self, db_connector: DatabaseConnector, es_connector: ElasticsearchConnector, state: State,
                 logger: logging.Logger):
        super().__init__(db_connector, es_connector, state, logger)
        pass

    def processing_data(self, entries_ids, index):
        entries = process_to_es_format(
            self.db_connector.get_films_by_id(entries_ids)
        )
        self.es_connector.bulk_index_entries(index, entries)

    def check_attended_info(
        self, target: str, last_updated_at: str
    ) -> tuple[List[tuple[Any, ...]], str]:
        collection = self.db_connector.get_changed_collection(
            target, last_updated_at
        )
        if collection:
            if target == str(Target.FILMS):
                return collection, str(collection[-1][1])[:-6]
            else:
                return (
                    self.db_connector.get_films_by_collection_id(
                        target, collection
                    ),
                    str(collection[-1][1])[:-6],
                )
        return None

    def activate(self):
        for t in Target:
            self.get_collection(str(t))
