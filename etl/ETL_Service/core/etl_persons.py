import logging

from connectors.db_connector import DatabaseConnector
from connectors.es_connector import ElasticsearchConnector
from service.state_storage import State
from service.transform_person import process_to_es_format
from core.etl_base import ETLBase


class ETLPersons(ETLBase):
    def __init__(self, db_connector: DatabaseConnector, es_connector: ElasticsearchConnector, state: State,
                 logger: logging.Logger):
        super().__init__(db_connector, es_connector, state, logger)
        pass

    def processing_data(self, entries_ids, index):
        entries = process_to_es_format(
            self.db_connector.get_persons_by_id(entries_ids)
        )
        self.es_connector.bulk_index_entries(index, entries)

    def activate(self):
        self.get_collection('person')
