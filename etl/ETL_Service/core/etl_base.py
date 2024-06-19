import logging

from connectors.db_connector import DatabaseConnector
from connectors.es_connector import ElasticsearchConnector
from service.state_storage import State
from datetime import datetime
from typing import Any, List


class ETLBase:
    def __init__(self,
                 db_connector: DatabaseConnector,
                 es_connector: ElasticsearchConnector,
                 state: State,
                 logger: logging.Logger,
                 ):
        self.db_connector = db_connector
        self.es_connector = es_connector
        self.state = state
        self.logger = logger
        self.delay_between_runs = 30

    def processing_data(self, entries_ids, index):
        pass

    def check_attended_info(
        self, target: str, last_updated_at: str
    ) -> tuple[List[tuple[Any, ...]], str]:
        collection = self.db_connector.get_changed_collection(
            target, last_updated_at
        )
        if collection:
            return (
                collection,
                str(collection[-1][1])[:-6],
            )
        return None

    def get_collection(self, target: str = 'movies'):
        last_updated_at = (
            self.state.get_state(f'last_updated_at', f'{target}_last_update')
            or str(datetime.min)
        )

        while data := self.check_attended_info(target, last_updated_at):
            collection = data[0]
            updated_at = data[1]
            self.processing_data(collection, target)
            if last_updated_at < updated_at:
                last_updated_at = updated_at
                state_val = self.state.get_state(f'last_updated_at')
                if state_val:
                    state_val.update({f'{target}_last_update': updated_at})
                else:
                    state_val = {f'{target}_last_update': updated_at}
                self.state.set_state(f'last_updated_at', state_val)

