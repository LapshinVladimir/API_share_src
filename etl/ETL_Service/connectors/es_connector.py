import json
import logging
from typing import Any, Dict, List

import backoff
import elasticsearch
from elasticsearch import Elasticsearch, NotFoundError, helpers


class ElasticsearchConnector:
    """
    Класс отвечающий за соединение с Elasticsearch, создание индекса
    и запись данных
    """

    @backoff.on_exception(backoff.expo, Exception, max_tries=5)
    def __init__(self, es_host: str, logger: logging.Logger) -> None:
        self.es = Elasticsearch([es_host])
        self.logger = logger
        self.mapping_file_paths = {
            'movies': 'index_films.json',
            'genres': 'index_genres.json',
            'persons': 'index_persons.json'
        }
        self.number_of_entries = 100

    def check_index(self, index) -> bool:
        try:
            self.es.indices.get_alias(index=index)
            return True
        except NotFoundError:
            return False

    def check_elasticsearch(self):
        try:
            return self.es.ping()
        except elasticsearch.ElasticsearchWarning as e:
            pass

    def create_mapping(self, index: str) -> None:
        try:
            with open(self.mapping_file_paths[index], 'r') as file:
                mapping = json.load(file)
        except FileNotFoundError:
            self.logger.info(f'Ошибка загрузки файла индекса!')
        self.es.indices.create(index=index, ignore=400, body=mapping)

    @backoff.on_exception(backoff.expo, Exception, max_tries=5)
    def bulk_index_entries(
        self, index_name: str, entries: List[Dict[str, Any]]
    ) -> None:

        for number in range(0, len(entries), self.number_of_entries):
            pack_of_entries = entries[number: number + self.number_of_entries]


            f = [
                    {'_index': index_name, '_id': doc['id'], '_source': doc}
                    for doc in pack_of_entries
                ]

            helpers.bulk(
                self.es,
                [
                    {'_index': index_name, '_id': doc['id'], '_source': doc}
                    for doc in pack_of_entries
                ],
            )
            self.logger.info(
                f'В индекс - {index_name} дозаписано {len(pack_of_entries)} записей!'
            )
