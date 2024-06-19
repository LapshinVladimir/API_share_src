import logging
import time
from datetime import datetime
from typing import Any, Dict, List

from connectors.db_connector import DatabaseConnector
from connectors.es_connector import ElasticsearchConnector
from core.etl_films import ETLFilms
from core.etl_genres import ETLGenres
from core.etl_persons import ETLPersons
from service.state_storage import JsonFileStorage, State


class ETLCoreService:
    """Основной скрипт, запрашивающий у PostgreSQL данные о фильмах и формирующий посылку для Elasticsearch.
    Основной метод класса это run, исполняющийся постоянно."""

    def __init__(
        self,
        db_connector: DatabaseConnector,
        es_connector: ElasticsearchConnector,
        logger: logging.Logger,
    ) -> None:
        self.films_routine = ETLFilms(db_connector,
                                      es_connector,
                                      State(JsonFileStorage('state_films.json')),
                                      logger)
        self.genres_routine = ETLGenres(db_connector,
                                        es_connector,
                                        State(JsonFileStorage('state_genres.json')),
                                        logger)
        self.persons_routine = ETLPersons(db_connector,
                                          es_connector,
                                          State(JsonFileStorage('state_persons.json')),
                                          logger)
        self.logger = logger
        self.es_connector = es_connector
        self.delay_between_runs = 30

    def wait_while_es_start(self):
        while not self.es_connector.check_elasticsearch():
            self.logger.error(f'Попытка подключения к Elasticsearch')
            time.sleep(1)

    def run(self) -> None:
        self.wait_while_es_start()

        indexes = ['movies', 'genres', 'persons']
        for index in indexes:
            if not self.es_connector.check_index(index):
                self.es_connector.create_mapping(index)

        while True:
            self.logger.info(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

            try:
                self.films_routine.activate()
                self.genres_routine.activate()
                self.persons_routine.activate()

            except Exception as e:
                self.logger.error(f'Произошла ошибка: {e}')

            time.sleep(self.delay_between_runs)

