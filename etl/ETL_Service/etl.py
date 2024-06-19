import logging

from connectors.db_connector import DatabaseConnector
from connectors.es_connector import ElasticsearchConnector
from core.etl_core import ETLCoreService
from service.settings import Settings
from service.state_storage import JsonFileStorage, State

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('elastic_transport').setLevel(logging.ERROR)
    logger = logging.getLogger(__name__)

    settings = Settings()

    db_connector = DatabaseConnector(settings)
    es_connector = ElasticsearchConnector(settings.es_host, logger)

    etl_service = ETLCoreService(db_connector, es_connector, logger)
    etl_service.run()
