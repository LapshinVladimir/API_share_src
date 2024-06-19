from typing import Any, List, Tuple

import backoff
import psycopg2
from psycopg2.extras import DictCursor

from service.settings import Settings


class DatabaseConnector:
    """
    Класс отвечающий за соединение с базой данных PostgreSQL
    и чтение записей из базы данных
    """

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError, max_tries=5)
    def __init__(self, settings: Settings) -> None:
        self.conn = psycopg2.connect(
            dbname=settings.dbname,
            user=settings.user,
            password=settings.password,
            host=settings.host,
            port=settings.port,
            cursor_factory=DictCursor,
        )

    def fetch_data(
        self, query: str, params: Tuple = None
    ) -> List[tuple[Any, ...]]:
        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def get_changed_collection(
        self, target: str, last_updated_at: str
    ) -> List[tuple[Any, ...]]:
        query = f"""
            SELECT id, updated_at
            FROM content.{target}
            WHERE updated_at > %s
            ORDER BY updated_at
            LIMIT 100;
        """
        result = self.fetch_data(query, (last_updated_at,))
        return result

    def get_films_by_collection_id(
        self, target: str, collection: List[tuple[Any, ...]]
    ) -> List[tuple[Any, ...]]:
        query = f"""
            SELECT fw.id, fw.updated_at
            FROM content.film_work fw
            LEFT JOIN content.{target}_film_work pfw ON pfw.film_work_id = fw.id
            WHERE pfw.{target}_id IN {tuple(map(lambda x: x[0], collection))}
            ORDER BY fw.updated_at
        """
        result = self.fetch_data(query)
        return result

    def get_genres_by_id(
            self, collection: List[tuple[Any, ...]]
    ) -> List[tuple[Any, ...]]:
        query = f"""
            SELECT 
                g.id AS id,
                g.name AS name,
                g.updated_at AS updated_at,
                json_agg(fw.id) AS films
            FROM 
                content.genre AS g
            LEFT JOIN 
                content.genre_film_work AS gfw ON g.id = gfw.genre_id
            LEFT JOIN 
                content.film_work AS fw ON gfw.film_work_id = fw.id
            WHERE 
                 gfw.genre_id IN {tuple(map(lambda x: x[0], collection))}
            GROUP BY 
                g.id, g.name, g.updated_at
            ORDER BY 
                g.name"""
        result = self.fetch_data(query)
        return result

    def get_persons_by_id(self, collection: List[tuple[Any, ...]]):
        query = f"""
            WITH film_roles AS (
                SELECT 
                    pfw.person_id,
                    pfw.film_work_id,
                    fw.title,
                    array_agg(pfw.role) AS roles
                FROM 
                    content.person_film_work AS pfw
                JOIN 
                    content.film_work AS fw ON pfw.film_work_id = fw.id
                WHERE 
                    pfw.person_id IN {tuple(map(lambda x: x[0], collection))}
                GROUP BY 
                    pfw.person_id, pfw.film_work_id, fw.title
            )
            SELECT 
                p.id AS id,
                p.full_name AS full_name,
                json_agg(
                    json_build_object(
                        'film_id', fr.film_work_id,
                        'film_title', fr.title,
                        'roles', fr.roles
                    )
                ) AS films
            FROM 
                content.person AS p
            JOIN 
                film_roles AS fr ON p.id = fr.person_id
            WHERE 
                p.id IN {tuple(map(lambda x: x[0], collection))}
            GROUP BY 
                p.id, p.full_name
            ORDER BY 
                p.full_name"""
        result = self.fetch_data(query)
        return result

    def get_films_by_id(self, films) -> List[tuple[Any, ...]]:
        query = f"""
            WITH film_genres AS (
                SELECT
                gfw.film_work_id,
                STRING_AGG(g.name, ', ') AS genres
                FROM content.genre_film_work gfw
                JOIN content.genre g ON g.id = gfw.genre_id
                GROUP BY gfw.film_work_id
            )
            SELECT
            fw.id as fw_id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            fw.creation_date ,
            fw.updated_at ,
            pfw.role,
            p.id as person_id,
            p.full_name,
            fg.genres
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN film_genres fg ON fg.film_work_id = fw.id
            WHERE fw.id IN {tuple(map(lambda x: x[0], films))}
        """
        result = self.fetch_data(query)
        return result
