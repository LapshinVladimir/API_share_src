from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class Genre:
    id: str
    name: str


def prepare_to_transform(genre_works: List[tuple[Any, ...]]) -> List[Genre]:
    """
    Класс позволяющий трансформировать данные из PostgreSQL,
    получая на выходе данные пригодные для посылки в Elasticsearch
    """
    genres = []
    all_genres_data = [dict(row) for row in genre_works]
    for genre in all_genres_data:
        genres.append(
            Genre(
                genre['id'],
                genre['name']
            )
        )

    return genres


def process_to_es_format(genre_works: List[tuple[Any, ...]]):
    genres = prepare_to_transform(genre_works)
    es_data = []

    for genre in genres:
        es_data.append(
            {
                'id': genre.id,
                'name': genre.name,
            }
        )
    return es_data
