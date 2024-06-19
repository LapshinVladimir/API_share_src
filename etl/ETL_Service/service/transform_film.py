from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class Person:
    id: str
    name: str


@dataclass
class Movie:
    id: str
    imdb_rating: float
    genres: list
    title: str
    description: str
    directors_names: list
    actors_names: list
    writers_names: list
    directors: List[Person]
    actors: List[Person]
    writers: List[Person]


def prepare_to_transform(film_works: List[tuple[Any, ...]]) -> List[Movie]:
    """
    Класс позволяющий трансформировать данные из PostgreSQL,
    получая на выходе данные пригодные для посылки в Elasticsearch
    """
    unique_films_id = set(film[0] for film in film_works)
    all_films_data = [dict(row) for row in film_works]
    movies = []
    for unique_id in unique_films_id:
        film_work = []
        actors = []
        directors = []
        writers = []
        for film_row in all_films_data:
            if film_row['fw_id'] == unique_id:
                film_work.append(film_row)
                if film_row['role'] == 'actor':
                    actors.append(
                        (film_row['person_id'], film_row['full_name'])
                    )
                elif film_row['role'] == 'director':
                    directors.append(
                        (film_row['person_id'], film_row['full_name'])
                    )
                elif film_row['role'] == 'writer':
                    writers.append(
                        (film_row['person_id'], film_row['full_name'])
                    )

        movies.append(
            Movie(
                film_work[0]['fw_id'],
                float(
                    '0.0'
                    if film_work[0]['rating'] is None
                    else film_work[0]['rating']
                ),
                film_work[0]['genres'].split(', '),
                film_work[0]['title'],
                film_work[0]['description'],
                [row[1] for row in directors],
                [row[1] for row in actors],
                [row[1] for row in writers],
                [Person(d[0], d[1]) for d in directors],
                [Person(d[0], d[1]) for d in actors],
                [Person(d[0], d[1]) for d in writers],
            )
        )

    return movies


def process_to_es_format(film_works: List[tuple[Any, ...]]):
    movies = prepare_to_transform(film_works)
    es_data = []

    for movie in movies:
        es_data.append(
            {
                'id': movie.id,
                'imdb_rating': movie.imdb_rating,
                'genres': movie.genres,
                'title': movie.title,
                'description': movie.description,
                'directors_names': movie.directors_names,
                'actors_names': movie.actors_names,
                'writers_names': movie.writers_names,
                'directors': [
                    {'id': p.id, 'name': p.name} for p in movie.directors
                ],
                'actors': [{'id': p.id, 'name': p.name} for p in movie.actors],
                'writers': [
                    {'id': p.id, 'name': p.name} for p in movie.writers
                ],
            }
        )
    return es_data
