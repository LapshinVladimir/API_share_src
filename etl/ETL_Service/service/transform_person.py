from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class PersonFilm:
    id: str
    roles: list


@dataclass
class Person:
    id: str
    full_name: str
    films: List[PersonFilm]


def prepare_to_transform(person_works: List[tuple[Any, ...]]) -> List[Person]:
    """
    Класс позволяющий трансформировать данные из PostgreSQL,
    получая на выходе данные пригодные для посылки в Elasticsearch
    """
    all_persons_data = [dict(row) for row in person_works]
    persons = []
    for person in all_persons_data:
        persons.append(
            Person(
                person['id'],
                person['full_name'],
                [PersonFilm(p['film_id'], p['roles']) for p in person['films']],
            )
        )

    return persons


def process_to_es_format(person_works: List[tuple[Any, ...]]):
    persons = prepare_to_transform(person_works)
    es_data = []

    for person in persons:
        es_data.append(
            {
                'id': person.id,
                'full_name': person.full_name,
                'films': [{'id': p.id, 'roles': p.roles} for p in person.films],
            }
        )
    return es_data
