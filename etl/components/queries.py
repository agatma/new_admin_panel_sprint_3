last_modified_films_query = """
SELECT
    fw.id,
    fw.rating AS imdb_rating,
    COALESCE (
        ARRAY_AGG(
            DISTINCT g.name
        ),
        '{}'
    ) AS genre,
    fw.title,
    fw.description,
    COALESCE (
       ARRAY_AGG(
           DISTINCT (p.full_name)
       ) FILTER (WHERE pfw.role = 'director'),
       '{}'
   ) as director,
   COALESCE (
       ARRAY_AGG(
           DISTINCT (p.full_name)
       ) FILTER (WHERE pfw.role = 'actor'),
       '{}'
   ) as actors_names,
     COALESCE (
       ARRAY_AGG(
           DISTINCT (p.full_name)
       ) FILTER (WHERE pfw.role = 'writer'),
       '{}'
   ) as writers_names,
    COALESCE (
       JSON_AGG(
           DISTINCT JSONB_BUILD_OBJECT(
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE pfw.role = 'actor'),
       '[]'
   ) as actors,
    COALESCE (
       JSON_AGG(
           DISTINCT JSONB_BUILD_OBJECT(
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE pfw.role = 'writer'),
       '[]'
   ) as writers
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON gfw.genre_id = g.id
WHERE fw.modified > %s OR p.modified > %s OR g.modified > %s
GROUP BY fw.id
ORDER BY fw.modified ASC;
"""
