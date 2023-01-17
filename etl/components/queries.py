last_modified_films_query = """
SELECT
    fw.id,
    fw.rating AS imdb_rating,
    COALESCE (
        json_agg(
            DISTINCT jsonb_build_object(
                'id', g.id,
                'name', g.name
                )
                ) FILTER (WHERE g.id is not null),
                '[]'
                ) as genres,
    fw.title,
    fw.description,
    COALESCE (
       ARRAY_AGG(
           DISTINCT (p.full_name)
       ) FILTER (WHERE pfw.role = 'director'),
       '{}'
   ) as directors_names,
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
       ) FILTER (WHERE pfw.role = 'director'),
       '[]'
   ) as directors,
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

last_modified_genres_query = """
SELECT g.id, 
	   g.name, 
	   g.description
FROM content.genre as g
WHERE g.modified > %s
ORDER BY g.modified ASC;

;
"""

last_modified_persons_films_query = """
SELECT p.id, p.full_name, COALESCE (
       JSON_AGG(
           DISTINCT JSONB_BUILD_OBJECT(
               'uuid', fw.id,
               'title', fw.title,
			   'role', pf.role,
			   'imdb_rating', fw.rating
           )
       ) FILTER (WHERE fw.id is not null)
	,'[]'
   ) as films
FROM content.person as p
LEFT JOIN content.person_film_work as pf on p.id = pf.person_id
LEFT JOIN content.film_work as fw on pf.film_work_id = fw.id
WHERE p.modified > %s 
GROUP BY p.id
ORDER BY p.modified ASC
;
"""
