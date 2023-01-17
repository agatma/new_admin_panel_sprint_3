SETTINGS = {
    "settings": {
        "refresh_interval": "1s",
        "analysis": {
            "filter": {
                "english_stop": {"type": "stop", "stopwords": "_english_"},
                "english_stemmer": {"type": "stemmer", "language": "english"},
                "english_possessive_stemmer": {
                    "type": "stemmer",
                    "language": "possessive_english",
                },
                "russian_stop": {"type": "stop", "stopwords": "_russian_"},
                "russian_stemmer": {"type": "stemmer", "language": "russian"},
            },
            "analyzer": {
                "ru_en": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop",
                        "english_stemmer",
                        "english_possessive_stemmer",
                        "russian_stop",
                        "russian_stemmer",
                    ],
                }
            },
        },
    },
}

FILMWORK_INDEX = {
    **SETTINGS,
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "id": {"type": "keyword"},
            "imdb_rating": {"type": "float"},
            "age_limit": {"type": "integer"},
            "genres": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {"type": "text", "analyzer": "ru_en"},
                },
            },
            "title": {
                "type": "text",
                "analyzer": "ru_en",
                "fields": {"raw": {"type": "keyword"}},
            },
            "description": {"type": "text", "analyzer": "ru_en"},
            "directors_names": {"type": "text", "analyzer": "ru_en"},
            "actors_names": {"type": "text", "analyzer": "ru_en"},
            "writers_names": {"type": "text", "analyzer": "ru_en"},
            "actors": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {"type": "text", "analyzer": "ru_en"},
                },
            },
            "directors": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {"type": "text", "analyzer": "ru_en"},
                },
            },
            "writers": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {"type": "text", "analyzer": "ru_en"},
                },
            },
        },
    },
}

GENRE_INDEX = {
    **SETTINGS,
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "id": {"type": "keyword"},
            "name": {"type": "keyword"},
            "description": {
                "type": "text",
                "analyzer": "ru_en",
            },
        },
    },
}

PERSONS_INDEX = {
    **SETTINGS,
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "id": {"type": "keyword"},
            "full_name": {
                "type": "text",
                "analyzer": "ru_en",
                "fields": {"raw": {"type": "keyword"}},
            },
            "films": {
                "type": "nested",
                "dynamic": "strict",
                "properties": {
                    "actor": {
                        "type": "nested",
                        "dynamic": "strict",
                        "properties": {
                            "id": {"type": "keyword"},
                            "imdb_rating": {"type": "float"},
                            "title": {
                                "type": "text",
                                "analyzer": "ru_en",
                                "fields": {"raw": {"type": "keyword"}},
                            },
                        },
                    },
                    "writer": {
                        "type": "nested",
                        "dynamic": "strict",
                        "properties": {
                            "id": {"type": "keyword"},
                            "imdb_rating": {"type": "float"},
                            "title": {
                                "type": "text",
                                "analyzer": "ru_en",
                                "fields": {"raw": {"type": "keyword"}},
                            },
                        },
                    },
                    "director": {
                        "type": "nested",
                        "dynamic": "strict",
                        "properties": {
                            "id": {"type": "keyword"},
                            "imdb_rating": {"type": "float"},
                            "title": {
                                "type": "text",
                                "analyzer": "ru_en",
                                "fields": {"raw": {"type": "keyword"}},
                            },
                        },
                    },
                },
            },
        },
    },
}
