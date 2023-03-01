SELECT COUNT(*)
FROM cast_info AS ci,
    keyword AS k,
    movie_keyword AS mk,
    name AS n,
    title AS t
WHERE k.keyword = 'marvel-cinematic-universe'
    AND n.name like '%Downey%Robert%'
    AND t.production_year > 2010

    AND k.id = mk.keyword_id
    AND t.id = mk.movie_id
    AND t.id = ci.movie_id
    AND ci.movie_id = mk.movie_id
    AND n.id = ci.person_id;
