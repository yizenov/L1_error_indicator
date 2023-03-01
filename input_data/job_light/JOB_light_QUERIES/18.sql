SELECT COUNT(*)
FROM title AS t,
	movie_keyword AS mk,
	cast_info AS ci
WHERE t.production_year > 2014

	AND t.id = mk.movie_id
	AND t.id = ci.movie_id;
