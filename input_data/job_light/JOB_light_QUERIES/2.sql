SELECT COUNT(*)
FROM title AS t,
	cast_info AS ci
WHERE t.production_year > 1980 and t.production_year < 1984

	AND t.id = ci.movie_id;
