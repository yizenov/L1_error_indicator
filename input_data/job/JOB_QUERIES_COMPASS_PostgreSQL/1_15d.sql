SELECT COUNT(*)
FROM aka_title AS att,
    company_name AS cn,
    company_type AS ct,
    info_type AS it,
    keyword AS k,
    movie_companies AS mc,
    movie_info AS mi,
    movie_keyword AS mk,
    title AS t
WHERE cn.country_code = '[us]'
    AND it.info = 'release dates'
    AND mi.note like '%internet%'
    AND t.production_year > 1990

    AND t.id = att.movie_id
    AND t.id = mi.movie_id
    AND t.id = mk.movie_id
    AND t.id = mc.movie_id
    AND mk.movie_id = mi.movie_id
    AND mk.movie_id = mc.movie_id
    AND mk.movie_id = att.movie_id
    AND mi.movie_id = mc.movie_id
    AND mi.movie_id = att.movie_id
    AND mc.movie_id = att.movie_id
    AND k.id = mk.keyword_id
    AND it.id = mi.info_type_id
    AND cn.id = mc.company_id
    AND ct.id = mc.company_type_id;
