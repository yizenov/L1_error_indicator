SELECT COUNT(*)
FROM company_type AS ct,
    info_type AS it,
    movie_companies AS mc,
    movie_info_idx AS mi_idx,
    title AS t
WHERE ct.kind = 'production companies'
    AND it.info = 'top 250 rank'
    AND mc.note not like '%(as Metro-Goldwyn-Mayer Pictures)%' and (mc.note like '%(co-production)%')
    AND t.production_year > 2010

    AND ct.id = mc.company_type_id
    AND t.id = mc.movie_id
    AND t.id = mi_idx.movie_id
    AND mc.movie_id = mi_idx.movie_id
    AND it.id = mi_idx.info_type_id;