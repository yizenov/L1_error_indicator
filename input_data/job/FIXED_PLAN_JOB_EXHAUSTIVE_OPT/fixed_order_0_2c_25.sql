SELECT COUNT(*) 
FROM movie_keyword AS mk 
JOIN keyword AS k ON (mk.keyword_id = k.id AND k.keyword = 'character-name-in-title') 
JOIN title AS t ON (t.id = mk.movie_id) 
JOIN movie_companies AS mc ON (mc.movie_id = t.id AND mc.movie_id = mk.movie_id) 
JOIN company_name AS cn ON (cn.id = mc.company_id AND cn.country_code = '[ge]');
