SELECT COUNT(*) 
FROM movie_companies AS mc 
JOIN movie_keyword AS mk ON (mc.movie_id = mk.movie_id) 
JOIN keyword AS k ON (mk.keyword_id = k.id AND k.keyword = 'character-name-in-title') 
JOIN company_name AS cn ON (cn.id = mc.company_id AND cn.country_code = '[ge]') 
JOIN title AS t ON (mc.movie_id = t.id AND t.id = mk.movie_id);
