SELECT COUNT(*) 
FROM keyword AS k 
JOIN movie_keyword AS mk ON (mk.keyword_id = k.id AND k.keyword = 'character-name-in-title') 
JOIN movie_companies AS mc ON (mc.movie_id = mk.movie_id) 
JOIN company_name AS cn ON (cn.id = mc.company_id AND cn.country_code = '[ge]') 
JOIN title AS t ON (mc.movie_id = t.id AND t.id = mk.movie_id);
