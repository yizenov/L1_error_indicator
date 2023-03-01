SELECT COUNT(*) 
FROM company_name AS cn 
JOIN movie_companies AS mc ON (cn.id = mc.company_id AND cn.country_code = '[ge]') 
JOIN title AS t ON (mc.movie_id = t.id) 
JOIN movie_keyword AS mk ON (t.id = mk.movie_id AND mc.movie_id = mk.movie_id) 
JOIN keyword AS k ON (mk.keyword_id = k.id AND k.keyword = 'character-name-in-title');
