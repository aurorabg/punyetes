# punyetes hola
https://raw.githubusercontent.com/tnavarrete-iedib/bigdata-24-25/refs/heads/main/raw_titles.csv 
https://raw.githubusercontent.com/tnavarrete-iedib/bigdata-24-25/refs/heads/main/raw_credits.csv 

1
SELECT title, rating, seasons
FROM raw_titles
WHERE seasons > 1
ORDER BY rating DESC
LIMIT 10;

2
SELECT year, SUM(votes) AS total_votes
FROM raw_titles
WHERE type = 'TV Show'
GROUP BY year
ORDER BY total_votes DESC
LIMIT 10;

3
SELECT director, COUNT(*) AS num_movies
FROM raw_credits
WHERE director IS NOT NULL
GROUP BY director
ORDER BY num_movies DESC
LIMIT 10;

4
SELECT actor, AVG(rating) AS avg_rating
FROM raw_credits
JOIN raw_titles ON raw_credits.movie_id = raw_titles.id
WHERE actor IS NOT NULL
GROUP BY actor
ORDER BY avg_rating DESC
LIMIT 10;

apartat 2

1
CREATE TABLE llibres (
  ID INT,
  Info_Llibre STRUCT<Títol: STRING, Autor: STRING, Any: INT>,
  Temes ARRAY<STRING>,
  Exemplars_Biblioteca MAP<STRING, INT>
)
STORED AS PARQUET;

2
INSERT INTO llibres VALUES
(1, NAMED_STRUCT('Títol', '1984', 'Autor', 'George Orwell', 'Any', 1949), ARRAY('Ficció', 'Distopia', 'Societat'), MAP('Centre', 5, 'Llevant', 2)),
(2, NAMED_STRUCT('Títol', 'Sapiens', 'Autor', 'Yuval Noah Harari', 'Any', 2011), ARRAY('Assaig', 'Història', 'Antropologia', 'Societat'), MAP('Llevant', 4, 'Ponent', 3)),
(3, NAMED_STRUCT('Títol', 'Dune', 'Autor', 'Frank Herbert', 'Any', 1965), ARRAY('Ficció', 'Aventura', 'Ciència-ficció'), MAP('Centre', 7, 'Ponent', 2)),
(4, NAMED_STRUCT('Títol', 'El Senyor dels anells', 'Autor', 'J.R.R. Tolkien', 'Any', 1954), ARRAY('Ficció', 'Aventura', 'Fantasia'), MAP('Centre', 8, 'Llevant', 3)),
(5, NAMED_STRUCT('Títol', 'Història de dues ciutats', 'Autor', 'Charles Dickens', 'Any', 1859), ARRAY('Ficció', 'Història', 'Drama'), MAP('Llevant', 2));

3
a
HiveQL:
SELECT Info_Llibre.Títol
FROM llibres
WHERE Info_Llibre.Any BETWEEN 1900 AND 1999;

Impala SQL:
SELECT Info_Llibre.Títol
FROM llibres
WHERE Info_Llibre.Any BETWEEN 1900 AND 1999;

b
HiveQL:
SELECT Info_Llibre.Títol
FROM llibres
WHERE 'Història' IN (SELECT explode(Temes));

Impala SQL:
SELECT Info_Llibre.Títol
FROM llibres
WHERE 'Història' IN (SELECT explode(Temes));


c
HiveQL:
SELECT SUM(Exemplars_Biblioteca['Llevant']) AS Total_Exemplars_Llevant
FROM llibres;

Impala SQL:
SELECT SUM(Exemplars_Biblioteca['Llevant']) AS Total_Exemplars_Llevant
FROM llibres;

d
HiveQL:
SELECT Info_Llibre.Títol
FROM llibres
WHERE 'Ficció' IN (SELECT explode(Temes))
AND 'Centre' IN (SELECT key FROM map_keys(Exemplars_Biblioteca));

Impala SQL:
SELECT Info_Llibre.Títol
FROM llibres
WHERE 'Ficció' IN (SELECT explode(Temes))
AND 'Centre' IN (SELECT key FROM map_keys(Exemplars_Biblioteca));
