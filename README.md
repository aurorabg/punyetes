# punyetes hola
https://raw.githubusercontent.com/tnavarrete-iedib/bigdata-24-25/refs/heads/main/raw_titles.csv 
https://raw.githubusercontent.com/tnavarrete-iedib/bigdata-24-25/refs/heads/main/raw_credits.csv 

CREATE DATABASE IF NOT EXISTS netflix;

USE netflix;

CREATE TABLE titles(
 index INT,
 id STRING,
 title STRING,
 type STRING,
 release_year INT,
 age_certification STRING,
 runtime INT,
 genres STRING,
 production_countries STRING,
 seasons FLOAT,
 imdb_id STRING,
 imdb_score FLOAT,
 imdb_votes FLOAT)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE credits(
 index INT,
 person_id INT,
 id STRING,
 name STRING,
 character STRING,
 role STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="1");

hdfs dfs -put /home/cloudera/netflix/raw_titles.csv /user/hive/warehouse/netflix.db/titles/
hdfs dfs -put /home/cloudera/netflix/raw_credits.csv /user/hive/warehouse/netflix.db/credits/

LOAD DATA INPATH '/user/hive/warehouse/netflix.db/raw_titles/raw_titles.csv' INTO TABLE titles;
LOAD DATA INPATH '/user/hive/warehouse/netflix.db/raw_credits/raw_credits.csv' INTO TABLE credits;

1
SELECT title, seasons, imdb_score
FROM titles
WHERE type = 'SHOW' 
AND seasons > 1
AND imdb_score IS NOT NULL
ORDER BY imdb_score DESC
LIMIT 10;

2
SELECT release_year, SUM(imdb_votes) AS total_votes
FROM titles
WHERE type = 'SHOW'
AND imdb_votes IS NOT NULL
GROUP BY release_year
ORDER BY total_votes DESC
LIMIT 10;

3
SELECT name, COUNT(*) AS num_movies
FROM credits
JOIN titles ON credits.id = titles.id
WHERE role = 'DIRECTOR' 
AND type = 'MOVIE'
GROUP BY name
ORDER BY num_movies DESC
LIMIT 10;

4
SELECT name, AVG(imdb_score) AS avg_rating
FROM credits
JOIN titles ON credits.id = titles.id
WHERE role = 'ACTOR' 
AND type = 'MOVIE'
AND imdb_score IS NOT NULL
GROUP BY name
HAVING COUNT(*) > 1
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

Apartat 3
hdfs dfs -put centres_educatius.json /user/hive/warehouse/centres_educatius/

CREATE EXTERNAL TABLE centres_educatius (
  codiCentre STRING,
  nomCentre STRING,
  tipusCentreNomCa STRING,
  nomMunicipi STRING,
  illa STRING,
  esPublic BOOLEAN,
  nomEtapa ARRAY<STRING>  -- Tipus complex ARRAY per les etapes educatives
)
STORED AS JSON
LOCATION '/user/hive/warehouse/centres_educatius';

a
SELECT COUNT(*) AS nombre_centres_publics_eivissa
FROM centres_educatius
WHERE esPublic = true AND illa = 'Eivissa';

b
SELECT nomCentre
FROM centres_educatius
WHERE nomMunicipi = 'Palma' AND nomEtapa CONTAINS 'Educació secundària';

c
SELECT tipusCentreNomCa, COUNT(*) AS nombre_centres
FROM centres_educatius
WHERE illa = 'Menorca'
GROUP BY tipusCentreNomCa;

d
SELECT nomCentre
FROM centres_educatius
WHERE illa = 'Mallorca' AND 'Grau superior' IN (SELECT explode(nomEtapa));

