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
CREATE TABLE biblioteca (
    id INT,
    info_llibre STRUCT<titol: STRING, autor: STRING, any: INT>,
    temes ARRAY<STRING>,
    exemplars_biblioteca MAP<STRING, INT>
)
STORED AS PARQUET;

2
INSERT INTO biblioteca VALUES
(1, NAMED_STRUCT('titol', '1984', 'autor', 'George Orwell', 'any', 1949), 
    ARRAY('Ficció', 'Distopia', 'Societat'), 
    MAP('Centre', 5, 'Llevant', 2)),
(2, NAMED_STRUCT('titol', 'Sapiens', 'autor', 'Yuval Noah Harari', 'any', 2011), 
    ARRAY('Assaig', 'Història', 'Antropologia', 'Societat'), 
    MAP('Llevant', 4, 'Ponent', 3)),
(3, NAMED_STRUCT('titol', 'Dune', 'autor', 'Frank Herbert', 'any', 1965), 
    ARRAY('Ficció', 'Aventura', 'Ciència-ficció'), 
    MAP('Centre', 7, 'Ponent', 2)),
(4, NAMED_STRUCT('titol', 'El Senyor dels anells', 'autor', 'J.R.R. Tolkien', 'any', 1954), 
    ARRAY('Ficció', 'Aventura', 'Fantasia'), 
    MAP('Centre', 8, 'Llevant', 3)),
(5, NAMED_STRUCT('titol', 'Història de dues ciutats', 'autor', 'Charles Dickens', 'any', 1859), 
    ARRAY('Ficció', 'Història', 'Drama'), 
    MAP('Llevant', 2));

3
a
HiveQL:
SELECT info_llibre.titol 
FROM biblioteca 
WHERE info_llibre.any >= 1901 AND info_llibre.any <= 2000;

Impala SQL:
SELECT info_llibre.titol 
FROM biblioteca 
WHERE info_llibre.any BETWEEN 1901 AND 2000;

b
HiveQL:
SELECT info_llibre.titol 
FROM biblioteca 
WHERE ARRAY_CONTAINS(temes, 'Història');

Impala SQL:
SELECT info_llibre.titol 
FROM biblioteca 
WHERE 'Història' IN temes;

c
HiveQL:
SELECT SUM(exemplars_biblioteca['Llevant']) AS total_exemplars_llevant
FROM biblioteca;

Impala SQL:
SELECT SUM(exemplars_biblioteca['Llevant']) AS total_exemplars_llevant
FROM biblioteca;

d
HiveQL:
SELECT info_llibre.titol 
FROM biblioteca 
WHERE ARRAY_CONTAINS(temes, 'Ficció') AND exemplars_biblioteca['Centre'] > 0;

Impala SQL:
SELECT info_llibre.titol 
FROM biblioteca 
WHERE 'Ficció' IN temes AND exemplars_biblioteca['Centre'] > 0;

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

