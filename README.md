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

sudo service hive-server2 status

sudo service hive-server2 start

127.0.0.1 quickstart.cloudera

netstat -tuln | grep 10000


1
CREATE TABLE biblioteca (
    id INT,
    info_llibre STRUCT<titol: STRING, autor: STRING, any: INT>,
    temes ARRAY<STRING>,
    exemplars_biblioteca MAP<STRING, INT>
)
STORED AS PARQUET;

2
INSERT INTO biblioteca 
SELECT 
    1 AS id,
    NAMED_STRUCT('titol', '1984', 'autor', 'George Orwell', 'any', 1949) AS info_llibre,
    ARRAY('Ficció', 'Distopia', 'Societat') AS temes,
    MAP('Centre', 5, 'Llevant', 2) AS exemplars_biblioteca;

INSERT INTO biblioteca 
SELECT 
    2 AS id,
    NAMED_STRUCT('titol', 'Sapiens', 'autor', 'Yuval Noah Harari', 'any', 2011) AS info_llibre,
    ARRAY('Assaig', 'Història', 'Antropologia', 'Societat') AS temes,
    MAP('Llevant', 4, 'Ponent', 3) AS exemplars_biblioteca;

INSERT INTO biblioteca 
SELECT 
    3 AS id,
    NAMED_STRUCT('titol', 'Dune', 'autor', 'Frank Herbert', 'any', 1965) AS info_llibre,
    ARRAY('Ficció', 'Aventura', 'Ciència-ficció') AS temes,
    MAP('Centre', 7, 'Ponent', 2) AS exemplars_biblioteca;

INSERT INTO biblioteca 
SELECT 
    4 AS id,
    NAMED_STRUCT('titol', 'El Senyor dels anells', 'autor', 'J.R.R. Tolkien', 'any', 1954) AS info_llibre,
    ARRAY('Ficció', 'Aventura', 'Fantasia') AS temes,
    MAP('Centre', 8, 'Llevant', 3) AS exemplars_biblioteca;

INSERT INTO biblioteca 
SELECT 
    5 AS id,
    NAMED_STRUCT('titol', 'Història de dues ciutats', 'autor', 'Charles Dickens', 'any', 1859) AS info_llibre,
    ARRAY('Ficció', 'Història', 'Drama') AS temes,
    MAP('Llevant', 2) AS exemplars_biblioteca;
    
INVALIDATE METADATA netflix.biblioteca;

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
WHERE ARRAY_CONTAINS(temes, 'Història');

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

https://raw.githubusercontent.com/tnavarrete-iedib/bigdata-24-25/refs/heads/main/centres_educatius.json 

hdfs dfs -mkdir -p /user/cloudera/impala/centres
hdfs dfs -put centres_educatius.json /user/cloudera/impala/centres/


hdfs dfs -put centres_educatius.json /user/hive/warehouse/centres_educatius/

CREATE EXTERNAL TABLE centres_educatius (
  codiCentre STRING,
  nomCentre STRING,
  tipusCentreNomCa STRING,
  nomMunicipi STRING,
  illa STRING,
  esPublic BOOLEAN,
  nomEtapa ARRAY<STRING>
)
LOCATION '/user/hive/warehouse/centres_educatius';

CREATE TABLE centres_educatius (
    adreca STRING,
    cif STRING,
    codiIlla STRING,
    codiMunicipi STRING,
    codiOficial STRING,
    cp STRING,
    esPublic BOOLEAN,
    latitud DOUBLE,
    longitud DOUBLE,
    mail STRING,
    nom STRING,
    nomEtapa ARRAY<STRING>,
    nomIlla STRING,
    nomMunicipi STRING,
    telf1 STRING,
    tipusCentreNomCa STRING
)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE centres_json (
    data STRUCT<
        adreca: STRING,
        cif: STRING,
        codiIlla: STRING,
        codiMunicipi: STRING,
        codiOficial: STRING,
        cp: STRING,
        esPublic: BOOLEAN,
        latitud: DOUBLE,
        longitud: DOUBLE,
        mail: STRING,
        nom: STRING,
        nomEtapa: STRING,
        nomIlla: STRING,
        nomMunicipi: STRING,
        telf1: STRING,
        tipusCentreNomCa: STRING
    >
)
STORED AS TEXTFILE
LOCATION '/ruta/al/json/'
TBLPROPERTIES (
    'serialization.format' = '1',
    'field.delim' = ',',
    'ignore.malformed.json' = 'true',
    'json.input.format' = 'org.apache.hadoop.hive.serde2.JsonSerDe'
);

INSERT INTO centres_educatius
SELECT
    data.adreca,
    data.cif,
    data.codiIlla,
    data.codiMunicipi,
    data.codiOficial,
    data.cp,
    data.esPublic,
    data.latitud,
    data.longitud,
    data.mail,
    data.nom,
    split(data.nomEtapa, ', ') AS nomEtapa,
    data.nomIlla,
    data.nomMunicipi,
    data.telf1,
    data.tipusCentreNomCa
FROM centres_json;

INVALIDATE METADATA centres_educatius;

SELECT nom, nomEtapa FROM centres_educatius WHERE esPublic = TRUE;


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

