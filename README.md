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
