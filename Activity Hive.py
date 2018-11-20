-- ratings y movie_names son archivos que se cargaron desde Ambari (u.data y u.item)

CREATE VIEW IF NOT EXISTS topMovieIDs AS
SELECT movie_id, count(movie_id) as ratingCount
FROM ratings
GROUP BY movie_id
ORDER BY ratingCount DESC;

SELECT n.name, ratingCount
FROM topMovieIDs t JOIN movie_names n
ON n.movie_id = t.movie_id;

DROP VIEW topMovieIDs;




--------------------------- FIND THE MOVIE WITH THE HIGHEST AVERAGE RATING ---------------------
------------------------------------------------------------------------------------------------


CREATE VIEW IF NOT EXISTS highestAvgMovies AS
SELECT movie_id, avg(rating) as avgRating, count(movie_id) as ratingCount
FROM ratings
GROUP BY movie_id
ORDER BY ratingCount DESC;


SELECT n.name, avgRating, ratingCount
FROM highestAvgMovies t JOIN movie_names n
ON n.movie_id = t.movie_id
WHERE ratingCount > 10
ORDER BY avgRating DESC;


DROP VIEW highestAvgMovies;



