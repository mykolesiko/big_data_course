CREATE TABLE IF NOT EXISTS movies_by_genre_rating (
    movieid int,
    genre text,
    year int,
    rating float,
    title text,
    PRIMARY KEY ((genre),  year, rating, movieid)
) WITH CLUSTERING ORDER BY (year DESC , rating DESC);
