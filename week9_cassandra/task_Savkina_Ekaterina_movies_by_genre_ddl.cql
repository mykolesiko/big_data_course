CREATE TABLE IF NOT EXISTS movies_by_genre (
    movieid int,
    genre text,
    year int,
    title text,
    PRIMARY KEY ((genre),  year, movieid)
) WITH CLUSTERING ORDER BY (year DESC);
