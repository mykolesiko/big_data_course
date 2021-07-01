CREATE TABLE IF NOT EXISTS movies_with_genre_index (
 movieid int,
 genres set<text>,
 year int,
 title text,
 PRIMARY KEY ((title), year, movieid)
 ) WITH CLUSTERING ORDER BY (year DESC);
CREATE INDEX genre_idx ON movies_with_genre_index (genres);
