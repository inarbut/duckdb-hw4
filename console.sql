

CREATE TABLE steam_reviews AS
SELECT * FROM read_json_auto(
    'C:\Users\illya\Desktop\programming\duckdb\hw4\steam_reviews.json',
    maximum_object_size = 67108864
);



CREATE TABLE steam_reviews_unnested_reviews AS
WITH expanded_reviews AS (
    SELECT
        game.appid,
        UNNEST(game.review_data.reviews) AS user_review
    FROM (SELECT UNNEST(reviews) AS game FROM steam_reviews)
)
SELECT
    appid,
    user_review.*
FROM expanded_reviews;

DROP TABLE steam_reviews_unnested_game_info;

CREATE TABLE steam_reviews_unnested_game_info AS
WITH expanded_info AS (
    SELECT
        game.appid,
        UNNEST(game.review_data.query_summary) AS game_info
    FROM (SELECT UNNEST(reviews) AS game FROM steam_reviews)
)
SELECT
    *
FROM expanded_info;



CREATE TABLE steam_games AS
SELECT * FROM read_json_auto(
    'C:\Users\illya\Desktop\programming\duckdb\hw4\steam_games.json',
    maximum_object_size = 106144477
);





DROP TABLE steam_games_unnested;
CREATE TABLE steam_games_unnested AS
WITH unnested_games AS (
    SELECT
        game.app_details.data AS game_det
    FROM (SELECT UNNEST(games) AS game FROM steam_games)
),
unnested_games1 AS (
    SELECT
    UNNEST(game_det)
FROM unnested_games
)
SELECT
    * EXCLUDE (price_overview, genres, release_date, achievements, required_age, developers),
    CAST(required_age AS INT64) as required_age,
    price_overview.initial/100 AS price_initial,
    price_overview.final/100 AS price_final,
    price_overview.currency,
    price_overview.discount_percent,
    price_overview.final_formatted as price_final_formatted,
    [x.description FOR x IN genres] as genres,
    release_date.coming_soon as is_coming_soon,
    release_date.date as release_date,
    IFNULL(achievements.total, 0) as total_achievments,
    developers[1] as main_dev_studio
FROM unnested_games1;


SELECT *
FROM steam_games_unnested
LIMIT 100;





WITH genre_unnested AS (
    SELECT
    currency,
    price_final,
    type,
    UNNEST(genres) as genre
    FROM steam_games_unnested
)
SELECT genre,
AVG(price_final) as avg_price
    FROM genre_unnested
WHERE currency NOT IN ('CAD', 'PHP', 'RUB', 'KRW')
GROUP BY genre
ORDER BY avg_price DESC; --most expensive genres

SELECT DISTINCT name, price_final, total_achievments FROM steam_games_unnested WHERE price_final IS NOT NULL AND total_achievments IS NOT NULL; -- just a simple table

WITH genre_unnested AS (
    SELECT
    required_age,
    UNNEST(genres) as genre
    FROM steam_games_unnested
)
SELECT genre,
MAX(required_age) as max_age
    FROM genre_unnested
GROUP BY genre
ORDER BY max_age DESC; -- genres that have the highest required age


SELECT
    type,
    SUM(is_free)/COUNT(*)*100 as percentage_of_free_stuff
FROM steam_games_unnested
GROUP BY type
HAVING type IS NOT NULL
ORDER BY percentage_of_free_stuff DESC; -- the percent of free programs for each type

SELECT
type,
COUNT(website)/COUNT(*)*100 as percent_of_ppl_website
FROM steam_games_unnested
WHERE type = 'game'
GROUP BY type; -- percentage of games that have a website


SELECT
type,
COUNT(controller_support)/COUNT(*)*100 as percent_of_contrl_support
FROM steam_games_unnested
WHERE type = 'game'
GROUP BY type; -- percentage of games that supports controllers

SELECT s_g.name, s_r.total_positive, s_r.total_negative,  total_positive/total_negative as pos_to_neg_ratio, IFNULL(s_g.price_final, 0) as price
FROM steam_reviews_unnested_game_info s_r
LEFT JOIN steam_games_unnested s_g on s_r.appid = s_g.steam_appid
ORDER BY s_r.total_reviews DESC
LIMIT 10; -- top 10 games by reviews

DROP TABLE most_exp_dev_studio;
CREATE TABLE most_exp_dev_studio AS
SELECT
    main_dev_studio,
    AVG(price_final) as avg_price
FROM steam_games_unnested
WHERE currency NOT IN ('CAD', 'PHP', 'RUB', 'KRW')
GROUP BY main_dev_studio
HAVING COUNT(*) > 2
ORDER BY avg_price DESC; -- dev studio that publish the most expensive games

