SELECT 
    g.genre_name,
    m.title AS movie_title,
    RANK() OVER (PARTITION BY g.genre_name ORDER BY m.popularity DESC) AS rank
FROM 
    movies.movie m
JOIN 
    movies.movie_genres mg ON m.movie_id = mg.movie_id
JOIN 
    movies.genre g ON mg.genre_id = g.genre_id
ORDER BY 
    g.genre_name, rank;




WITH Company_Revenue AS (
    SELECT
        pc.company_name,
        m.title AS movie_title,
        m.revenue,
        ROW_NUMBER() OVER (PARTITION BY pc.company_name ORDER BY m.revenue DESC) AS rank
    FROM 
        movies.movie m
    JOIN 
        movies.movie_company mc ON m.movie_id = mc.movie_id
    JOIN 
        movies.production_company pc ON mc.company_id = pc.company_id
)
SELECT 
    company_name,
    movie_title,
    revenue
FROM 
    Company_Revenue
WHERE 
    rank <= 3
ORDER BY 
    company_name, rank;




WITH Genre_Budgets AS (
    SELECT
        g.genre_name,
        m.title AS movie_title,
        m.budget,
        SUM(m.budget) OVER (
            PARTITION BY g.genre_name 
            ORDER BY m.budget DESC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_total_budget
    FROM 
        movies.movie m
    JOIN 
        movies.movie_genres mg ON m.movie_id = mg.movie_id
    JOIN 
        movies.genre g ON mg.genre_id = g.genre_id
)
SELECT 
    genre_name,
    movie_title,
    budget,
    running_total_budget
FROM 
    Genre_Budgets
ORDER BY 
    genre_name, running_total_budget DESC;



WITH Recent_Movies AS (
    SELECT
        g.genre_name,
        m.title AS movie_title,
        m.release_date,
        FIRST_VALUE(m.title) OVER (
            PARTITION BY g.genre_name 
            ORDER BY m.release_date DESC
        ) AS most_recent_movie
    FROM 
        movies.movie m
    JOIN 
        movies.movie_genres mg ON m.movie_id = mg.movie_id
    JOIN 
        movies.genre g ON mg.genre_id = g.genre_id
)
SELECT DISTINCT
    genre_name,
    most_recent_movie AS movie_title,
    MAX(release_date) AS release_date
FROM 
    Recent_Movies
GROUP BY 
    genre_name, most_recent_movie
ORDER BY 
    genre_name;







