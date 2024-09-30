WITH Actor_Counts AS (
    SELECT
        p.person_name,
        COUNT(mc.movie_id) AS movie_count
    FROM 
        movies.movie_cast mc
    JOIN 
        movies.person p ON mc.person_id = p.person_id
    GROUP BY 
        p.person_name
),
Ranked_Actors AS (
    SELECT
        person_name,
        movie_count,
        DENSE_RANK() OVER (ORDER BY movie_count DESC) AS rank
    FROM 
        Actor_Counts
)
SELECT 
    person_name,
    rank
FROM 
    Ranked_Actors
ORDER BY 
    rank;


WITH Director_Avg_Ratings AS (
    SELECT
        p.person_name AS director_name,
        AVG(m.vote_average) AS avg_rating
    FROM 
        movies.movie m
    JOIN 
        movies.movie_crew mc ON m.movie_id = mc.movie_id
    JOIN 
        movies.person p ON mc.person_id = p.person_id
    JOIN 
        movies.department d ON mc.department_id = d.department_id
    WHERE 
        d.department_id = 2  -- Department ID for directing
    GROUP BY 
        p.person_name
),
Ranked_Directors AS (
    SELECT
        director_name,
        avg_rating,
        RANK() OVER (ORDER BY avg_rating DESC) AS rank
    FROM 
        Director_Avg_Ratings
)
SELECT 
    director_name,
    avg_rating
FROM 
    Ranked_Directors
WHERE 
    rank = 1
ORDER BY 
    avg_rating DESC;



WITH Actor_Revenue AS (
    SELECT
        p.person_name AS actor_name,
        SUM(m.revenue) AS total_revenue
    FROM 
        movies.movie m
    JOIN 
        movies.movie_cast mc ON m.movie_id = mc.movie_id
    JOIN 
        movies.person p ON mc.person_id = p.person_id
    GROUP BY 
        p.person_name
)
SELECT 
    actor_name,
    total_revenue
FROM 
    Actor_Revenue
ORDER BY 
    total_revenue DESC;



WITH Director_Budgets AS (
    SELECT
        p.person_name AS director_name,
        SUM(m.budget) AS total_budget
    FROM 
        movies.movie m
    JOIN 
        movies.movie_crew mc ON m.movie_id = mc.movie_id
    JOIN 
        movies.person p ON mc.person_id = p.person_id
    JOIN 
        movies.department d ON mc.department_id = d.department_id
    WHERE 
        d.department_id = 2  -- Department ID for directing
    GROUP BY 
        p.person_name
),
Ranked_Directors AS (
    SELECT
        director_name,
        total_budget,
        RANK() OVER (ORDER BY total_budget DESC) AS rank
    FROM 
        Director_Budgets
)
SELECT 
    director_name,
    total_budget
FROM 
    Ranked_Directors
WHERE 
    rank = 1
ORDER BY 
    total_budget DESC;



