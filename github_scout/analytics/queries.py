"""Pre-defined analytical SQL queries for DuckDB."""

__all__: list[str] = [
    "TOP_POTENTIAL_REPOS",
    "TRENDING_7D",
    "LANGUAGE_DISTRIBUTION",
    "TOPIC_HEATMAP",
    "STAR_VELOCITY_PERCENTILES",
    "SCORE_DISTRIBUTION",
]

TOP_POTENTIAL_REPOS: str = """
SELECT
    full_name,
    primary_language,
    stars,
    forks,
    open_issues,
    contributors_count,
    star_velocity,
    momentum_7d,
    readme_quality,
    potential_score,
    created_at,
    url
FROM repositories
WHERE potential_score IS NOT NULL
ORDER BY potential_score DESC
LIMIT 50;
"""

TRENDING_7D: str = """
WITH current_snap AS (
    SELECT repo_id, stars, snapshot_at,
           ROW_NUMBER() OVER (PARTITION BY repo_id ORDER BY snapshot_at DESC) AS rn
    FROM repo_snapshots
),
old_snap AS (
    SELECT repo_id, stars, snapshot_at,
           ROW_NUMBER() OVER (
               PARTITION BY repo_id
               ORDER BY snapshot_at ASC
           ) AS rn
    FROM repo_snapshots
    WHERE snapshot_at >= current_timestamp - INTERVAL '7 days'
)
SELECT
    r.full_name,
    r.stars AS current_stars,
    o.stars AS stars_7d_ago,
    (c.stars - o.stars) AS delta_stars,
    CASE WHEN o.stars > 0
         THEN (c.stars - o.stars)::DOUBLE / o.stars
         ELSE 0.0
    END AS momentum_7d,
    r.potential_score,
    r.url
FROM current_snap c
JOIN old_snap o ON c.repo_id = o.repo_id AND o.rn = 1
JOIN repositories r ON r.id = c.repo_id
WHERE c.rn = 1
ORDER BY delta_stars DESC
LIMIT 50;
"""

LANGUAGE_DISTRIBUTION: str = """
SELECT
    COALESCE(primary_language, 'Unknown') AS language,
    COUNT(*) AS repo_count,
    ROUND(AVG(stars), 1) AS avg_stars,
    ROUND(AVG(potential_score), 2) AS avg_score
FROM repositories
GROUP BY primary_language
ORDER BY repo_count DESC;
"""

TOPIC_HEATMAP: str = """
WITH exploded AS (
    SELECT UNNEST(topics) AS topic, potential_score
    FROM repositories
    WHERE topics IS NOT NULL
)
SELECT
    topic,
    COUNT(*) AS repo_count,
    ROUND(AVG(potential_score), 2) AS avg_score
FROM exploded
GROUP BY topic
ORDER BY repo_count DESC
LIMIT 50;
"""

STAR_VELOCITY_PERCENTILES: str = """
SELECT
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY star_velocity) AS p50,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY star_velocity) AS p90,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY star_velocity) AS p99
FROM repositories
WHERE star_velocity IS NOT NULL;
"""

SCORE_DISTRIBUTION: str = """
SELECT
    CAST(FLOOR(potential_score / 10) AS INTEGER) AS bucket,
    COUNT(*) AS count
FROM repositories
WHERE potential_score IS NOT NULL
GROUP BY bucket
ORDER BY bucket;
"""
