CREATE TABLE IF NOT EXISTS external_links (
    id SERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    link TEXT NOT NULL,
    domain TEXT,
    path TEXT,
    is_homepage_or_subsection BOOLEAN,
    subsection TEXT,
    country_code TEXT,
    category TEXT,
    is_ad_domain BOOLEAN
);

CREATE MATERIALIZED VIEW IF NOT EXISTS domain_aggregation AS
SELECT
  domain,
  COUNT(*) AS total_links,
  SUM(CASE WHEN is_homepage_or_subsection AND subsection IS NULL THEN 1 ELSE 0 END) AS homepage_count,
  SUM(CASE WHEN is_homepage_or_subsection AND subsection IS NOT NULL THEN 1 ELSE 0 END) AS subsection_count,
  COUNT(DISTINCT CASE WHEN is_homepage_or_subsection AND subsection IS NOT NULL THEN path END) AS distinct_subsections
FROM external_links
GROUP BY domain
ORDER BY total_links DESC;