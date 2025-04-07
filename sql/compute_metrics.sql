-- 1. Number of unique domains
SELECT COUNT(DISTINCT split_part(split_part(link, '://', 2), '/', 1)) as unique_domains
FROM external_links;

-- 2. Number of links by category
SELECT category, COUNT(*) 
FROM external_links
GROUP BY category
ORDER BY COUNT(*) DESC;

-- 3. Percentage of homepage vs. subsection
SELECT 
    is_homepage,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percentage
FROM external_links
GROUP BY is_homepage;

-- 4. Top 10 countries by domain count
SELECT country_code, COUNT(*) as cnt
FROM external_links
GROUP BY country_code
ORDER BY cnt DESC
LIMIT 10;

-- 5. Ratio of ad-based domains
SELECT
    (SELECT COUNT(*) FROM external_links WHERE is_ad_domain = TRUE) * 1.0 /
    (SELECT COUNT(*) FROM external_links) AS ad_domain_ratio;
