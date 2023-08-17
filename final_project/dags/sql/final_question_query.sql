SELECT
    e.state,
    COUNT(*) AS tv_count,
    TIMESTAMP_DIFF(s.purchase_date, e.birth_date, YEAR) AS age_at_purchase
FROM `silver.sales` s
JOIN `gold.enrich_user_profiles` e ON s.client_id = e.client_id
WHERE TIMESTAMP_DIFF(s.purchase_date, e.birth_date, YEAR) >= 20 AND TIMESTAMP_DIFF(s.purchase_date, e.birth_date, YEAR) <= 30
    AND s.product_name LIKE 'TV'
    AND EXTRACT(DAY FROM s.purchase_date) <= 10
    AND EXTRACT(MONTH FROM s.purchase_date) = 9
GROUP BY e.state, age_at_purchase
ORDER BY tv_count DESC
LIMIT 10;