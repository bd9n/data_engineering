DELETE FROM `{{ params.project_id }}.silver.sales`
WHERE DATE(purchase_date) = "{{ ds }}"
;

INSERT `{{ params.project_id }}.silver.sales` (
    client_id,
    purchase_date,
    product_name,
    price
)
SELECT
    CAST(CustomerId as INTEGER),
    CAST(PurchaseDate AS DATE) AS purchase_date,
    Product,
    CAST(REGEXP_REPLACE(Price, r'(\$|£|USD)', '') AS INTEGER) AS price,
FROM `{{ params.project_id }}.bronze.sales`
WHERE DATE(PurchaseDate) = "{{ ds }}"
;