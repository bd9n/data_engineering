DELETE FROM `{{ params.project_id }}.bronze.sales`
WHERE DATE(PurchaseDate) = "{{ ds }}"
;

INSERT `{{ params.project_id }}.bronze.sales` (
    CustomerId,
    PurchaseDate,
    Product,
    Price
)
SELECT
    CustomerId,
    PurchaseDate,
    Product,
    Price
FROM sales_csv
;