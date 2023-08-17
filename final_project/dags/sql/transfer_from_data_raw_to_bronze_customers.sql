DELETE FROM `{{ params.project_id }}.bronze.customers`
WHERE DATE(RegistrationDate) = "{{ ds }}"
;

INSERT `{{ params.project_id }}.bronze.customers` (
    Id,
    FirstName,
    LastName,
    Email,
    RegistrationDate,
    State
)
SELECT
    Id,
    FirstName,
    LastName,
    Email,
    RegistrationDate,
    State
FROM customers_csv
;