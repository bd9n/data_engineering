DELETE FROM `{{ params.project_id }}.silver.customers`
WHERE DATE(registration_date) = "{{ ds }}"
;

INSERT `{{ params.project_id }}.silver.customers` (
    client_id,
    first_name,
    last_name,
    email,
    registration_date,
    state
)
SELECT
    CAST(Id as INTEGER) AS client_id,
    FirstName,
    LastName,
    Email,
    CAST(RegistrationDate AS DATE) AS purchase_date,
    State,
FROM `{{ params.project_id }}.bronze.customers`
WHERE DATE(RegistrationDate) = "{{ ds }}"
;