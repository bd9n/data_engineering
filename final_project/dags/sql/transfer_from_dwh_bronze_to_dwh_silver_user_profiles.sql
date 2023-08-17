DELETE FROM `{{ params.project_id }}.silver.user_profiles`
WHERE true
;

INSERT `{{ params.project_id }}.silver.user_profiles` (
    email,
    full_name,
    state,
    birth_date,
    phone_number
)
SELECT
    email,
    full_name,
    state,
    CAST(birth_date AS DATE) AS birth_date,
    phone_number,
FROM `{{ params.project_id }}.bronze.user_profiles`
;