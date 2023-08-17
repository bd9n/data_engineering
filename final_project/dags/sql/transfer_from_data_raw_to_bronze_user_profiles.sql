DELETE FROM `{{ params.project_id }}.bronze.user_profiles`
WHERE true
;

INSERT `{{ params.project_id }}.bronze.user_profiles` (
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
    birth_date,
    phone_number
FROM user_profiles_jsonl
;