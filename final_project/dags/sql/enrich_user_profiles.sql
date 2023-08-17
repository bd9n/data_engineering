DELETE FROM `{{ params.project_id }}.gold.enrich_user_profiles`
WHERE true
;

MERGE INTO {{ params.project_id }}.gold.enrich_user_profiles AS target
USING (
    SELECT
        c.client_id,
        SPLIT(up.full_name, ' ')[OFFSET(0)] AS first_name,
        SPLIT(up.full_name, ' ')[OFFSET(1)] AS last_name,
        up.state,
        c.email,
        c.registration_date,
        up.birth_date,
        up.phone_number
    FROM {{ params.project_id }}.silver.customers c
    LEFT JOIN {{ params.project_id }}.silver.user_profiles up ON c.email = up.email
) AS source
ON target.email = source.email
WHEN MATCHED THEN
    UPDATE SET
        target.state = source.state,
        target.birth_date = source.birth_date,
        target.phone_number = source.phone_number
WHEN NOT MATCHED THEN
    INSERT (client_id, first_name, last_name, state, birth_date, phone_number, email, registration_date)
    VALUES (source.client_id, source.first_name, source.last_name, source.state, source.birth_date, source.phone_number, source.email, source.registration_date)
    ;
