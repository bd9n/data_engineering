/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
SELECT
    c.name AS category_name,
    COUNT(*) AS film_count
FROM
    film_category fc
JOIN
    category c ON fc.category_id = c.category_id
GROUP BY
    c.name
ORDER BY film_count DESC




/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
SELECT
    a.actor_id,
    a.first_name,
    a.last_name,
    COUNT(*) AS rental_count
FROM
    actor a
JOIN
    film_actor fa ON a.actor_id = fa.actor_id
JOIN
    film f ON fa.film_id = f.film_id
JOIN
    inventory i ON f.film_id = i.film_id
JOIN
    rental r ON i.inventory_id = r.inventory_id
GROUP BY
    a.actor_id,
    a.first_name,
    a.last_name
ORDER BY
    rental_count DESC
LIMIT 10;



/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
WITH category_revenue AS (
    SELECT
        c.category_id,
        c.name AS category_name,
        SUM(p.amount) AS total_revenue
    FROM
        category c
    JOIN
        film_category fc ON c.category_id = fc.category_id
    JOIN
        film f ON fc.film_id = f.film_id
    JOIN
        inventory i ON f.film_id = i.film_id
    JOIN
        rental r ON i.inventory_id = r.inventory_id
    JOIN
        payment p ON r.rental_id = p.rental_id
    GROUP BY
        c.category_id,
        c.name
)
SELECT category_id, category_name, total_revenue
FROM category_revenue
WHERE total_revenue = (
    SELECT MAX(total_revenue) FROM category_revenue
)



/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
SELECT f.title
FROM film f
LEFT JOIN inventory i ON f.film_id = i.film_id
WHERE i.film_id IS NULL;


/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
SELECT actor.actor_id, actor.first_name, actor.last_name, COUNT(*) AS film_count
FROM actor
         INNER JOIN film_actor ON actor.actor_id = film_actor.actor_id
         INNER JOIN film_category ON film_actor.film_id = film_category.film_id
         INNER JOIN category ON film_category.category_id = category.category_id
WHERE category.name = 'Children'
GROUP BY actor.actor_id, actor.first_name, actor.last_name
ORDER BY film_count DESC
LIMIT 3;
