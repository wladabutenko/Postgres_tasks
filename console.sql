/*task 1*/

SELECT COUNT(film.title) AS film_counts,
       c.name AS film_category
FROM film
    INNER JOIN film_category ON film.film_id = film_category.film_id
    INNER JOIN category c on film_category.category_id = c.category_id
GROUP BY c.name
ORDER BY film_counts DESC;

/*task 2*/
SELECT concat(a.first_name,' ',a.last_name) AS full_name,
       COUNT(r.rental_id) AS rent_amount
FROM actor a
    INNER JOIN film_actor fa on a.actor_id = fa.actor_id
    INNER JOIN film f on f.film_id = fa.film_id
    INNER JOIN inventory i on f.film_id = i.film_id
    INNER JOIN rental r on i.inventory_id = r.inventory_id
GROUP BY full_name
ORDER BY rent_amount DESC
LIMIT 10;

/*task 3*/
SELECT DISTINCT category_name,
       SUM(rental_spent) OVER (PARTITION BY category_name) as exspenses
FROM
(
SELECT c.name as category_name,
        f.rental_rate * f.rental_duration as rental_spent
 FROM category c
          INNER JOIN film_category fc on c.category_id = fc.category_id
          INNER JOIN film f on f.film_id = fc.film_id) AS X
ORDER BY exspenses DESC
LIMIT 1


/*TASK 4 option 1*/
SELECT film.film_id
FROM film
EXCEPT
SELECT inventory.film_id
FROM inventory;

/*task 4 option 2*/
SELECT title,
       film_id
FROM film
AS film_not_in_inventory
WHERE NOT EXISTS (SELECT
inventory_id FROM inventory
    WHERE inventory.film_id = film_not_in_inventory.film_id)

/*task 5*/

SELECT full_name, rnk
FROM (SELECT full_name,
             dense_rank() OVER (ORDER BY film_counts DESC) as rnk
      FROM (SELECT concat(a.first_name, ' ', a.last_name) AS full_name,
                   COUNT(f.title)                         AS film_counts
            FROM actor a
                     INNER JOIN film_actor fa on a.actor_id = fa.actor_id
                     INNER JOIN film f on f.film_id = fa.film_id
                     INNER JOIN film_category fc on f.film_id = fc.film_id
                     INNER JOIN category c on c.category_id = fc.category_id
            WHERE c.name = 'Children'
            GROUP BY full_name
            ORDER BY film_counts DESC) X
) AS ranked_actors
WHERE rnk <= 3

/*task 6*/

SELECT c.city as city_name,
    SUM(CASE
        WHEN c2.active = 0 THEN 1
        ELSE 0
        END) as not_active_clients,
    SUM(CASE
        WHEN c2.active = 1 then 1
        ELSE 0
        END) as active_clients
FROM city c
    INNER JOIN address a on c.city_id = a.city_id
    INNER JOIN customer c2 on a.address_id = c2.address_id
GROUP BY c.city
ORDER BY not_active_clients DESC;

/*TASK 7 */
/* вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city),
   и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.
 */
SELECT category_name, city_name, sum_rent, rnk
FROM
(SELECT category_name, city_name, sum_rent,
       dense_rank() OVER (PARTITION BY category_name ORDER BY sum_rent DESC) as rnk
FROM
(
SELECT c3.name as category_name,
        c2.city as city_name,
        SUM(p.amount) OVER (PARTITION BY p.customer_id ORDER BY p.amount DESC) as sum_rent
 FROM city c2
          INNER JOIN address a on c2.city_id = a.city_id
          INNER JOIN customer c on a.address_id = c.address_id
          INNER JOIN payment p on c.customer_id = p.customer_id
          INNER JOIN inventory i on c.store_id = i.store_id
          INNER JOIN film f on f.film_id = i.film_id
          INNER JOIN film_category fc on f.film_id = fc.film_id
          INNER JOIN category c3 on c3.category_id = fc.category_id
 WHERE c2.city LIKE 'a%'
    or c2.city LIKE '%-%'
 GROUP BY c3.name, c2.city, p.amount, p.customer_id
 ) as x
) as ranked_category
WHERE rnk = 1

/*task 8 - with cte*/
WITH cte_film AS (
    SELECT film_id,
           title,
           (CASE
               WHEN length < 60 THEN 'Short'
               WHEN length < 100 THEN 'Medium'
               ELSE 'Long'
            END) length
FROM film
)
SELECT film_id,
        title,
        length
FROM cte_film
WHERE length = 'Medium'
ORDER BY film_id;



