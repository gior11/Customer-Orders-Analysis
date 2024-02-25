WITH favorite AS (
SELECT ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY COUNT(stores.name) DESC, MAX(orders.creation_time) DESC),
stores.name, COUNT(stores.name), orders.user_id, MAX(orders.creation_time) FROM orders
LEFT JOIN stores ON orders.store_id = stores.id
GROUP BY orders.user_id, stores.name  
  ),

delivered AS (
SELECT user_id, COUNT(user_id) AS delivered_orders FROM orders WHERE final_status = 'DeliveredStatus' GROUP BY 1
  ),
 
cte AS (
SELECT
ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY creation_time DESC),
user_id,
creation_time
FROM orders ),

cte1 AS (
SELECT
user_id,
creation_time AS last_time
FROM cte
WHERE cte.row_number = 1
),

cte2 AS (
SELECT
user_id,
creation_time AS second_time
FROM cte
WHERE cte.row_number = 2)

SELECT
orders.user_id AS user_id,
date_part('day', current_date - users.signed_up_time) AS days_signed,
COUNT(orders.user_id) AS total_orders,
round(AVG(orders.total_price)::numeric, 2) AS average_order_value,
favorite.name AS favorite_store,
delivered.delivered_orders *100/count(orders.user_id) AS "%_delivered",
MAX(orders.creation_time) AS last_order_time,
extract (day FROM cte1.last_time - cte2.second_time) AS days_last_secondlast
FROM orders
INNER JOIN users ON orders.user_id = users.id
LEFT JOIN stores ON orders.store_id = stores.id
INNER JOIN favorite ON orders.user_id = favorite.user_id and favorite.row_number = 1
INNER JOIN delivered ON orders.user_id = delivered.user_id
INNER JOIN cte1 ON orders.user_id = cte1.user_id
INNER JOIN cte2 ON orders.user_id = cte2.user_id
GROUP BY orders.user_id, favorite.name, cte1.last_time, cte2.second_time, users.signed_up_time, delivered_orders
HAVING COUNT(orders.user_id) >= 5
ORDER BY 3 DESC;