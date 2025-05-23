
--🧠 Dataset: bigquery-public-data.stackoverflow
-- Para cada año, ¿cuál fue el usuario con mayor número de respuestas publicadas?

with total_posts as (
SELECT owner_user_id, extract(year from creation_date) as year_post, count(*) as count_posts
FROM `bigquery-public-data.stackoverflow.posts_answers` 
where owner_user_id is not null
group by owner_user_id, year_post
), ranked_t as   
(select *,
  row_number() over (partition by year_post order by count_posts desc) as ranked_year
  from total_posts
) select *
from ranked_t
where ranked_year = 1
order by year_post desc;


--¿Cuál es el promedio de score de las respuestas por usuario, pero solo para aquellos que tienen al menos 10 respuestas?

select owner_user_id, avg(score) as avg_score, count(*) as count_answers
from `bigquery-public-data.stackoverflow.posts_answers`
group by owner_user_id
having count(*) >= 10;


-- ¿Cuántas preguntas con la etiqueta python recibieron más de 5 respuestas en los últimos 2 años?

with q_filtered as (
select id
from `bigquery-public-data.stackoverflow.posts_questions`
where tags like '%python%' and
date(creation_date) >= date_sub(current_date(), interval 2 year)
), answers_counted as (
select parent_id as question_id, count(*) as total_answers
from `bigquery-public-data.stackoverflow.posts_answers`
group by parent_id
) select count(*) as total_questions_final
from q_filtered q
join answers_counted a
on q.id = a.question_id
where total_answers >= 5;


--📦 Dataset: bigquery-public-data.thelook_ecommerce
-- ¿Cuál es el producto más caro vendido cada mes?

with year_month_t as (
SELECT product_id, sale_price, format_date('%Y-%m', created_at) as year_month
FROM `bigquery-public-data.thelook_ecommerce.order_items`
), ranked_prod as (select *, 
row_number() over (partition by year_month order by sale_price desc) as rank_price
from year_month_t
), expesive_prod as
( select product_id, sale_price, year_month
from ranked_prod
where rank_price = 1
) select prod.name, ep.sale_price, ep.year_month 
from expesive_prod ep
join `bigquery-public-data.thelook_ecommerce.products` prod
on ep.product_id = prod.id
order by ep.year_month desc;

--¿Cuánto gastó en promedio cada cliente en su primera compra?

with user_orders as (
select user_id, order_id, min(created_at) as min_date, sum(sale_price) as total_order
FROM bigquery-public-data.thelook_ecommerce.order_items
group by user_id, order_id
), ranked_min_order as( select *, 
row_number() over (partition by user_id order by min_date) as min_order
from user_orders
) select avg(ranked_min_order.total_order)
from ranked_min_order
where min_order = 1;

--Calcula el ingreso acumulado por mes

with year_month_sales as (
select format_date('%Y-%m', created_at) as year_month, sum(sale_price) as montly_sales
FROM `bigquery-public-data.thelook_ecommerce.order_items`
group by year_month
) select year_month, sum(montly_sales) over (order by year_month)
from year_month_sales
order by year_month desc;


--¿Qué clientes están en el top 10% de gasto total?

with user_sales as(
select user_id, sum(sale_price) as sum_sales
from `bigquery-public-data.thelook_ecommerce.order_items`
group by user_id
), ntile_results as (select *,
ntile(10) over (order by sum_sales desc) ntile_column
from user_sales
) select *
from ntile_results
where ntile_column = 1;

--🏷 Dataset: bigquery-public-data.thelook_ecommerce.orders

--1: Clientes más frecuentes en los últimos 3 meses

select user_id, count(*)
from `bigquery-public-data.thelook_ecommerce.orders`
where date(created_at) >= date_sub(current_date(), interval 3 month)
group by user_id
order by count(*) desc
limit 10;

--2: Pedidos repetidos por cliente
--¿Cuántos clientes realizaron más de 1 pedido el mismo día?

with clients_orders as (
select user_id, date(created_at) as order_day, count(*) as count_orders
from `bigquery-public-data.thelook_ecommerce.orders`
group by user_id, order_day
having count(*) > 1
) select count(distinct user_id)
from clients_orders;

--🏷 Dataset: bigquery-public-data.thelook_ecommerce.order_items
---Ejercicio 3: Productos más vendidos por cantidad total de unidades
--¿Cuáles son los 5 productos más vendidos en total?

 select oi.product_id, p.name, count(*) as total_qty
from `bigquery-public-data.thelook_ecommerce.order_items` oi 
join `bigquery-public-data.thelook_ecommerce.products` p
on oi.product_id = p.id
where status <> 'returned'
group by oi.product_id, p.name
order by count(*) desc
limit 5;

--🏷 Dataset: bigquery-public-data.thelook_ecommerce.users + orders
--Ejercicio 4: Clientes nuevos por mes
--¿Cuántos clientes nuevos se registraron cada mes?

select format_date('%Y-%m',u.created_at) as user_date,
count(distinct u.id) as user_count
from `bigquery-public-data.thelook_ecommerce.users` u 
join `bigquery-public-data.thelook_ecommerce.orders` o
on u.id = o.user_id
group by user_date
order by user_date desc;

--🏷 Dataset: bigquery-public-data.thelook_ecommerce.orders
-- Ejercicio 5: Tiempo promedio entre pedidos por cliente
--¿Cuál es el tiempo promedio entre pedidos para los 10 clientes más activos?

with top10 as (
select user_id, count(*) as total_orders
from `bigquery-public-data.thelook_ecommerce.orders`
group by user_id
order by count(*) desc
limit 10
),dates as (
select t.user_id as user_id, date(o.created_at) as curr_date, 
lag(date(o.created_at)) over (partition by t.user_id order by date(o.created_at)) as prev_date
from `bigquery-public-data.thelook_ecommerce.orders` o 
join top10 t on
o.user_id = t.user_id
), days as( 
select user_id, date_diff(curr_date, prev_date, day) as date_dif
from dates 
where date_diff(curr_date, prev_date, day) is not null
order by date_dif desc
) select user_id, avg(date_dif) as avg_days_diff
from days
group by user_id
order by avg_days_diff;

--📘 Dataset: bigquery-public-data.stackoverflow.posts_questions
--✅ Ejercicio 6: Preguntas resueltas por mes
--¿Cuántas preguntas con respuesta aceptada (accepted_answer_id no nulo) se registraron por mes?

select extract(year from creation_date) as year, extract(month from creation_date) as month, 
count(*) as total_accepted_answers
from `bigquery-public-data.stackoverflow.posts_questions`
where accepted_answer_id is not null
group by year, month
order by year desc, month desc;

--✅ Ejercicio 7: Lenguajes más mencionados en el último año
--¿Cuáles son los 10 lenguajes más mencionados en las etiquetas (tags) de las preguntas del último año?

select tags, count(*) as tag_mentions
from `bigquery-public-data.stackoverflow.posts_questions`,
unnest(split(tags, '|')) as tags
where date(creation_date) >= date_sub(current_date(), interval 1 year)
group by tags
order by tag_mentions desc
limit 10;


--✅ Ejercicio 8: Usuarios con más preguntas resueltas
--¿Qué usuarios (owner_user_id) tienen la mayor cantidad de preguntas con respuesta aceptada?

select owner_user_id, count(*) as answered_q
from `bigquery-public-data.stackoverflow.posts_questions`
where accepted_answer_id is not null and 
owner_user_id is not null
group by owner_user_id
order by answered_q desc;

--✅ Ejercicio 9: Pregunta sin resolver más antigua
--¿Cuál es la pregunta más antigua que aún no tiene una respuesta aceptada?

select id, creation_date
from `bigquery-public-data.stackoverflow.posts_questions`
where accepted_answer_id is null
order by creation_date
limit 1;

--✅ Ejercicio 10: Duración promedio hasta la aceptación
--¿Cuánto tiempo promedio tarda en aceptarse una respuesta desde que se publica la pregunta?

select avg(date_diff(a.creation_date, q.creation_date, day)) as days
from `bigquery-public-data.stackoverflow.posts_questions` q 
join `bigquery-public-data.stackoverflow.posts_answers` a 
on q.accepted_answer_id = a.id
where q.accepted_answer_id is not null;

--✅ Ejercicio 11: Distribución de preguntas por día de la semana
--¿Cuál es el día de la semana con más preguntas publicadas? ¿Y con menos?

with dayofweek_t as (
select extract(dayofweek from creation_date) as day_week, count(*) as total_q
from `bigquery-public-data.stackoverflow.posts_questions`
group by day_week
), ranked_t as 
( select *,
rank() over (order by total_q) as min_rank,
rank() over (order by total_q desc) as max_rank
from dayofweek_t
) select day_week, total_q
from ranked_t
where min_rank = 1
or max_rank = 1
