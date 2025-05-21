
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

