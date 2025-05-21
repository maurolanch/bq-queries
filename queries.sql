
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
order by year_post desc


--¿Cuál es el promedio de score de las respuestas por usuario, pero solo para aquellos que tienen al menos 10 respuestas?

select owner_user_id, avg(score) as avg_score, count(*) as count_answers
from `bigquery-public-data.stackoverflow.posts_answers`
group by owner_user_id
having count(*) >= 10
¶

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
where total_answers >= 5