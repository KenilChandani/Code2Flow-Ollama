--Selecting all data from customer table
select * from ds_tt.customer_kc;

--Selecting distinct countries from customer table
select distinct country from ds_tt.customer_kc;

--Where caluse
select * from ds_tt.customer_kc
where country='United States';

select min(total_spent),max(total_spent),sum(total_spent),avg(total_spent) from ds_tt.customer_kc;

select count(customer_id) from ds_tt.customer_kc
where country='United States';

select * from ds_tt.employee_mk em  
where email like '%x%.%';

select * from ds_tt.customer_kc
where country IN('France');

select * from ds_tt.customer_kc 
where total_spent between 5000 and 6000;

select country,count(customer_id)  from ds_tt.customer_kc 
group by country ;

select country,count(customer_id)  from ds_tt.customer_kc 
group by country
having avg(total_spent) < 3000;

select tpp.first_name,emp.last_name ,gpa,salary from ds_tt.topper_students_pp   as tpp inner join ds_tt.employee_MK  as emp
on tpp.student_id= emp.employee_id 

SELECT ck.first_name FROM ds_tt.customer_kc AS ck
UNION
SELECT em.FIRST_name FROM ds_tt.employee_mk AS  em 

SELECT ck.first_name FROM ds_tt.customer_kc AS ck
INTERSECT 
SELECT em.FIRST_name FROM ds_tt.employee_mk AS  em 

SELECT ck.first_name FROM ds_tt.customer_kc AS ck
EXCEPT	
SELECT em.FIRST_name FROM ds_tt.employee_mk AS  em 

SELECT * FROM ds_tt.customer_kc AS ck 
WHERE customer_id IN (
	SELECT student_id FROM ds_tt.topper_students_pp WHERE gpa > 3
)

SELECT country,sum(total_spent)  from ds_tt.customer_kc 
group by country ORDER BY sum(total_spent) DESC  ;

SELECT customer_id ,first_name ,last_name,total_spent,
CASE 
	WHEN total_spent> 2500 THEN 'Regular customer'
	WHEN total_spent<2500 THEN 'Seasonal customer'
END AS type
FROM ds_tt.customer_kc 


SELECT * FROM ds_tt.customer_kc LIMIT 100 OFFSET 200

WITH country_sales AS (
	SELECT country,sum(total_spent) AS total FROM ds_tt.customer_kc GROUP BY country 
),top_country AS (
	SELECT country FROM country_sales WHERE total > (SELECT SUM(total)/51 FROM country_sales)
)
SELECT country ,sum(total_spent) FROM ds_tt.customer_kc 
WHERE country IN (SELECT country FROM top_country)
GROUP BY country  ORDER BY sum(total_spent) DESC 

SELECT job_title ,sum(salary) FROM ds_tt.employee_mk 
GROUP BY job_title ORDER BY sum(salary) DESC

SELECT DISTINCT(department)  FROM ds_tt.employee_mk 


SELECT DISTINCT(department),ARRAY (
	SELECT DISTINCT(job_title) FROM ds_tt.employee_mk WHERE department=department
)FROM ds_tt.employee_mk

SELECT department ,array_agg(DISTINCT job_title )FROM  ds_tt.employee_mk GROUP BY department


SELECT country,max(total_spent) FROM ds_tt.customer_kc ck 
GROUP BY country

SELECT total_spent ,ROW_NUMBER () OVER( ORDER BY total_spent) AS r1,ROW_NUMBER () OVER( ORDER BY total_spent DESC) AS r2 FROM ds_tt.customer_kc

SELECT avg(total_spent) AS "median" FROM (
	SELECT total_spent ,ROW_NUMBER () OVER( ORDER BY total_spent) AS r1,
	ROW_NUMBER () OVER( ORDER BY total_spent DESC) AS r2 FROM ds_tt.customer_kc	
) AS db
WHERE r1 IN (r2,r2-1,r2+1)




SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY total_spent) FROM ds_tt.customer_kc 

SELECT hire_date,age(current_date,hire_date) AS experience FROM ds_tt.employee_mk em  
ORDER BY age(current_date,hire_date) DESC

SELECT current_date,current_time,CURRENT_TIMESTAMP,localtime,localtimestamp,now()

SELECT date_part('day',now()) AS DAY,date_part('day',now()) AS month,date_part('year',now()) AS year  


 SELECT first_name|| ' ' || COALESCE (last_name,'') AS full_name FROM ds_tt.customer_kc ck 
 

 SELECT 
 (CASE 
 	WHEN city='San Pablo' THEN(
 	CASE 
 		WHEN country='Philippines' THEN 'Philippine' ELSE 'Peruvian'
 	END
 	) 	
 	ELSE country
 END) AS nationality,*
FROM ds_tt.customer_kc ck WHERE city='San Pablo';

SELECT city FROM ds_tt.customer_kc ck WHERE city IN (
	select  city from ds_tt.customer_kc 
	GROUP BY  city
	HAVING count(city)>1
)

select * from ds_tt.customer_kc
where city='San Pablo';

SELECT LAG(customer_id ,1 ) OVER( order BY customer_id) AS prev 

SELECT * FROM ds_tt.customer_kc ck 
WHERE customer_id = (
	SELECT LAG(customer_id ,1 ) OVER( order BY customer_id) AS prev 
	FROM ds_tt.customer_kc ck WHERE customer_id =10
)

SELECT LAG(customer_id ,1 ) OVER( order BY customer_id) AS prev ,LEAD (customer_id ,1 ) OVER( order BY customer_id) AS next  FROM ds_tt.customer_kc ck 

SELECT customer_id ,LAG(customer_id,1)OVER() AS prev FROM ds_tt.customer_kc ck WHERE customer_id=10

--DENSE_RANK() FUNCTION WITH PARTITION BY clause
SELECT customer_id,country,total_spent,DENSE_RANK () OVER (PARTITION  BY country ORDER BY total_spent DESC)rank FROM ds_tt.customer_kc ck 

--Aggregrated function with OVER()
SELECT city,state,country,avg(total_spent) OVER(PARTITION BY country) country_avg FROM ds_tt.customer_kc ck 


--use With CTE to  get data
WITH rank_1 AS(
	SELECT customer_id,country,total_spent,DENSE_RANK () OVER (PARTITION  BY country ORDER BY total_spent DESC)rank FROM ds_tt.customer_kc ck
)
SELECT  * FROM rank_1 WHERE rank=1 

--Update employee salary with gpa>3.9
SELECT employee_id ,salary FROM ds_tt.employee_mk em JOIN ds_tt.topper_students_pp tsp ON employee_id =student_id WHERE gpa>3.9

UPDATE ds_tt.employee_mk em
SET salary=em.salary*1.1
FROM ds_tt.topper_students_pp JOIN ds_tt.employee_mk  ON employee_id =student_id
WHERE gpa>3.9


--sum and rank

SELECT sum(salary) OVER(PARTITION BY department),DENSE_RANK() OVER(PARTITION BY department ),department FROM ds_tt.employee_mk em 


WITH sum_salary AS (
	SELECT sum(salary) OVER(PARTITION BY department) AS sum_sal,department FROM ds_tt.employee_mk em ORDER BY sum_sal desc
),rank AS (
	SELECT DENSE_RANK() OVER(ORDER BY sum_sal),* FROM sum_salary ORDER BY sum_sal desc
)
SELECT * FROM rank


--JSON
INSERT INTO ds_tt.json_data 
VALUES('{"brand":"Apple","price":1200,"specs":{"cpu":"i7","ram":"16gb"}}')

SELECT   jsonb_path_query(info,'$.specs.*') FROM ds_tt.json_data 

SELECT * FROM ds_tt.json_data_pp jdp 

SELECT jsonb_path_query(j_data,'$.glossary') FROM ds_tt.json_data_pp jdp 

CREATE TYPE info AS (brand TEXT,price TEXT ,specs TEXT )

SELECT json_populate_record(NULL::info,'{"brand":"Apple","price":1200,"specs":{"cpu":"i7","ram":"16gb"}}') 

SELECT * FROM json_populate_record(NULL::specs,'{"cpu":"i7","ram":"16gb"}') 

SELECT json_populate_recordset(NULL::specs,col)  FROM (SELECT info->'specs' AS col FROM ds_tt.json_data jd ) AS x

SELECT
	col->>'brand' AS brand,
	col->'price' AS price,
	col->'specs'->>'cpu' AS cpu,
	col->'specs'->>'ram' AS ram
FROM
	(
	SELECT
		info AS col
	FROM
		ds_tt.json_data jd ) AS x

SELECT info->'specs' FROM ds_tt.json_data jd 

--pattern matching
SELECT email FROM ds_tt.employee_mk em  WHERE  email !~*'@.*x.*';
--REGEXP_MATCHES(email,'[*x*]'),

--Counting Occurences
WITH json_string AS(
	SELECT job_title,x,count(*) FROM(
		SELECT job_title,UNNEST(string_to_array(lower(job_title),null)) AS x,count(*) 
		FROM ds_tt.employee_mk em  
		GROUP BY job_title
	) sq
	GROUP BY job_title,x
) 
SELECT job_title,jsonb_object_agg(x,count)  FROM json_string GROUP BY job_title

