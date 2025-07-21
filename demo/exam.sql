-------Q5---------
SELECT date_part('day',hire_date),date_part('month',hire_date),date_part('year',hire_date)  FROM ds_tt.employee


----------Q6---------
SELECT salary,CASE 
	WHEN salary < 50000 THEN 'LOW'
	WHEN salary BETWEEN 50000 AND 100000 THEN 'MEDIUM'
	ELSE 'HIGH'
END AS sal_category
FROM ds_tt.employee e 


----------Q7---------
SELECT * FROM ds_tt.employee e 
WHERE date_part('year',hire_date)='2021' 


-------Q8--------
SELECT * FROM ds_tt.students s WHERE age> 20 and major = 'Biology'
UNION 
SELECT * FROM ds_tt.students s2 WHERE major ='History'


SELECT * FROM ds_tt.students s WHERE (age> 20 and major = 'Biology') OR (major='History')

--------------Q9-----------
SELECT major,min(gpa) AS min_gpa,max(gpa) AS max_gpa FROM ds_tt.students s GROUP BY major  


-------------Q10-------------
WITH median AS (
	SELECT major,gpa,ROW_NUMBER() OVER(PARTITION BY major ORDER BY gpa ) AS asce,ROW_NUMBER() OVER(PARTITION BY major ORDER BY gpa desc) AS dsc FROM ds_tt.students s  
),median_gpa AS (
	SELECT avg(gpa) AS median_gpa FROM median WHERE asce IN (dsc,dsc+1,dsc-1) AND major='Computer Science' GROUP  BY major		
)
SELECT * FROM ds_tt.students,median_gpa WHERE major='Biology' AND gpa< median_gpa


-------------Q12-------------
SELECT * FROM ds_tt.students s WHERE first_name LIKE '%a'


--------------Q17------------
WITH min_sal AS (
	SELECT ROW_NUMBER () OVER (PARTITION BY department ORDER BY salary) AS row_num,department,salary FROM ds_tt.employee e 
)
SELECT department,salary FROM min_sal WHERE row_num=1


----------------Q19-----------
SELECT * ,avg(salary) OVER (PARTITION BY department) FROM ds_tt.employee e 