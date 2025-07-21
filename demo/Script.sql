--SELECT TRUNC(EXTRACT(EPOCH FROM ((TO_char(est_startedatdatetime,'HH:MI:SS')))/30) FROM ds_tt.event_data ed 
  
SELECT * FROM ds_tt.tt_data td 
  
SELECT * FROM ds_tt.event_data ed 

--Fetching epochs
SELECT
	trunc(EXTRACT(EPOCH FROM(est_startedatdatetime::timestamp::time)/ 120)) startepoch,
	trunc(EXTRACT(EPOCH FROM(est_endedatdatetime::timestamp::time)/ 120)) endepoch,
	*
FROM ds_tt.event_data ed 

	
SELECT td.linkid,epochcode,ed.eventid  FROM ds_tt.tt_data td INNER JOIN ds_tt.event_data ed 
ON td.day_of_month =date_part('day',est_startedatdatetime::TIMESTAMP) AND td."MONTH" =date_part('month',est_startedatdatetime::TIMESTAMP)


-----------------------------------------------------LInk epochs of particaular event--------------------------------------------

SELECT linkid,"MONTH",day_of_month,est_startedatdatetime,epochcode,traveltime,eventid,linkids
FROM ds_tt.tt_data td
INNER JOIN ds_tt.event_data ed 
ON
	td.day_of_month = date_part('day',est_startedatdatetime::TIMESTAMP)
	AND td."MONTH" = date_part('month',est_startedatdatetime::TIMESTAMP)
	AND td.linkid = ed.linkids
	AND td.epochcode BETWEEN EXTRACT(EPOCH FROM (est_startedatdatetime::timestamp::time)/120) AND EXTRACT(EPOCH FROM (est_endedatdatetime::timestamp::time)/120)
	

-----------------------------------------------------------------
	
SELECT * FROM ds_tt.tt_data td WHERE linkid=811144289 AND day_of_month IN (21,22,23)


---------------------------------------------INCIDENT FLAG---------------------------------------------------------
SELECT linkid,epochcode,day_of_month, td."MONTH", CASE 
	WHEN epochcode BETWEEN EXTRACT(EPOCH FROM (est_startedatdatetime::timestamp::time)/120) AND EXTRACT(EPOCH FROM (est_endedatdatetime::timestamp::time)/120) THEN 1
	ELSE 0
END AS congestion_flag
FROM ds_tt.tt_data td 
INNER JOIN ds_tt.event_data ed 
ON
	td.day_of_month = date_part('day',est_startedatdatetime::TIMESTAMP)
	AND td."MONTH" = date_part('month',est_startedatdatetime::TIMESTAMP) 
	AND td.linkid = ed.linkids
--	AND td.epochcode BETWEEN EXTRACT(EPOCH FROM (est_startedatdatetime::timestamp::time)/120) AND EXTRACT(EPOCH FROM (est_endedatdatetime::timestamp::time)/120)
ORDER BY linkid,"MONTH",day_of_month,epochcode


-------------------------------------------------------------------------------------------------------------------------------------------
SELECT DATE_PART('day',est_endedatdatetime::timestamp)-DATE_PART('day',est_startedatdatetime::timestamp) AS day_diff FROM ds_tt.event_data ed 

--INSERT INTO ds_tt.event_data VALUES(1236831020,	'New York',	30.0,	'congestion'	,'2022-04-21 11:51:19.000'	,'2022-04-22 05:47:43.000'	,811144289)

--DELETE FROM ds_tt.event_data WHERE est_endedatdatetime='2022-04-22 05:47:43.000'

SELECT DISTINCT "MONTH",day_of_month  FROM ds_tt.tt_data td  WHERE linkid =811144289 AND day_of_month IN (21,22)

--INSERT INTO ds_tt.tt_data  SELECT linkid, "YEAR", "MONTH",22 AS day_of_month  , daynum, epochcode, traveltime  FROM ds_tt.tt_data WHERE linkid =811144289 AND day_of_month IN (21,22)

------------------------------------------------------------------------------------------
SELECT linkid,"MONTH",day_of_month,est_startedatdatetime,est_endedatdatetime,epochcode,traveltime,eventid,linkids, CASE 
	WHEN epochcode BETWEEN trunc(EXTRACT(EPOCH FROM (est_startedatdatetime::timestamp::time)/120)) AND trunc(EXTRACT(EPOCH FROM (est_endedatdatetime::timestamp::time)/120)) THEN 1
	ELSE 0
END AS congestion_flag
FROM ds_tt.tt_data td 
INNER JOIN ds_tt.event_data ed 
ON
	td.day_of_month = date_part('day',est_startedatdatetime::TIMESTAMP)
	AND td."MONTH" = date_part('month',est_startedatdatetime::TIMESTAMP) 
	AND td.linkid = ed.linkids
	--AND td.epochcode BETWEEN EXTRACT(EPOCH FROM (est_startedatdatetime::timestamp::time)/120) AND EXTRACT(EPOCH FROM (est_endedatdatetime::timestamp::time)/120)
WHERE linkid =811144289 AND day_of_month IN (21,22)
ORDER BY linkid,"MONTH",day_of_month,epochcode

-----------------------------------------------------------------------------------------------------------
SELECT DISTINCT eventid,generate_series(trunc(EXTRACT(EPOCH FROM (est_startedatdatetime::timestamp::time)/120))::numeric,719) AS epoch 
FROM ds_tt.event_data ed  WHERE linkids =811144289 ORDER BY epoch

UNION ALL 

SELECT   generate_series(0,trunc(EXTRACT(EPOCH FROM (est_endedatdatetime::timestamp::time)/120)::numeric)) AS epoch
FROM ds_tt.event_data ed WHERE  linkids =811144289 ORDER BY epoch

----------------------------------------------------------------------------------------------------------------------------------

 

WITH cte AS (
	SELECT linkid,"MONTH",day_of_month,est_startedatdatetime,est_endedatdatetime,epochcode,traveltime,eventid,linkids, DATE_PART('day',est_endedatdatetime::timestamp)-DATE_PART('day',est_startedatdatetime::timestamp) AS day_diff
	FROM ds_tt.tt_data td 
	INNER JOIN ds_tt.event_data ed 
	ON
		td.day_of_month = date_part('day',est_startedatdatetime::TIMESTAMP)
		AND td."MONTH" = date_part('month',est_startedatdatetime::TIMESTAMP) 
		AND td.linkid = ed.linkids
		--AND td.epochcode BETWEEN EXTRACT(EPOCH FROM (est_startedatdatetime::timestamp::time)/120) AND EXTRACT(EPOCH FROM (est_endedatdatetime::timestamp::time)/120)
		WHERE linkid =811144289 AND day_of_month IN (21,22)
	ORDER BY linkid,"MONTH",day_of_month,epochcode
)
SELECT DISTINCT linkids,generate_series(0,trunc(EXTRACT(EPOCH FROM (est_endedatdatetime::timestamp::time)/120)::numeric)) AS epoch
FROM cte  ORDER BY epoch



---------------------------------------------------------------FINAL UPDATED QUERY LOGIC WITH POSTGRESQL---------------------------------------------------------------------------------------------------------------------------
WITH main AS (
	SELECT eventid,maineventtype,est_startedatdatetime,est_endedatdatetime,linkids,est_startedatdatetime::date AS startdate,DATE_PART('day',est_startedatdatetime::timestamp) AS startday,date_part('month',est_startedatdatetime::TIMESTAMP) AS startmonth,
	trunc(EXTRACT(EPOCH FROM(est_startedatdatetime::timestamp::time)/ 120)) AS  startepoch,est_endedatdatetime::date AS enddate,DATE_PART('day',est_endedatdatetime::timestamp) AS endday,date_part('month',est_endedatdatetime::TIMESTAMP) AS endmonth,trunc(EXTRACT(EPOCH FROM(est_endedatdatetime::timestamp::time)/ 120)) AS  endepoch
	FROM ds_tt.event_data ed 
),main2 AS (
	SELECT  linkid, "YEAR", "MONTH", day_of_month, daynum, epochcode, traveltime,concat("YEAR" ,'-' , "MONTH" , '-' ,day_of_month)::date AS date_column FROM ds_tt.tt_data td 
),joinq AS (
	SELECT DISTINCT linkid,maineventtype,"YEAR","MONTH",day_of_month,daynum,date_column,est_startedatdatetime,est_endedatdatetime,epochcode,traveltime,eventid,linkids,startdate,startday,startmonth,startepoch,enddate,endday,endmonth,endepoch,endday-startday AS day_diff 
	FROM main2
	INNER JOIN main 
	ON 
		main2.day_of_month IN  (SELECT UNNEST (ARRAY (SELECT date_part('day',generate_series(startdate::date,enddate::date,'1 day'))::integer))) 
		AND main2."MONTH" IN (SELECT UNNEST (ARRAY (SELECT date_part('month',generate_series(startdate::date,enddate::date,'1 month'))::integer))) 
		AND main2.linkid = linkids
		--WHERE linkid =811144289 AND day_of_month IN (21,22,23) AND eventid=1236831020
),cte AS (
	SELECT CASE 
		WHEN day_diff=0 THEN ARRAY (SELECT generate_series(startepoch::NUMERIC ,endepoch::NUMERIC))
		WHEN day_diff=1 AND date_column=startdate AND "MONTH"=startmonth THEN  ARRAY (SELECT generate_series(startepoch::NUMERIC,719))
		WHEN day_diff=1 AND date_column=enddate AND "MONTH"=endmonth  THEN ARRAY (SELECT generate_series(0,endepoch::NUMERIC))
		WHEN day_diff>1 AND date_column=startdate AND "MONTH"=startmonth THEN ARRAY (SELECT generate_series(startepoch::NUMERIC,719))
		WHEN day_diff>1 AND date_column>startdate AND date_column<enddate THEN ARRAY (SELECT generate_series(0,719))
		WHEN day_diff>1 AND date_column=enddate AND "MONTH"=endmonth THEN ARRAY (SELECT generate_series(0,endepoch::NUMERIC))
	END AS epoch_array,
	* FROM joinq
),flag AS (
	SELECT CASE 
			WHEN maineventtype='crash' AND epochcode = ANY(epoch_array) THEN 1 ELSE 0
		END AS crash,
		CASE 
			WHEN maineventtype='congestion' AND epochcode=ANY(epoch_array) THEN 1 ELSE 0 
		END AS congestion,
		linkid, maineventtype,"YEAR", "MONTH", day_of_month, daynum,est_startedatdatetime, est_endedatdatetime, epochcode, traveltime, eventid
	FROM cte 
) 
SELECT linkid, "YEAR", "MONTH", day_of_month, daynum, epochcode, traveltime,max(crash) AS crash_flag,max(congestion) AS congestion_flag
FROM flag
GROUP BY linkid, "YEAR", "MONTH", day_of_month, daynum, epochcode, traveltime
ORDER BY linkid,"MONTH",day_of_month,epochcode	
