--------------------------------------------------PERCENTAGE_DIFF next_connected_count =2 AND next_pcc =1---------------------------------------------------------
WITH vol AS
(SELECT DISTINCT isg_osm_id,next_isg_osm_id , next_connected_count, next_pcc, volume, next_volume FROM tm_new_data.nys_hour_8_volume_240321 nhvx 
WHERE next_connected_count =2 AND next_pcc =1 AND next_volume >0 AND volume>0)
,cap_vol AS(
	SELECT a.isg_osm_id, next_isg_osm_id, volume, next_volume, cap_vol, isg_lanes, capacity,perc,round(cap_vol / sum(cap_vol) OVER (PARTITION BY a.isg_osm_id),2) cap_vol_perc ,
	perc-round(cap_vol / sum(cap_vol) OVER (PARTITION BY a.isg_osm_id),2) perc_diff,max_cap
	FROM ( 
		SELECT  v.isg_osm_id, next_isg_osm_id, volume, next_volume, isg_lanes,capacity,round(next_volume ::NUMERIC / sum(next_volume) OVER (PARTITION BY v.isg_osm_id),2) perc ,
		round(volume*(capacity / sum(capacity) OVER(PARTITION BY v.isg_osm_id))) cap_vol,max(capacity) OVER(PARTITION BY next_isg_osm_id) max_cap
		FROM  vol v INNER JOIN gis_tables.nys_osm_hvc_data nohd 
		ON next_isg_osm_id = nohd.isg_osm_id 
		WHERE  v.isg_osm_id IN(SELECT isg_osm_id FROM vol GROUP BY isg_osm_id HAVING count(*) = 2)
		--LIMIT 100
	) a
)
SELECT count(*) FILTER(WHERE capacity >0 AND capacity=max_cap) AS total,count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND abs(perc_diff)<=0.05 ) AS thres_5,count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND abs(perc_diff)<=0.10 ) AS thres_10, 
count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND abs(perc_diff)<=0.20 ) AS thres_20,count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND abs(perc_diff)<=0.30 ) AS thres_30 FROM cap_vol
--SELECT count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND perc > cap_vol_perc) AS total,count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND perc > cap_vol_perc AND abs(perc_diff)<=0.05 ) AS thres_5,count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND perc > cap_vol_perc AND abs(perc_diff)<=0.10 ) AS thres_10, 
--count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND perc > cap_vol_perc AND abs(perc_diff)<=0.20 ) AS thres_20,count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND perc > cap_vol_perc AND abs(perc_diff)<=0.30 ) AS thres_30
--SELECT count(*) FILTER(WHERE capacity > 0 AND capacity= max_cap AND perc=cap_vol_perc)
--FROM cap_vol
--SELECT isg_osm_id, next_isg_osm_id, volume, next_volume, cap_vol, isg_lanes, capacity, perc, cap_vol_perc,perc_diff  FROM cap_vol
--WHERE capacity >0 AND capacity=max_cap --AND perc < cap_vol_perc AND abs(perc_diff)<=0.30
--ORDER BY isg_osm_id

------------------------------------------------GEH_CHECK next_connected_count =2 AND next_pcc =1-------------------------------------------------------------------

WITH vol AS(
	SELECT DISTINCT isg_osm_id,next_isg_osm_id , next_connected_count, next_pcc, volume, next_volume FROM tm_new_data.nys_hour_8_volume_240321 nhvx 
	WHERE next_connected_count =2 AND next_pcc =1 AND next_volume >0 AND volume>0)
,cap_vol AS(
	SELECT a.isg_osm_id, next_isg_osm_id, volume, next_volume, isg_lanes, capacity,cap_vol,round(sqrt(((2*(power((next_volume-cap_vol),2))))/(next_volume+cap_vol)),2) AS geh,max_cap
	FROM ( 
		SELECT  v.isg_osm_id, next_isg_osm_id, volume, next_volume, isg_lanes,capacity,round(volume*(capacity / sum(capacity) OVER(PARTITION BY v.isg_osm_id))) cap_vol
		,max(capacity) OVER(PARTITION BY next_isg_osm_id) max_cap
		FROM  vol v INNER JOIN gis_tables.nys_osm_hvc_data nohd 
		ON next_isg_osm_id = nohd.isg_osm_id 
		WHERE  v.isg_osm_id IN(SELECT isg_osm_id FROM vol GROUP BY isg_osm_id HAVING count(*) = 2)
		--LIMIT 100
	) a
)
SELECT count(*) FILTER(WHERE geh>10 AND capacity >0 AND capacity = max_cap) AS geh_10,count(*) FILTER(WHERE geh>15 AND capacity >0 AND capacity = max_cap) AS geh_15,
count(*) FILTER(WHERE geh>20 AND capacity >0 AND capacity = max_cap) AS geh_20,count(*) FILTER(WHERE geh>25 AND capacity >0 AND capacity = max_cap) AS geh_25 FROM cap_vol
--SELECT * FROM gis_tables.nys_osm_map WHERE isg_osm_id IN (SELECT next_isg_osm_id FROM cap_vol WHERE geh > 25) AND ramp=true
--SELECT * FROM cap_vol WHERE capacity >0 AND capacity = max_cap --AND geh>25 AND capacity>next_volume
--ORDER BY isg_osm_id


----------------------------------------------------PERC_DIFF  prev_connected_count =2 AND prev_ncc =1--------------------------------
WITH vol AS
(SELECT DISTINCT isg_osm_id,prev_isg_osm_id , prev_connected_count, prev_ncc, volume, prev_volume FROM tm_new_data.nys_hour_8_volume_240321 nhvx 
WHERE prev_connected_count =2 AND prev_ncc =1 AND prev_volume >0 AND volume>0)
,cap_vol AS(
	SELECT a.isg_osm_id, prev_isg_osm_id, volume, prev_volume, cap_vol, isg_lanes, capacity,perc,round(cap_vol / sum(cap_vol) OVER (PARTITION BY a.isg_osm_id),2) cap_vol_perc ,
	perc-round(cap_vol / sum(cap_vol) OVER (PARTITION BY a.isg_osm_id),2) perc_diff,max_cap
	FROM ( 
		SELECT  v.isg_osm_id, prev_isg_osm_id, volume, prev_volume, isg_lanes,capacity,round(prev_volume ::NUMERIC / sum(prev_volume) OVER (PARTITION BY v.isg_osm_id),2) perc ,
		round(volume*(capacity / sum(capacity) OVER(PARTITION BY v.isg_osm_id))) cap_vol,max(capacity) OVER(PARTITION BY prev_isg_osm_id) max_cap
		FROM  vol v INNER JOIN gis_tables.nys_osm_hvc_data nohd 
		ON prev_isg_osm_id = nohd.isg_osm_id 
		WHERE  v.isg_osm_id IN(SELECT isg_osm_id FROM vol GROUP BY isg_osm_id HAVING count(*) = 2)
		--LIMIT 100
	) a
)
--SELECT count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND perc < cap_vol_perc) AS total,count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND perc < cap_vol_perc AND abs(perc_diff)<=0.05 ) AS thres_5,count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND perc < cap_vol_perc AND abs(perc_diff)<=0.10 ) AS thres_10, 
--count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND perc < cap_vol_perc AND abs(perc_diff)<=0.20 ) AS thres_20,count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND perc < cap_vol_perc AND abs(perc_diff)<=0.30 ) AS thres_30
--FROM cap_vol
SELECT count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND perc > cap_vol_perc) AS total,count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND perc > cap_vol_perc AND abs(perc_diff)<=0.05 ) AS thres_5,count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND perc > cap_vol_perc AND abs(perc_diff)<=0.10 ) AS thres_10, 
count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND perc > cap_vol_perc AND abs(perc_diff)<=0.20 ) AS thres_20,count(*) FILTER(WHERE capacity >0 AND capacity=max_cap AND perc > cap_vol_perc AND abs(perc_diff)<=0.30 ) AS thres_30
FROM cap_vol
--SELECT count(*) FILTER(WHERE capacity > 0 AND capacity= max_cap AND perc=cap_vol_perc)
--FROM cap_vol
--SELECT isg_osm_id, prev_isg_osm_id, volume, prev_volume, cap_vol, isg_lanes, capacity, perc, cap_vol_perc,perc_diff  FROM cap_vol
--WHERE capacity >0 AND capacity=max_cap --AND perc < cap_vol_perc AND abs(perc_diff)<=0.30
--ORDER BY isg_osm_id


---------------------------------------------------GEH CHECK prev_connected_count=2 AND prev_ncc=1-----------------------------------
WITH vol AS(
	SELECT DISTINCT isg_osm_id,prev_isg_osm_id , prev_connected_count, prev_ncc, volume, prev_volume FROM tm_new_data.nys_hour_8_volume_240321 nhvx 
	WHERE prev_connected_count=2 AND prev_ncc=1 AND prev_volume >0 AND volume>0)
,cap_vol AS(
	SELECT a.isg_osm_id, prev_isg_osm_id, volume, prev_volume, isg_lanes, capacity,cap_vol,round(sqrt(((2*(power((prev_volume-cap_vol),2))))/(prev_volume+cap_vol)),2) AS geh,max_cap
	FROM ( 
		SELECT  v.isg_osm_id, prev_isg_osm_id, volume, prev_volume, isg_lanes,capacity,round(volume*(capacity / sum(capacity) OVER(PARTITION BY v.isg_osm_id))) cap_vol
		,max(capacity) OVER(PARTITION BY prev_isg_osm_id) max_cap
		FROM  vol v INNER JOIN gis_tables.nys_osm_hvc_data nohd 
		ON prev_isg_osm_id = nohd.isg_osm_id 
		WHERE  v.isg_osm_id IN(SELECT isg_osm_id FROM vol GROUP BY isg_osm_id HAVING count(*) = 2)
		--LIMIT 100
	) a
)
--SELECT count(*) FILTER(WHERE capacity >0 AND capacity = max_cap) AS totla,count(*) FILTER(WHERE geh>10 AND capacity >0 AND capacity = max_cap) AS geh_10,count(*) FILTER(WHERE geh>15 AND capacity >0 AND capacity = max_cap) AS geh_15,
--count(*) FILTER(WHERE geh>20 AND capacity >0 AND capacity = max_cap) AS geh_20,count(*) FILTER(WHERE geh>25 AND capacity >0 AND capacity = max_cap) AS geh_25 FROM cap_vol
--SELECT count(*) FILTER(WHERE capacity >0 AND capacity = max_cap) AS total,count(*) FILTER(WHERE geh<10 AND capacity >0 AND capacity = max_cap) AS geh_10,count(*) FILTER(WHERE geh<15 AND capacity >0 AND capacity = max_cap) AS geh_15,
--count(*) FILTER(WHERE geh<20 AND capacity >0 AND capacity = max_cap) AS geh_20,count(*) FILTER(WHERE geh<25 AND capacity >0 AND capacity = max_cap) AS geh_25 FROM cap_vol
SELECT * FROM gis_tables.nys_osm_map WHERE isg_osm_id IN (SELECT prev_isg_osm_id FROM cap_vol WHERE geh > 25) AND ramp=TRUE 
--SELECT * FROM cap_vol WHERE capacity >0 AND capacity = max_cap --AND geh>25 AND capacity>next_volume
--ORDER BY isg_osm_id



--------------------------------fc and areatype on ramps-------------------------------------------
SELECT * FROM gis_tables.nys_osm_hvc_data nohd WHERE fc IN (1,2)AND isg_osm_id IN (
	SELECT isg_osm_id  FROM gis_tables.nys_osm_map nom WHERE ramp=true
)
ORDER BY isg_lanes desc


WITH cte AS (
	SELECT fc,areatype,capacity,max(capacity) OVER (PARTITION BY isg_osm_id) max_cap FROM gis_tables.nys_osm_hvc_data nohd 
	WHERE isg_osm_id IN (
	SELECT isg_osm_id  FROM gis_tables.nys_osm_map nom WHERE ramp=true
	)
)
SELECT fc,areatype ,count(*),avg(capacity),min(capacity),percentile_cont(0.25) WITHIN GROUP (ORDER BY capacity) AS perc_25,
percentile_cont(0.50) WITHIN GROUP (ORDER BY capacity) AS perc_50,
percentile_cont(0.75) WITHIN GROUP (ORDER BY capacity) AS perc_75,max(capacity) FROM cte 
WHERE capacity=max_cap
GROUP BY fc,areatype -----------------------capacity WITHOUT lanes


WITH cte AS (
	SELECT fc,areatype,isg_lanes,capacity,max(capacity) OVER (PARTITION BY isg_osm_id) max_cap FROM gis_tables.nys_osm_hvc_data nohd 
	WHERE isg_osm_id IN (
	SELECT isg_osm_id  FROM gis_tables.nys_osm_map nom WHERE ramp=true
	)
)
SELECT fc,areatype ,isg_lanes,count(*),avg(capacity),min(capacity),percentile_cont(0.25) WITHIN GROUP (ORDER BY capacity) AS perc_25,
percentile_cont(0.50) WITHIN GROUP (ORDER BY capacity) AS perc_50,
percentile_cont(0.75) WITHIN GROUP (ORDER BY capacity) AS perc_75,max(capacity) FROM cte 
WHERE capacity=max_cap
GROUP BY fc,areatype ,isg_lanes -------------capacity WITH lanes


--SELECT  fc, areatype,count(*),avg(volume),min(volume),percentile_cont(0.25) WITHIN GROUP (ORDER BY volume) AS perc_25,
--percentile_cont(0.50) WITHIN GROUP (ORDER BY volume) AS perc_50,
--percentile_cont(0.75) WITHIN GROUP (ORDER BY volume) AS perc_75,max(volume)  
--FROM tm_new_data.nys_hour_8_volume_240321 nhvx INNER JOIN gis_tables.nys_osm_hvc_data nohd 
--ON nhvx.isg_osm_id =nohd.isg_osm_id 
--WHERE nhvx.isg_osm_id IN ( 
--	SELECT isg_osm_id  FROM gis_tables.nys_osm_map nom WHERE ramp=true
--)
--WHERE volume>0 AND nhvx.isg_osm_id IN ( 
--	SELECT isg_osm_id  FROM gis_tables.nys_osm_map nom WHERE ramp=true
--)
--GROUP BY fc,areatype ------------------------volume WITHOUT lanes

SELECT fc,areatype,count(DISTINCT rmp.isg_osm_id),avg(volume),min(volume),percentile_cont(0.25) WITHIN GROUP (ORDER BY volume) AS perc_25,
percentile_cont(0.50) WITHIN GROUP (ORDER BY volume) AS perc_50,
percentile_cont(0.75) WITHIN GROUP (ORDER BY volume) AS perc_75,max(volume) FROM 
(SELECT DISTINCT isg_osm_id,volume FROM tm_new_data.nys_hour_8_volume_240321 WHERE volume>0 and highway_id in(2,4,6,8,15))rmp,
(SELECT isg_osm_id,fc,areatype from gis_tables.nys_osm_hvc_data WHERE highway LIKE '%_link') fc
WHERE fc.isg_osm_id=rmp.isg_osm_id
GROUP BY fc,areatype---------------------------------------volume>0 WITHOUT lanes

SELECT fc,areatype,isg_lanes,count(DISTINCT rmp.isg_osm_id),avg(volume),min(volume),percentile_cont(0.25) WITHIN GROUP (ORDER BY volume) AS perc_25,
percentile_cont(0.50) WITHIN GROUP (ORDER BY volume) AS perc_50,
percentile_cont(0.75) WITHIN GROUP (ORDER BY volume) AS perc_75,max(volume) FROM 
(SELECT DISTINCT isg_osm_id,volume FROM tm_new_data.nys_hour_8_volume_240321 WHERE volume>0 and highway_id in(2,4,6,8,15))rmp,
(SELECT isg_osm_id,fc,areatype,isg_lanes from gis_tables.nys_osm_hvc_data WHERE highway LIKE '%_link') fc
WHERE fc.isg_osm_id=rmp.isg_osm_id
GROUP BY fc,areatype,isg_lanes-------------------------------------------------volume>0 WITH lanes


SELECT fc,areatype,count(DISTINCT rmp.isg_osm_id),avg(volume),min(volume),percentile_cont(0.25) WITHIN GROUP (ORDER BY volume) AS perc_25,
percentile_cont(0.50) WITHIN GROUP (ORDER BY volume) AS perc_50,
percentile_cont(0.75) WITHIN GROUP (ORDER BY volume) AS perc_75,max(volume) FROM 
(SELECT DISTINCT isg_osm_id,volume FROM tm_new_data.nys_hour_8_volume_240321 WHERE highway_id in(2,4,6,8,15))rmp,
(SELECT isg_osm_id,fc,areatype from gis_tables.nys_osm_hvc_data WHERE highway LIKE '%_link') fc
WHERE fc.isg_osm_id=rmp.isg_osm_id
GROUP BY fc,areatype---------------------------------------volume WITHOUT lanes


SELECT fc,areatype,isg_lanes,count(DISTINCT rmp.isg_osm_id),avg(volume),min(volume),percentile_cont(0.25) WITHIN GROUP (ORDER BY volume) AS perc_25,
percentile_cont(0.50) WITHIN GROUP (ORDER BY volume) AS perc_50,
percentile_cont(0.75) WITHIN GROUP (ORDER BY volume) AS perc_75,max(volume) FROM 
(SELECT DISTINCT isg_osm_id,volume FROM tm_new_data.nys_hour_8_volume_240321 WHERE highway_id in(2,4,6,8,15))rmp,
(SELECT isg_osm_id,fc,areatype,isg_lanes from gis_tables.nys_osm_hvc_data WHERE highway LIKE '%_link') fc
WHERE fc.isg_osm_id=rmp.isg_osm_id 
GROUP BY fc,areatype,isg_lanes-------------------------------------------------volume WITH lanes


------------------------------------------------Ramp counts------------------------------------------------------------

SELECT cur_lanes,count(*) FROM (
	SELECT DISTINCT isg_osm_id ,cur_lanes FROM tm_new_data.nys_hour_8_volume_240321 nhv 
	WHERE highway_id IN (2,4,6,8,15)) rmp
GROUP BY cur_lanes
 
---------------------------------------------------------------------------------------


SELECT * FROM gis_tables.nys_osm_hvc_data nohd 
WHERE isg_osm_id IN (492213375001,668339512001,668339512002,668339513001,668339513002)

SELECT * FROM gis_tables.nys_osm_map nom WHERE isg_osm_id IN (492213375001,668339512001,668339512002,668339513001,668339513002)

-------------------------------------------------------------------------------------------------------------------------------------------
--SELECT nhv.isg_osm_id,next_isg_osm_id, prev_isg_osm_id, nhv.highway_group_id,next_highway_group_id,prev_highway_group_id,nohd.capacity,nohd2.capacity AS next_cap,nohd3.capacity AS prev_cap,nohd.fc,nohd.isg_lanes,nohd.areatype,nohd.hvp,nohd.geom 

WITH cte AS (
	SELECT nhv.highway_group_id ,
	--ARRAY_AGG(DISTINCT nhv.isg_osm_id ORDER BY nhv.isg_osm_id)::bigint[] as isg_osm_ids,
	--ARRAY_AGG(DISTINCT nohd.capacity )::bigint[] as capacity,
	--ARRAY_AGG(DISTINCT nohd2.capacity )::bigint[] as prev_capacity,
	--ARRAY_AGG(DISTINCT nohd3.capacity )::bigint[] as next_capacity,
	ST_UNION(ARRAY_AGG(DISTINCT nhv.geom )::geometry[]) as geom
	FROM tm_new_data.nys_hour_8_volume_240321 nhv 
	--INNER JOIN gis_tables.nys_osm_hvc_data_240419 nohd 
	--ON nhv.isg_osm_id =nohd.isg_osm_id 
	--LEFT JOIN gis_tables.nys_osm_hvc_data_240419 nohd2 
	--ON nhv.prev_isg_osm_id =nohd2.isg_osm_id 
	--LEFT JOIN gis_tables.nys_osm_hvc_data_240419 nohd3
	--ON nhv.next_isg_osm_id =nohd3.isg_osm_id 
	WHERE  nhv.highway_group_id!=nhv.next_highway_group_id OR nhv.highway_group_id !=nhv.prev_highway_group_id 
	GROUP BY nhv.highway_group_id
	--HAVING MIN(nhv.volume) >=50 AND MAX(nhv.volume) > 2000	
)
SELECT  DISTINCT c.highway_group_id,nhv.prev_highway_group_id,nhv.next_highway_group_id,nohd.capacity AS capacity,nohd2.capacity AS prev_capacity,nohd3.capacity AS next_capacity,c.geom FROM cte c
INNER JOIN tm_new_data.nys_hour_8_volume_240321 nhv
ON nhv.highway_group_id =c.highway_group_id
LEFT JOIN gis_tables.nys_osm_hvc_data_240419 nohd 
ON nohd.highway_group_id =nhv.highway_group_id 
LEFT JOIN gis_tables.nys_osm_hvc_data_240419 nohd2 
ON nohd2.highway_group_id =nhv.prev_highway_group_id 
LEFT JOIN gis_tables.nys_osm_hvc_data_240419 nohd3 
ON nohd3.highway_group_id =nhv.next_highway_group_id 



-----------------------------------------------------------------------------------------------------------

--WITH a as(
SELECT
  nhv.highway_group_id,
  ARRAY_AGG(DISTINCT nhv.isg_osm_id ORDER BY nhv.isg_osm_id)::bigint[] as isg_osm_ids,
  AVG(nhv.volume) AS avg_volume,
  MIN(nhv.volume) AS min_volume,
  MAX(nhv.volume) AS max_volume,
  AVG(capacity) AS avg_capacity,
  MIN(capacity) AS min_capacity,
  MAX(capacity) AS max_capacity,
  max(nhv.volume)-min(nhv.volume) AS min_mamx_diff, 
  ((max(nhv.volume)-min(nhv.volume))/min(nhv.volume))*100 AS max_min_perc_diff
FROM
  tm_new_data.nys_hour_8_volume_240321 nhv
INNER JOIN gis_tables.nys_osm_hvc_data_240419 nohd 
ON nhv.isg_osm_id = nohd.isg_osm_id
WHERE
  nhv.volume > 0
GROUP BY
  nhv.highway_group_id
HAVING MIN(nhv.volume) BETWEEN 20 AND 300 AND MAX(nhv.volume) > 2000	
 
SELECT r.*,ln.src from gis_tables.nys_osm_hvc_data_240419 r,
gis_tables.nys_osm_lanes_speed_details_speed_2  LN
WHERE ln.isg_osm_id = r.isg_osm_id 

WHERE highway_group_id = 421325 117858 

WITH ratio AS (
	select *, volume/capacity as vc_ratio,floor((volume/capacity)*10) as bucket  from gis_tables.nys_osm_hvc_data_240419  where volume > 0
)
SELECT r.*,LN.src FROM ratio r,
gis_tables.nys_osm_lanes_speed_details_speed_2 ln
WHERE LN.isg_osm_id = r.isg_osm_id --AND bucket>=11--vc_ratio >=1.1
 

--SELECT fc,count(*)  FROM ratio r WHERE bucket>=11 GROUP BY fc
--SELECT isg_lanes,maxspeed,count(*) FROM ratio r WHERE bucket>=11 GROUP BY isg_lanes,maxspeed
--SELECT isg_lanes,count(*)  FROM ratio r WHERE bucket>=11 GROUP BY isg_lanes
--SELECT maxspeed,count(*)  FROM ratio r WHERE bucket>=11 GROUP BY maxspeed

WITH ratio AS (
	select *, volume/capacity as vc_ratio,floor((volume/capacity)*10) as bucket  from gis_tables.nys_osm_hvc_data_240419  where volume > 0 AND fc IN (1,2,3) AND highway  LIKE '%_link'
)
SELECT bucket,count(*) FROM ratio 
GROUP BY bucket
ORDER BY bucket

SELECT isg_lanes,maxspeed,count1,sum(count1) OVER (PARTITION BY isg_lanes) FROM (SELECT isg_lanes,maxspeed,count(*) AS count1  FROM ratio GROUP BY isg_lanes,maxspeed) a

--SELECT isg_lanes,maxspeed, count(*) OVER(PARTITION BY isg_lanes,maxspeed),count(*) OVER(PARTITION BY isg_lanes) FROM ratio




 

SELECT * from
gis_tables.nys_osm_lanes_speed_details_speed_2
WHERE 
isg_osm_id IN (20226325001)
(5700479002,20111097001,20174249001,21087543001,24117867002,27260412001,32131879001,32144449005)

--SELECT isg_lanes,maxspeed,count(*) OVER (PARTITION  BY isg_lanes,maxspeed) AS count1,count(*)OVER (PARTITION  BY isg_lanes) FROM cte WHERE bucket=0



------------------------------------------------------------------------------------------

with grp_table as
(select highway_group_id,count(*) total,count(*) filter(where volume >0) available ,
min(volume) filter(where volume >0) as min_vol,
max(volume) filter(where volume >0) as max_vol,
round(avg(volume) filter(where volume >0),0) as avg_vol,
percentile_cont(0.25) within group(order by volume) filter(where volume >0) p_25,
percentile_cont(0.5) within group(order by volume) filter(where volume >0) p_5,
percentile_cont(0.75) within group(order by volume) filter(where volume >0) p_75,
percentile_cont(0.95) within group(order by volume) filter(where volume >0) p_95
from gis_tables.nys_osm_hvc_data_240419 
group by highway_group_id 
having (max(volume) filter(where volume >0)  - min(volume) filter(where volume >0))>1500
order by count(*) filter(where volume >0) desc 
limit 10)
select highway_group_id,areatype,isg_lanes,spd_cat,
count(*) total, count(*) filter(where volume >0) available,
count(*) filter(where volume <0) un_available,
st_union(geom) filter(where volume >0) available,
st_union(geom) filter(where volume <0) un_available,
avg(vc_ratio) filter(where volume >0) as avg_vc,
 avg(capacity) as avg_cap,
array_agg(vc_ratio) filter(where volume >0) as vc_array,
array_agg(volume) filter(where volume >0) as volume_array,
array_agg(capacity) capacity_array,
round(avg(vc_ratio) filter(where volume >0) * avg(capacity),0) as estimated_vol
from
(select distinct isg_osm_id,volume,capacity,highway_group_id,areatype,isg_lanes,
case 
    when maxspeed between 0 and 30 then 'Low' 
    when maxspeed between 31 and 45 then 'Mid'
    when maxspeed > 45 then  'High'
end as spd_cat,
volume/capacity as vc_ratio,geom
from gis_tables.nys_osm_hvc_data_240419 where highway_group_id in(select highway_group_id from grp_table))foo
group by highway_group_id,areatype,isg_lanes,spd_cat
order by highway_group_id,areatype,isg_lanes,spd_cat

---------------------------------------------------------------------------------------------------------------

SELECT isg_osm_id ,highway_group_id,cur_fc,countyid,cur_maxspeed,volume ,geom FROM tm_new_data.nys_hour_8_volume_240321 nhv
WHERE cur_fc=2 AND volume >0 AND countyid IN (94,92,35,42,55,71) --AND volume BETWEEN 0 AND 100
GROUP BY isg_osm_id ,highway_group_id,cur_fc,countyid,cur_maxspeed,volume ,geom
HAVING min(volume) < 100
ORDER BY isg_osm_id 

SELECT * FROM tm_new_data.osm_ris_master_bidir_230622 ormb
WHERE isg_osm_id IN (
	SELECT isg_osm_id FROM gis_tables.nys_osm_hvc_data_240419 nohd WHERE fc = 2 AND volume BETWEEN 0 AND 100 AND countyid IN (94, 92, 35, 42, 55, 71)
)
ORDER BY isg_osm_id




SELECT * FROM tm_new_data.sc_combined_data_with_osm_231109 WHERE isg_osm_id=20120517001 AND  HOUR=9 



SELECT isg_osm_id,countyid,maxspeed,isg_lanes,geom,volume,highway_group_id FROM gis_tables.nys_osm_hvc_data_240419 nohd 
WHERE fc=2 AND volume BETWEEN 0 AND 100 AND countyid IN (94,92,35,42,55,71)
ORDER BY isg_osm_id

SELECT isg_osm_id, maxspeed,highway_group_id ,volume ,geom  FROM gis_tables.nys_osm_hvc_data_240419 nohd 
WHERE fc=1 AND volume BETWEEN 0 AND 100  AND highway IN ('motorway_link') AND countyid IN (94,92,35,42,55,71)
ORDER BY isg_osm_id


SELECT isg_osm_id,avg(total) FROM tm_new_data.sc_combined_data_with_osm_231109 scdwo WHERE  HOUR=9 AND total>0 AND isg_osm_id IN (
	SELECT isg_osm_id  FROM gis_tables.nys_osm_hvc_data_240419 nohd 
	WHERE fc=1 AND volume BETWEEN 0 AND 100  AND highway IN ('motorway_link') AND countyid IN (94,92,35,42,55,71)
	ORDER BY isg_osm_id	
)
GROUP BY isg_osm_id 


SELECT * FROM tm_new_data.osm_ris_master_bidir_230622 ormb WHERE isg_osm_id IN (20174036001)


---------------------------------------------------fc=4 ANALYSIS-------------------------------------------------------------------------------------

SELECT DISTINCT isg_osm_id,countyid,region,roadwaytype_id,cur_maxspeed,cur_length,volume,geom FROM tm_new_data.nys_hour_8_volume_240321 nhv  
WHERE volume>0 AND cur_fc =4 AND "source" ='Original'

WITH bucket AS (
	SELECT DISTINCT isg_osm_id,countyid,region,roadwaytype_id,cur_lanes,cur_maxspeed,cur_length,volume,highway_id,
	CASE 
		WHEN volume BETWEEN 0 AND 200 THEN 1
		WHEN volume BETWEEN 201 AND 400 THEN 2
		WHEN volume BETWEEN 401 AND 600 THEN 3
		WHEN volume BETWEEN 601 AND 900 THEN 4
		WHEN volume BETWEEN 801 AND 1000 THEN 5
		WHEN volume BETWEEN 1001 AND 1200 THEN 6
		WHEN volume BETWEEN 1201 AND 1400 THEN 7
		WHEN volume BETWEEN 1401 AND 1600 THEN 8
		WHEN volume BETWEEN 1601 AND 1800 THEN 9
		WHEN volume BETWEEN 1801 AND 2000 THEN 10
		WHEN volume BETWEEN 2001 AND 2200 THEN 11
		WHEN volume BETWEEN 2201 AND 2400 THEN 12
		WHEN volume BETWEEN 2401 AND 2600 THEN 13
		WHEN volume BETWEEN 2601 AND 2800 THEN 14
		WHEN volume BETWEEN 2801 AND 3000 THEN 15
		WHEN volume BETWEEN 3001 AND 3200 THEN 16
	END AS bin,
	geom FROM tm_new_data.nys_hour_8_volume_240321 nhv 
	WHERE volume >0 AND cur_fc =4 AND "source" ='Original'
)
SELECT *  FROM bucket WHERE countyid IN(34,42,82,83,84,89,92,94) 

SELECT * FROM tm_new_data.nys_hour_8_volume_240321 nhv 

SELECT * FROM bucket WHERE highway_id=8

SELECT * FROM tm_new_data.osm_ris_master_bidir_230622 ormb WHERE isg_osm_id IN (99920138357008)

SELECT * FROM tm_new_data.osm_atr_master_bidir_221124 oamb  WHERE osmid IN (710301976001)


SELECT countyid,count(*) FROM bucket WHERE bin IN(1,2,3,4) GROUP BY countyid 

SELECT b.countyid,county_name,count(*),avg(volume),min(volume),percentile_cont(0.25) WITHIN GROUP (ORDER BY volume) AS p_25,
percentile_cont(0.50) WITHIN GROUP (ORDER BY volume) AS p_50,percentile_cont(0.75) WITHIN GROUP (ORDER BY volume) AS p_75,
percentile_cont(0.95) WITHIN GROUP (ORDER BY volume) AS p_95,max(volume) 
FROM bucket b INNER JOIN tm_new_data.nys_region_county_master_230712 nrcm 
ON nrcm.countyid=b.countyid GROUP BY b.countyid,county_name
--SELECT bin,count(*),avg(volume) AS average FROM bucket GROUP BY bin

SELECT * FROM tm_new_data.nys_region_county_master_230712 nrcm 

SELECT * FROM tm_new_data.highway_master_240321 hm 

SELECT * FROM tm_new_data.tm_clean_data_wd_231109 tcdw WHERE isg_osm_id =99920138357008

SELECT * FROM tm_new_data.tm_master_clean_data_wd_231109 tmcdw WHERE isg_osm_id =99920138357008

SELECT * FROM tm_new_data.nycdot_atr_data_with_osm_231109 nadwo WHERE isg_osm_id =5676734008 AND HOUR=8

SELECT * FROM tm.nycdot_atr_clean_data_220811 nacd WHERE segmentid =22096 AND hh=8

SELECT * FROM tm_new_data.osm_roadtype_master_231211 orm 

SELECT * FROM tm_ss.nycdot_atr nacd WHERE segmentid =22096 AND hh=8 


--CREATE INDEX ON tm_ss.nycdot_atr (segmentid)




WITH cte AS (
	SELECT fc,highway,count(*) AS total,count(*) FILTER(WHERE volume>0) AS available,count(*) FILTER(WHERE volume<0) AS not_available,
	count(*) FILTER (WHERE volume=1) vol_1,
	count(*) FILTER (WHERE volume BETWEEN 1 AND 10) vol_10,
	count(*) FILTER (WHERE volume BETWEEN 1 AND 15) vol_15,
	count(*) FILTER (WHERE volume BETWEEN 1 AND 20) vol_20,
	count(*) FILTER (WHERE volume BETWEEN 1 AND 25) vol_25,
	count(*) FILTER (WHERE volume BETWEEN 1 AND 30) vol_30,
	count(*) FILTER (WHERE volume BETWEEN 1 AND 35) vol_35,
	count(*) FILTER (WHERE volume BETWEEN 1 AND 40) vol_40,
	count(*) FILTER (WHERE volume BETWEEN 1 AND 45) vol_45,
	count(*) FILTER (WHERE volume BETWEEN 1 AND 50) vol_50
	FROM gis_tables.nys_osm_hvc_data_240419 nohd  
	GROUP BY fc,highway
)
SELECT fc,highway,total,available,not_available,round((available/total::numeric)*100,2) AS within_perc,
round(total::numeric/sum(total) OVER(),4)*100 AS total_perc,
round(available::numeric/sum(available) OVER(),4)*100 AS available_perc
FROM cte

SELECT fc, highway, total, available, vol_1,round((vol_1/available::numeric)*100,2) AS perc_1,
vol_10, round((vol_10/available::numeric)*100,2) AS perc_10, 
vol_15, round((vol_15/available::numeric)*100,2) AS perc_15,
vol_20, round((vol_20/available::numeric)*100,2) AS perc_20,
vol_25, round((vol_25/available::numeric)*100,2) AS perc_25,
vol_30, round((vol_30/available::numeric)*100,2) AS perc_30,
vol_35, round((vol_35/available::numeric)*100,2) AS perc_35,
vol_40, round((vol_40/available::numeric)*100,2) AS perc_40,
vol_45, round((vol_45/available::numeric)*100,2) AS perc_45,
vol_50, round((vol_50/available::numeric)*100,2) AS perc_50
FROM cte



select fc,highway,
count(*) total,
count(*) filter(where volume >0) available,
min(volume) filter(where volume >0 ) as min_vol,
percentile_cont(0.01) within group(order by volume) filter(where volume >0) as p_01,
percentile_cont(0.02) within group(order by volume) filter(where volume >0) as p_02,
percentile_cont(0.03) within group(order by volume) filter(where volume >0) as p_03,
percentile_cont(0.04) within group(order by volume) filter(where volume >0) as p_04,
percentile_cont(0.05) within group(order by volume) filter(where volume >0) as p_05,
percentile_cont(0.10) within group(order by volume) filter(where volume >0) as p_10,
percentile_cont(0.15) within group(order by volume) filter(where volume >0) as p_15,
percentile_cont(0.20) within group(order by volume) filter(where volume >0) as p_20,
percentile_cont(0.25) within group(order by volume) filter(where volume >0) as p_25,
percentile_cont(0.30) within group(order by volume) filter(where volume >0) as p_30,
percentile_cont(0.35) within group(order by volume) filter(where volume >0) as p_35,
percentile_cont(0.40) within group(order by volume) filter(where volume >0) as p_40,
percentile_cont(0.45) within group(order by volume) filter(where volume >0) as p_45,
percentile_cont(0.5) within group(order by volume) filter(where volume >0) as p_50,
percentile_cont(0.55) within group(order by volume) filter(where volume >0) as p_55,
percentile_cont(0.60) within group(order by volume) filter(where volume >0) as p_60,
percentile_cont(0.65) within group(order by volume) filter(where volume >0) as p_65,
percentile_cont(0.70) within group(order by volume) filter(where volume >0) as p_70,
percentile_cont(0.75) within group(order by volume) filter(where volume >0) as p_75,
percentile_cont(0.80) within group(order by volume) filter(where volume >0) as p_80,
percentile_cont(0.85) within group(order by volume) filter(where volume >0) as p_85,
percentile_cont(0.90) within group(order by volume) filter(where volume >0) as p_90,
percentile_cont(0.95) within group(order by volume) filter(where volume >0) as p_95,
percentile_cont(0.96) within group(order by volume) filter(where volume >0) as p_96,
percentile_cont(0.97) within group(order by volume) filter(where volume >0) as p_97,
percentile_cont(0.98) within group(order by volume) filter(where volume >0) as p_98,
percentile_cont(0.99) within group(order by volume) filter(where volume >0) as p_99,
max(volume) filter(where volume >0) as max_vol,
round(avg(volume) filter(where volume >0)) as avg_vol
from gis_tables.nys_osm_hvc_data_240419
group by fc,highway 
order by fc, highway

SELECT * FROM gis_tables.nys_osm_hvc_data_240419 nohd 
WHERE fc=5 AND highway ='tertiary' AND volume >5000

SELECT * FROM tm_new_data.osm_ris_master_bidir_230622 ormb WHERE rc_station IN (
SELECT rc_station FROM tm_new_data.osm_ris_master_bidir_230622 ormb 
WHERE isg_osm_id !=ori_isg_osm_id 
GROUP BY rc_station 
HAVING count(DISTINCT point_direction)=2 AND count(DISTINCT isg_osm_id)=2
)

--------------------------------------------Capacity Formula Update and Table Creation-------------------------------------------
------------------------------------------WITH LANES>2 AND ALL UPDATE CONDITIONS AND WITHOUT NULL VALUES--------------------------------------------------

--CREATE TABLE gis_tables.nys_osm_hvc_data_240611 AS 
SELECT isg_osm_id, region, countyid, fc, highway, maxspeed, final_place, isg_lanes, length, areatype, hvp, 
	CASE 
		WHEN highway='motorway' THEN 'Freeway'
		WHEN highway='trunk' THEN 'Multilane'
		WHEN highway = 'primary' AND areatype='Urban' THEN 'Primary_Urban'
		WHEN highway = 'primary' AND areatype='Rural' THEN 'Primary_Rural'
		WHEN highway LIKE '%_link' THEN 'Ramp'
	END AS r_type, 
	CASE 
		WHEN highway='motorway' THEN round(((2200+(10*(LEAST(70,maxspeed)-50)))/(1+(hvp::numeric / 100)))*isg_lanes,0)
		WHEN highway ='trunk' THEN 
			CASE 
				WHEN maxspeed<=60  THEN round(((1000+(20*maxspeed))*isg_lanes)/(1+(hvp::NUMERIC/100)),0) 
				ELSE round((2200*isg_lanes)/(1+(hvp::NUMERIC/100)),0)
			END
		WHEN highway IN ('primary','primary_link')  AND areatype='Urban' THEN 950*isg_lanes
		WHEN highway IN ('primary','primary_link') AND areatype='Rural' THEN 745*isg_lanes
		WHEN highway IN ('motorway_link','trunk_link') THEN  
				CASE WHEN isg_lanes = 1 THEN 
					CASE 
						WHEN maxspeed<30 THEN 1800	
						WHEN maxspeed BETWEEN 30 AND 50 THEN 1900
						WHEN maxspeed > 50 AND maxspeed <=65 THEN 2000
						ELSE 2100
					END
					ELSE 
					CASE
						WHEN maxspeed<30 THEN 2700	
						WHEN maxspeed BETWEEN 30 AND 50 THEN 3000
						WHEN maxspeed > 50 AND maxspeed <=65 THEN 3300
						ELSE 3600
					END
				END 
	END AS capacity
	, geom, highway_group_id, next_connected_count, prev_connected_count, volume FROM gis_tables.nys_osm_hvc_data_240419 nohd 
WHERE fc IN (1,2,3)
	
--CREATE INDEX ON gis_tables.nys_osm_hvc_data_240430(isg_osm_id)

SELECT * FROM gis_tables.nys_osm_hvc_data_240430 nohd 

--------------------------------------------------------UPDATING CAPACITY TABLE FOR OLD CAPACITY TABLE----------------------------------------

--UPDATE gis_tables.nys_osm_hvc_data_240430 
--SET capacity=950* isg_lanes
--WHERE highway='primary_link' AND areatype='Urban'

--UPDATE gis_tables.nys_osm_hvc_data_240430 
--SET capacity=745* isg_lanes
--WHERE highway='primary_link' AND areatype='Rural'

--UPDATE gis_tables.nys_osm_hvc_data_240430 
--SET capacity=capacity-500
--WHERE highway='motorway_link' AND isg_lanes > 1
-----------------------------------------------------------------------------------------------------------------------



WITH ratio AS (
	SELECT *, volume/capacity AS vc_ratio,floor((volume/capacity)*10) AS bucket  from gis_tables.nys_osm_hvc_data_240430  
	WHERE volume > 0 --AND highway='trunk_link'
)
SELECT * FROM ratio

SELECT bucket,count(*) FROM ratio  
GROUP BY bucket 
ORDER BY bucket


SELECT isg_osm_id,maxspeed ,isg_lanes,length,hvp,capacity FROM gis_tables.nys_osm_hvc_data_240419 nohd WHERE highway IN ('motorway_link','primary')

SELECT nohd.highway,count(*),avg(nohd.capacity)  FROM gis_tables.nys_osm_hvc_data_240430 nohd INNER JOIN gis_tables.nys_osm_hvc_data_240419 nohd2 
ON nohd.isg_osm_id =nohd2.isg_osm_id
WHERE nohd.highway ='primary' OR nohd2.highway ='motorway_link'
GROUP BY nohd.highway

SELECT highway ,isg_lanes ,count(*),min(capacity),
percentile_cont(0.25) WITHIN GROUP (ORDER BY capacity) AS p_25,
percentile_cont(0.50) WITHIN GROUP (ORDER BY capacity) AS p_50,
percentile_cont(0.75) WITHIN GROUP (ORDER BY capacity) AS p_75,
percentile_cont(0.95) WITHIN GROUP (ORDER BY capacity) AS p_95,
percentile_cont(0.99) WITHIN GROUP (ORDER BY capacity) AS p_99,
max(capacity)
FROM gis_tables.nys_osm_hvc_data_240430 nohd 
WHERE isg_lanes=1
GROUP BY highway,isg_lanes



SELECT * FROM tm_new_data.augmented_lanes_gid alg     

WITH ratio AS (
	SELECT *, volume/capacity AS vc_ratio,floor((volume/capacity)*10) AS bucket  from gis_tables.nys_osm_hvc_data_240430  
	WHERE volume > 0 AND region=11 --AND highway='trunk_link'
)
SELECT bucket,count(*) FROM ratio
GROUP BY bucket
ORDER BY bucket



----------------------------------------------model interpretation-----------------------------------------------

SELECT foo.cur_maxspeed  ,count(*)  FROM 
(
	SELECT DISTINCT isg_osm_id,highway_id,roadwaytype_id,cur_lanes,cur_maxspeed,cur_final_place,volume FROM tm_new_data.nys_hour_8_volume_240321 nhv 
) foo
,gis_tables.nys_osm_hvc_data_240430 nohd 
WHERE foo.isg_osm_id =nohd.isg_osm_id 
GROUP BY foo.cur_maxspeed 
ORDER BY foo.cur_maxspeed

SELECT foo.* ,region,next_connected_count,prev_connected_count,areatype, capacity  FROM 
(
	SELECT DISTINCT isg_osm_id,highway_id,roadwaytype_id,cur_lanes,CASE 
		WHEN cur_maxspeed BETWEEN 0 AND 30 THEN 'Low'
		WHEN cur_maxspeed BETWEEN 31 AND 45 THEN 'Mid'
		WHEN cur_maxspeed > 45 THEN 'High'
	END AS cur_maxspeed,cur_final_place,volume FROM tm_new_data.nys_hour_8_volume_240321 nhv 
) foo
,gis_tables.nys_osm_hvc_data_240430 nohd 
WHERE foo.isg_osm_id =nohd.isg_osm_id AND region IN (1,10,11)

SELECT * FROM gis_tables.nys_osm_hvc_data_240430 nohd WHERE isg_osm_id =48593989001

-------------------------------------------------------------------------------------------------------------------------

WITH cte1 AS (
	SELECT foo.isg_osm_id,foo.highway_id,foo.roadwaytype_id,foo.cur_lanes,foo.cur_maxspeed,foo.cur_final_place ,region,next_connected_count,prev_connected_count,areatype, 
		CASE 
			WHEN capacity BETWEEN 0 AND  2000 THEN 1
				WHEN capacity BETWEEN 2001 AND  5000 THEN 2
				WHEN capacity > 5000 THEN 3
		END AS capacity, 
		CASE 
			WHEN foo.volume  BETWEEN 1 AND 1000 THEN 1
			WHEN foo.volume BETWEEN 1001 AND 2500 THEN 2
			WHEN foo.volume >2500 THEN 3
		END AS volume  
	FROM 
	(
		SELECT DISTINCT isg_osm_id,highway_id,roadwaytype_id,cur_lanes,cur_maxspeed,cur_final_place,volume FROM tm_new_data.nys_hour_8_volume_240321 nhv WHERE volume>0
	) foo
	,gis_tables.nys_osm_hvc_data_240430 nohd 
	WHERE foo.isg_osm_id =nohd.isg_osm_id AND fc IN (1,2,3)
)
SELECT   roadwaytype_id,count(*) FROM cte1 
GROUP BY roadwaytype_id
ORDER BY roadwaytype_id


SELECT  capacity,volume,count(*) FROM cte1 WHERE cur_lanes>1
GROUP BY capacity,volume
ORDER BY capacity,volume 


SELECT DISTINCT roadwaytype_id,roadwaytype ,oneway, ramp, toll, bridge, tunnel FROM tm_new_data.osm_roadtype_master_231211 orm 
WHERE roadwaytype_id IN (
	SELECT  DISTINCT roadwaytype_id FROM cte1 WHERE capacity='Med_cap' AND volume='Low_vol'
)
ORDER BY roadwaytype_id

SELECT * FROM tm_new_data.osm_roadtype_master_231211 orm 

WITH cte1 AS (
	SELECT foo.isg_osm_id,foo.highway_id,foo.roadwaytype_id,foo.cur_lanes,foo.cur_maxspeed,foo.cur_final_place,foo.volume ,region,next_connected_count,prev_connected_count,areatype, 
		CASE 
			WHEN foo.cur_maxspeed BETWEEN 0 AND  30 THEN 1
				WHEN foo.cur_maxspeed BETWEEN 31 AND  45 THEN 2
				WHEN foo.cur_maxspeed > 45 THEN 3
		END AS maxspeed   
	FROM 
	(
		SELECT DISTINCT isg_osm_id,highway_id,roadwaytype_id,cur_lanes,cur_maxspeed,cur_final_place,volume FROM tm_new_data.nys_hour_8_volume_240321 nhv WHERE volume>0
	) foo
	,gis_tables.nys_osm_hvc_data_240430 nohd 
	WHERE foo.isg_osm_id =nohd.isg_osm_id AND fc IN (1,2,3)
)
SELECT   maxspeed,min(volume),max(volume),avg(volume),
	PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY volume) AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY volume) AS p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY volume) AS p75,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY volume) AS p90
FROM cte1 
GROUP BY maxspeed
ORDER BY maxspeed

---------------------------------------------------adjacent geh analysis 240521---------------------------

SELECT * FROM  tm_new_data.nys_hour_8_volume_240521_xgb_unseen_adjancent_geh_check  
WHERE geh>25

SELECT * FROM tm_new_data.nys_hour_8_volume_240521_xgb_unseen

SELECT * FROM gis_tables.hour_8_volume_fc_1_2_3_ml WHERE roadwaytype_id =21 AND SOURCE NOT IN ('ML')

SELECT * FROM tm_new_data.osm_roadtype_master_231211 orm WHERE roadwaytype_id =13

SELECT  min(volume),max(volume),avg(volume) FROM gis_tables.hour_8_volume_fc_1_2_3_ml WHERE cur_lanes=4 AND roadwaytype_id =12
GROUP BY cur_lanes 

SELECT roadwaytype_id ,count(*) FROM gis_tables.hour_8_volume_fc_1_2_3_ml hvfm WHERE isg_osm_id IN (
	SELECT isg_osm_id  FROM  tm_new_data.nys_hour_8_volume_240521_xgb_unseen_adjancent_geh_check  
	WHERE geh>25
)
GROUP BY roadwaytype_id 

WITH cte AS(
	SELECT   roadwaytype_id,count(*),avg(volume),min(volume),
	PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY volume) AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY volume) AS p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY volume) AS p75,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY volume) AS p90,
   	max(volume)
	FROM  gis_tables.hour_8_volume_fc_1_2_3_ml hvfm
	WHERE SOURCE NOT IN ('ML') --AND cur_lanes>=4
	GROUP BY roadwaytype_id
) 
SELECT * FROM cte WHERE roadwaytype_id=12

SELECT * FROM gis_tables.nys_osm_hvc_data_240430 nohd WHERE isg_osm_id IN (24118434001,122738318001,38031044001)

SELECT * FROM tm_new_data.nys_hour_8_volume_240530_RF_next_unseen_adjancent_geh_check WHERE geh>25
AND isg_osm_id IN (
SELECT isg_osm_id  FROM gis_tables.hour_8_volume_fc_1_2_3_rf_next hvfrn WHERE next_connected_count >1
)
 

SELECT * FROM gis_tables.hour_8_volume_fc_1_2_3_rf_1 hvfr 

WITH cte AS (
	SELECT * FROM tm_new_data.nys_hour_8_volume_240530_RF_prev_unseen_adjancent_geh_check WHERE geh>25
	UNION 
	SELECT * FROM tm_new_data.nys_hour_8_volume_240530_RF_next_unseen_adjancent_geh_check WHERE geh>25
)
SELECT * FROM cte --WHERE conn_tag='single'--SOURCE='ML' AND 'ML'=ANY(conn_src_array)

SELECT DISTINCT isg_osm_id  FROM tm_new_data.nys_hour_8_volume_240530_rf_next 
WHERE isg_osm_id IN (
	SELECT isg_osm_id FROM tm_new_data.nys_hour_8_volume_240530_RF_next_unseen_adjancent_geh_check WHERE geh>25
)

SELECT * FROM tm_new_data.nys_hour_8_volume_240530_rf_prev_unseen 
WHERE isg_osm_id IN (
	SELECT isg_osm_id FROM tm_new_data.nys_hour_8_volume_240530_RF_prev_unseen_adjancent_geh_check WHERE geh>25
)

SELECT * FROM tm_new_data.nys_hour_8_volume_240521_xgb_unseen_adjancent_geh_check nhvxuagc WHERE geh > 25 

WITH cte AS (
	SELECT 
	cur.isg_osm_id,cur.cur_fc,cur.highway_id,cur.countyid,cur.region,cur.roadwaytype_id,cur.next_connected_count,cur.prev_connected_count,	
	cur.cur_lanes,cur.cur_maxspeed,cur.cur_final_place,cur.volume,cur."source",
	CASE 
		WHEN cur_next.cur_next_vol IS NULL THEN ARRAY [cur_prev.cur_prev_vol] 
		WHEN cur_prev.cur_prev_vol IS NULL THEN ARRAY[cur_next.cur_next_vol]
		ELSE ARRAY[cur_next.cur_next_vol,cur_prev.cur_prev_vol]
	END AS conn_vol,
	CASE 
		WHEN cur_next.cur_next_vol IS NULL THEN ARRAY['prev']
		WHEN cur_prev.cur_prev_vol IS NULL THEN ARRAY['next']
		ELSE ARRAY['next','prev']
	END AS tag
	FROM gis_tables.hour_8_volume_fc_1_2_3_ml cur 
	LEFT JOIN 
	(SELECT isg_osm_id,volume  AS cur_next_vol FROM gis_tables.hour_8_volume_fc_1_2_3_rf_next hvfrn) cur_next
	ON cur.isg_osm_id =cur_next.isg_osm_id
	LEFT JOIN 
	(SELECT isg_osm_id,volume  AS cur_prev_vol FROM gis_tables.hour_8_volume_fc_1_2_3_rf_prev hvfrp) cur_prev
	ON cur.isg_osm_id = cur_prev.isg_osm_id
	WHERE SOURCE='ML' AND cur.isg_osm_id IN ( 
		SELECT isg_osm_id  FROM tm_new_data.nys_hour_8_volume_240530_RF_prev_unseen_adjancent_geh_check WHERE geh>25
		UNION 
		SELECT isg_osm_id  FROM tm_new_data.nys_hour_8_volume_240530_RF_next_unseen_adjancent_geh_check WHERE geh>25
	)
) 
SELECT * FROM cte --WHERE volume < cur_next_vol OR volume < cur_prev_vol

WITH cte AS (
	SELECT 
	cur.isg_osm_id,cur.cur_fc,cur.highway_id,cur.countyid,cur.region,cur.roadwaytype_id,cur.next_connected_count,cur.prev_connected_count,	
	cur.cur_lanes,cur.cur_maxspeed,cur.cur_final_place,cur.volume,cur."source",cur_next.cur_next_vol,cur_prev.cur_prev_vol
	FROM gis_tables.hour_8_volume_fc_1_2_3_ml cur 
	LEFT JOIN 
	(SELECT isg_osm_id,volume  AS cur_next_vol FROM gis_tables.hour_8_volume_fc_1_2_3_rf_next hvfrn) cur_next
	ON cur.isg_osm_id =cur_next.isg_osm_id
	LEFT JOIN 
	(SELECT isg_osm_id,volume  AS cur_prev_vol FROM gis_tables.hour_8_volume_fc_1_2_3_rf_prev hvfrp) cur_prev
	ON cur.isg_osm_id = cur_prev.isg_osm_id
	WHERE cur.isg_osm_id IN ( 
		SELECT isg_osm_id  FROM tm_new_data.nys_hour_8_volume_240530_RF_prev_unseen_adjancent_geh_check WHERE geh>25
		UNION 
		SELECT isg_osm_id  FROM tm_new_data.nys_hour_8_volume_240530_RF_next_unseen_adjancent_geh_check WHERE geh>25
	)
) 
SELECT * FROM cte WHERE volume > cur_next_vol AND volume > cur_prev_vol


SELECT tm.geh_check(4908,5007)
------------------old vs new---------------------------------

SELECT *, 'prev' AS tag FROM tm_new_data.nys_hour_8_volume_240530_rf_prev_unseen_adjancent_geh_check 
WHERE geh <=5  AND ("source" ='ML' OR 'ML'= ANY (conn_src_array))


WITH new AS (
  SELECT *, 'prev' AS tag FROM tm_new_data.nys_hour_8_volume_240530_rf_prev_unseen_adjancent_geh_check WHERE geh > 25
  --UNION 
  --SELECT *, 'next' AS tag FROM tm_new_data.nys_hour_8_volume_240530_rf_next_unseen_adjancent_geh_check WHERE geh > 25
)
SELECT NEW.isg_osm_id, NEW.conn_link_array, NEW.connected_count, NEW.source, NEW.conn_src_array,old.conn_src_array AS old_conn_src_array, NEW.volume , OLD.volume AS old_volume,
NEW.total_conn_vol AS new_total_conn_vol,OLD.total_conn_vol AS old_total_conn_vol,
NEW.conn_vol_array AS new_conn_vol_array,old.conn_vol_array AS old_conn_vol_array,NEW.geh AS new_geh,OLD.geh AS old_geh,tag
FROM NEW,(SELECT isg_osm_id,volume,total_conn_vol,conn_src_array,conn_vol_array,geh FROM tm_new_data.nys_hour_8_volume_240521_xgb_unseen_adjancent_geh_check 
WHERE geh > 25) OLD 
WHERE new.isg_osm_id = old.isg_osm_id
ORDER BY NEW.geh DESC 


WITH cte AS (
	SELECT DISTINCT *  FROM gis_tables.hour_8_volume_fc_1_2_3_rf_prev hvfrp  --tm_new_data.nys_hour_8_volume_240530_rf_prev  --tm_new_data.nys_hour_8_volume_240530_rf_prev_unseen
	WHERE isg_osm_id IN (
		SELECT isg_osm_id FROM tm_new_data.nys_hour_8_volume_240530_rf_prev_unseen_adjancent_geh_check WHERE geh>25
	)
)
SELECT source,count(*) FROM cte 
GROUP BY source


SELECT * FROM tm_ss.nycdot_atr_clean_data_220804 nacd  


SELECT * FROM tm.lion_data ld 

SELECT * FROM tm_ss.ris_data rd 

SELECT * FROM tm_ss.mta_hourly_bridge_counts mhbc 

SELECT * FROM tm.mta_bridge_master mbm 

SELECT * FROM osm_master.street_route_osm_bidir_extracted srobe 

SELECT * FROM tm_ss.custom_holiday ch 

SELECT * FROM osm_ny.osm_polygon

SELECT * FROM tm_new_data."Unseen_Prediction_new_approch_OHE" upnao 

SELECT * FROM tm_new_data.nys_hour_8_volume_240321 nhv WHERE isg_osm_id IN (4350451001)

SELECT * FROM tm_new_data.nys_hour_8_volume_240521_xgb_unseen

SELECT * FROM tm_new_data.nys_hour_8_volume_240530_rf_prev_unseen_adjancent_geh_check nhvrpuagc 




