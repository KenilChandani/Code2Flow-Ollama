-------------------------------------------------conn_vol_pred------------------------------------------------------------------------
select distinct foo.*,areatype,r_type,capacity from
(select 
distinct isg_osm_id, next_isg_osm_id as conn_isg_osm_id, 
cur_fc, next_fc as conn_fc, 
highway_id, next_highway_id as conn_highway_id, 
countyid, next_countyid as conn_countyid,
region, next_region as conn_region, 
roadwaytype_id, next_roadwaytype_id as conn_roadwaytype_id,
next_connected_count as conn_count,  next_pcc as conn_ncc_pcc, 
cur_lanes, next_lanes as conn_lanes, 
cur_maxspeed, next_maxspeed as conn_maxspeed,
cur_final_place, next_final_place as conn_final_place,
volume, next_volume as conn_volume,
'next' as conn_tag
from 
tm_new_data.nys_hour_8_volume_240321 where next_isg_osm_id >0 and volume>0 and(next_connected_count > 1 or next_pcc > 1)
and 
(highway_id in(3,6,10,13,16) and next_highway_id in(3,6,10,13,16) and next_pcc = 2)
or (highway_id in(3,6,10,13,16) and next_highway_id not in(3,6,10,13,16) and next_pcc = 1)
or highway_id not in(3,6,10,13,16)
union
select 
distinct isg_osm_id, prev_isg_osm_id, 
cur_fc, prev_fc, 
highway_id, prev_highway_id, 
countyid, prev_countyid,
region, prev_region, 
roadwaytype_id, prev_roadwaytype_id,
prev_connected_count, prev_ncc,  
cur_lanes, prev_lanes, 
cur_maxspeed, prev_maxspeed,
cur_final_place, prev_final_place,
volume, prev_volume,
'prev' as conn_tag
from 
tm_new_data.nys_hour_8_volume_240321 where prev_isg_osm_id >0 and volume>0 and(prev_connected_count > 1 or prev_ncc > 1)
and 
(highway_id in(3,6,10,13,16) and prev_highway_id in(3,6,10,13,16) and prev_ncc = 2)
or (highway_id in(3,6,10,13,16) and prev_highway_id not in(3,6,10,13,16) and prev_ncc = 1)
or highway_id not in(3,6,10,13,16)
)foo,
gis_tables.nys_osm_hvc_data_240430 cap
where cap.isg_osm_id = foo.conn_isg_osm_id


------------------------------------------------------conn_vol_pred_fc_1_2_3----------240305---------------------------------------------------------------

WITH conn_osm AS (
	SELECT DISTINCT isg_osm_id,next_isg_osm_id AS conn_isg_osm_id,
	cur_fc,next_fc AS conn_fc,
	highway_id ,next_highway_id AS conn_highway_id,
	countyid,next_countyid AS conn_countyid,
	region ,next_region AS conn_region,
	roadwaytype_id, next_roadwaytype_id as conn_roadwaytype_id,
	next_connected_count AS conn_count,next_pcc AS conn_ncc_pcc,
	cur_lanes,next_lanes AS conn_lanes,
	cur_maxspeed,next_maxspeed AS conn_maxspeed,
	cur_final_place ,next_final_place AS conn_final_place,
	volume,next_volume AS conn_volume,
	'next' AS conn_tag
	FROM tm_new_data.nys_hour_8_volume_240321
	WHERE next_isg_osm_id>0 AND volume>0 AND cur_fc IN (1,2,3) AND next_fc IN (1,2,3) AND (next_connected_count > 1 OR next_pcc>1) 
	AND (highway_id IN (2,4,6) AND next_highway_id IN (2,4,6) AND next_pcc=2) OR (highway_id IN (2,4,6) AND next_highway_id IN (2,4,6) AND next_pcc=1)
	OR highway_id NOT IN (2,4,6)
	UNION 
	SELECT DISTINCT isg_osm_id, prev_isg_osm_id, 
	cur_fc, prev_fc, 
	highway_id, prev_highway_id, 
	countyid, prev_countyid,
	region, prev_region, 
	roadwaytype_id, prev_roadwaytype_id,
	prev_connected_count, prev_ncc,  
	cur_lanes, prev_lanes, 
	cur_maxspeed, prev_maxspeed,
	cur_final_place, prev_final_place,
	volume, prev_volume,
	'prev' as conn_tag
	FROM tm_new_data.nys_hour_8_volume_240321 
	WHERE prev_isg_osm_id >0 AND volume>0 AND cur_fc IN (1,2,3) AND next_fc IN (1,2,3) AND(prev_connected_count > 1 OR prev_ncc > 1)
	AND (highway_id IN(2,4,6) AND prev_highway_id IN(2,4,6) AND prev_ncc = 2)
	OR(highway_id IN(2,4,6) and prev_highway_id NOT IN(2,4,6) AND prev_ncc = 1)
	OR highway_id NOT IN(2,4,6)
)
SELECT DISTINCT conn_osm.*,areatype,r_type,capacity FROM conn_osm,gis_tables.nys_osm_hvc_data_240430 nohd 
WHERE conn_osm.isg_osm_id = nohd.isg_osm_id


-----------------------------------------------------------vol_pred_fc_123_without_capacity----------------------------------------

SELECT DISTINCT isg_osm_id,next_isg_osm_id AS conn_isg_osm_id,
	cur_fc,next_fc AS conn_fc,
	highway_id ,next_highway_id AS conn_highway_id,
	countyid,next_countyid AS conn_countyid,
	region ,next_region AS conn_region,
	roadwaytype_id, next_roadwaytype_id as conn_roadwaytype_id,
	next_connected_count AS conn_count,next_pcc AS conn_ncc_pcc,
	cur_lanes,next_lanes AS conn_lanes,
	cur_maxspeed,next_maxspeed AS conn_maxspeed,
	cur_final_place ,next_final_place AS conn_final_place,
	volume,next_volume AS conn_volume,
'next' AS conn_tag
FROM tm_new_data.nys_hour_8_volume_240321
WHERE next_isg_osm_id>0 AND volume>0 AND cur_fc IN (1,2,3) AND next_fc IN (1,2,3) AND next_connected_count > 1 AND next_pcc=1
UNION 
SELECT DISTINCT isg_osm_id, prev_isg_osm_id, 
	cur_fc, prev_fc, 
	highway_id, prev_highway_id, 
	countyid, prev_countyid,
	region, prev_region, 
	roadwaytype_id, prev_roadwaytype_id,
	prev_connected_count, prev_ncc,  
	cur_lanes, prev_lanes, 
	cur_maxspeed, prev_maxspeed,
	cur_final_place, prev_final_place,
	volume, prev_volume,
	'prev' as conn_tag
FROM tm_new_data.nys_hour_8_volume_240321 
WHERE prev_isg_osm_id >0 AND volume>0 AND cur_fc IN (1,2,3) AND prev_fc IN (1,2,3) AND prev_connected_count = 1 AND prev_ncc > 1

-----------------------------------------------------------------vol_pred_fc_123_without_capacity With Categorized Speed-------------------------------

SELECT DISTINCT isg_osm_id,next_isg_osm_id AS conn_isg_osm_id,
	cur_fc,next_fc AS conn_fc,
	highway_id ,next_highway_id AS conn_highway_id,
	countyid,next_countyid AS conn_countyid,
	region ,next_region AS conn_region,
	roadwaytype_id, next_roadwaytype_id as conn_roadwaytype_id,
	next_connected_count AS conn_count,next_pcc AS conn_ncc_pcc,
	cur_lanes,next_lanes AS conn_lanes,
	CASE 
		WHEN cur_maxspeed BETWEEN 0 AND 30 THEN 'Low'
		WHEN cur_maxspeed BETWEEN 31 AND 45 THEN 'Mid'
		WHEN cur_maxspeed > 45 then 'High'
	END AS cur_maxspeed 
	,
	CASE
		WHEN next_maxspeed BETWEEN 0 AND 30 THEN 'Low'
		WHEN next_maxspeed BETWEEN 31 AND 45 THEN 'Mid'
		WHEN next_maxspeed > 45 THEN 'High'
	END AS conn_maxspeed,
	cur_final_place ,next_final_place AS conn_final_place,
	volume,next_volume AS conn_volume,
'next' AS conn_tag
FROM tm_new_data.nys_hour_8_volume_240321
WHERE next_isg_osm_id>0 AND volume>0 AND cur_fc IN (1,2,3) AND next_fc IN (1,2,3) AND next_connected_count > 1 AND next_pcc=1
UNION 
SELECT DISTINCT isg_osm_id, prev_isg_osm_id, 
	cur_fc, prev_fc, 
	highway_id, prev_highway_id, 
	countyid, prev_countyid,
	region, prev_region, 
	roadwaytype_id, prev_roadwaytype_id,
	prev_connected_count, prev_ncc,  
	cur_lanes, prev_lanes, 
	CASE 
		WHEN cur_maxspeed BETWEEN 0 AND 30 THEN 'Low'
		WHEN cur_maxspeed BETWEEN 31 AND 45 THEN 'Mid'
		WHEN cur_maxspeed > 45 THEN 'High'
	END AS cur_maxspeed ,
	CASE
		WHEN prev_maxspeed BETWEEN 0 AND 30 THEN 'Low'
		WHEN prev_maxspeed BETWEEN 31 AND 45 THEN 'Mid'
		WHEN prev_maxspeed > 45 THEN 'High'
	END AS conn_maxspeed,
	cur_final_place, prev_final_place,
	volume, prev_volume,
	'prev' as conn_tag
FROM tm_new_data.nys_hour_8_volume_240321 
WHERE prev_isg_osm_id >0 AND volume>0 AND cur_fc IN (1,2,3) AND prev_fc IN (1,2,3) AND prev_connected_count = 1 AND prev_ncc > 1


--------------------------------------------------base_model_with_capacity_240508--------------------------------------


SELECT foo.* ,region,next_connected_count,prev_connected_count,areatype, capacity  FROM 
(
	SELECT DISTINCT isg_osm_id,highway_id,roadwaytype_id,cur_lanes,cur_maxspeed,cur_final_place,volume FROM tm_new_data.nys_hour_8_volume_240321 nhv 
) foo
,gis_tables.nys_osm_hvc_data_240430 nohd 
WHERE foo.isg_osm_id =nohd.isg_osm_id 

--------------------------------------------------base_model_with_capacity_and_catspeed_240508--------------------------------------
SELECT foo.* ,region,next_connected_count,prev_connected_count,areatype, capacity  FROM 
(
	SELECT DISTINCT isg_osm_id,highway_id,roadwaytype_id,cur_lanes,CASE 
		WHEN cur_maxspeed BETWEEN 0 AND 30 THEN 'Low'
		WHEN cur_maxspeed BETWEEN 31 AND 45 THEN 'Mid'
		WHEN cur_maxspeed > 45 THEN 'High'
	END AS cur_maxspeed,cur_final_place,volume FROM tm_new_data.nys_hour_8_volume_240321 nhv 
) foo
,gis_tables.nys_osm_hvc_data_240430 nohd 
WHERE foo.isg_osm_id =nohd.isg_osm_id 

---------------------------------------------------conn_vol_pred_FC_1_2_3-----------------------------------------------------------

select distinct foo.*,
cur.areatype as cur_areatype,conn.areatype as next_areatype,
cur.capacity as cur_capacity,conn.capacity as next_capacity
from
(select distinct 
isg_osm_id ,next_isg_osm_id,
highway_id, next_highway_id, 
region, next_region,
roadwaytype_id, next_roadwaytype_id,
next_connected_count, next_pcc, 
cur_lanes, next_lanes, 
case when cur_lanes = 1 then 1 else 2 end as cur_lanes_cat,
case when next_lanes = 1 then 1 else 2 end as next_lanes_cat,
cur_maxspeed, next_maxspeed, 
case
    when cur_maxspeed between 0 and 30 then 1
    when cur_maxspeed between 31 and 45 then 2
    when cur_maxspeed > 45 then  3
end as cur_spd_cat,
case
    when next_maxspeed between 0 and 30 then 1
    when next_maxspeed between 31 and 45 then 2
    when next_maxspeed > 45 then  3
end as next_spd_cat,
cur_final_place, next_final_place, 
volume, next_volume 
from tm_new_data.nys_hour_8_volume_240321 
where next_fc in(1,2,3) and volume >0 and(next_connected_count > 1 or next_pcc > 1)
union
select distinct
isg_osm_id ,prev_isg_osm_id,
highway_id, prev_highway_id, 
region, prev_region,
roadwaytype_id, prev_roadwaytype_id, 
prev_connected_count, prev_ncc, 
cur_lanes, prev_lanes, 
case when cur_lanes = 1 then 1 else 2 end as cur_lanes_cat,
case when prev_lanes = 1 then 1 else 2 end as prev_lanes_cat,
cur_maxspeed, prev_maxspeed, 
case
    when cur_maxspeed between 0 and 30 then 1
    when cur_maxspeed between 31 and 45 then 2
    when cur_maxspeed > 45 then  3
end as cur_spd_cat,
case
    when prev_maxspeed between 0 and 30 then 1
    when prev_maxspeed between 31 and 45 then 2
    when prev_maxspeed > 45 then  3
end as prev_spd_cat,
cur_final_place, prev_final_place, 
volume, prev_volume 
from tm_new_data.nys_hour_8_volume_240321 
where prev_fc in(1,2,3) and volume >0 and(prev_connected_count > 1 or prev_ncc > 1)
)foo,
gis_tables.nys_osm_hvc_data_240430 cur,
gis_tables.nys_osm_hvc_data_240430 conn
where cur.isg_osm_id = foo.isg_osm_id 
and conn.isg_osm_id = foo.next_isg_osm_id


SELECT * FROM gis_tables.nys_osm_hvc_data_240430 nohd 


-------------------------------------------------Blending------------------------------------------------------------------------------------

select row_number()over(order by isg_osm_id,next_isg_osm_id) as row_num, * from(select distinct foo.*,
cur.areatype as cur_areatype,conn.areatype as next_areatype,
cur.capacity::int as cur_capacity,conn.capacity::int as next_capacity
from
(select distinct 
isg_osm_id ,next_isg_osm_id,
highway_id, next_highway_id, 
region, next_region,
roadwaytype_id, next_roadwaytype_id, 
next_connected_count, next_pcc, 
cur_lanes, next_lanes, 
case when cur_lanes = 1 then 1 else 2 end as cur_lanes_cat,
case when next_lanes = 1 then 1 else 2 end as next_lanes_cat,
cur_maxspeed, next_maxspeed, 
case
    when cur_maxspeed between 0 and 30 then 1
    when cur_maxspeed between 31 and 45 then 2
    when cur_maxspeed > 45 then  3
end as cur_spd_cat,
case
    when next_maxspeed between 0 and 30 then 1
    when next_maxspeed between 31 and 45 then 2
    when next_maxspeed > 45 then  3
end as next_spd_cat,
cur_final_place, next_final_place, 
volume, next_volume 
from tm_new_data.nys_hour_8_volume_240321 
where next_fc in(1,2,3) and volume >0 and(next_connected_count > 1 or next_pcc > 1)
union
select distinct
isg_osm_id ,prev_isg_osm_id,
highway_id, prev_highway_id, 
region, prev_region,
roadwaytype_id, prev_roadwaytype_id, 
prev_connected_count, prev_ncc, 
cur_lanes, prev_lanes, 
case when cur_lanes = 1 then 1 else 2 end as cur_lanes_cat,
case when prev_lanes = 1 then 1 else 2 end as prev_lanes_cat,
cur_maxspeed, prev_maxspeed, 
case
    when cur_maxspeed between 0 and 30 then 1
    when cur_maxspeed between 31 and 45 then 2
    when cur_maxspeed > 45 then  3
end as cur_spd_cat,
case
    when prev_maxspeed between 0 and 30 then 1
    when prev_maxspeed between 31 and 45 then 2
    when prev_maxspeed > 45 then  3
end as prev_spd_cat,
cur_final_place, prev_final_place, 
volume, prev_volume 
from tm_new_data.nys_hour_8_volume_240321 
where prev_fc in(1,2,3) and volume >0 and(prev_connected_count > 1 or prev_ncc > 1)
)foo,
gis_tables.nys_osm_hvc_data_240430 cur,
gis_tables.nys_osm_hvc_data_240430 conn
where cur.isg_osm_id = foo.isg_osm_id 
and conn.isg_osm_id = foo.next_isg_osm_id)foo


------------------------------Pre-Processing--------------------------------------------


SELECT DISTINCT isg_osm_id,array_agg(conn_isg_osm_id)OVER ( PARTITION BY isg_osm_id) AS conn_osm_id,volume,
array_agg(conn_volume)OVER ( PARTITION BY isg_osm_id) AS conn_vol ,sum(conn_volume) OVER ( PARTITION BY isg_osm_id) AS agg_volume,
cur_lanes,array_agg(conn_lanes)OVER ( PARTITION BY isg_osm_id) AS conn_lanes,sum(conn_lanes) OVER ( PARTITION BY isg_osm_id) AS agg_lanes
FROM (
	SELECT DISTINCT isg_osm_id,next_isg_osm_id AS conn_isg_osm_id,
	cur_fc,next_fc AS conn_fc,
	highway_id ,next_highway_id AS conn_highway_id,
	countyid,next_countyid AS conn_countyid,
	region ,next_region AS conn_region,
	roadwaytype_id, next_roadwaytype_id as conn_roadwaytype_id,
	next_connected_count AS conn_count,next_pcc AS conn_ncc_pcc,
	cur_lanes,next_lanes AS conn_lanes,
	cur_maxspeed,next_maxspeed AS conn_maxspeed,
	cur_final_place ,next_final_place AS conn_final_place,
	volume,next_volume AS conn_volume,
	'next' AS conn_tag
	FROM tm_new_data.nys_hour_8_volume_240321
	WHERE next_fc IN(1,2,3) AND volume >0 AND(next_connected_count > 1 OR next_pcc > 1)
	UNION 
	SELECT DISTINCT isg_osm_id, prev_isg_osm_id, 
		cur_fc, prev_fc, 
		highway_id, prev_highway_id, 
		countyid, prev_countyid,
		region, prev_region, 
		roadwaytype_id, prev_roadwaytype_id,
		prev_connected_count, prev_ncc,  
		cur_lanes, prev_lanes, 
		cur_maxspeed, prev_maxspeed,
		cur_final_place, prev_final_place,
		volume, prev_volume,
		'prev' as conn_tag
	FROM tm_new_data.nys_hour_8_volume_240321 
	WHERE prev_fc IN (1,2,3) AND (prev_connected_count >1 OR  prev_ncc > 1)
)foo
WHERE volume>0 AND conn_volume>0
ORDER BY isg_osm_id



