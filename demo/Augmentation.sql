----------------------------------------Augmentation--24/05/24--------------------------------------------------
--ADD Augmented Volume 
--create table tm_new_data.nys_hour_8_volume_240524_new AS
select nhv.isg_osm_id,
nhv.next_isg_osm_id,
nhv.prev_isg_osm_id,
nhv.highway_group_id,
nhv.next_highway_group_id,
nhv.prev_highway_group_id,
nhv.cur_fc, nhv.next_fc, nhv.prev_fc,nhv.highway_id, nhv.next_highway_id, nhv.prev_highway_id,
nhv.countyid,
nhv.next_countyid,
nhv.prev_countyid,
nhv.region,
nhv.next_region,
nhv.prev_region,
nhv.roadwaytype_id,
nhv.next_roadwaytype_id,
nhv.prev_roadwaytype_id,
nhv.next_connected_count,
nhv.prev_connected_count,nhv.next_ncc, nhv.next_pcc, nhv.prev_ncc,nhv.prev_pcc, nhv.cur_lanes, nhv.next_lanes, nhv.prev_lanes,
nhv.cur_maxspeed, nhv.next_maxspeed, nhv.prev_maxspeed, nhv.cur_length, nhv.next_length, nhv.prev_length, nhv.cur_final_place, nhv.next_final_place, nhv.prev_final_place,
coalesce(aug_cur.volume::int,nhv.volume) as volume,
coalesce(aug_next.volume::int,nhv.next_volume) as next_volume,
coalesce(aug_prev.volume::int,nhv.prev_volume) as prev_volume,
nhv.geom,
coalesce(aug_cur."source",nhv."source") as source,
coalesce(aug_cur.original_ids,nhv.original_ids ) as original_ids,
--coalesce(aug_cur.processed_ids,nhv.processed_ids) as processed_ids,
coalesce(aug_cur.single_filled,nhv.single_filled) as single_filled,
--coalesce(aug_cur.multi_filled,nhv.multi_filled ) as multi_filled,
coalesce(aug_cur.iteration,nhv.iteration) as iteration
from 
tm_new_data.nys_hour_8_volume_240524 nhv 
left join
(select * from tm_new_data.nys_augmentation_hour_8_volume_231211 where "source" in('RMBA')) aug_cur
on aug_cur.isg_osm_id = nhv.isg_osm_id 
left join
(select * from tm_new_data.nys_augmentation_hour_8_volume_231211 where "source" in('RMBA')) aug_next
on aug_next.isg_osm_id = nhv.next_isg_osm_id 
left join
(select * from tm_new_data.nys_augmentation_hour_8_volume_231211 where "source" in('RMBA')) aug_prev
on aug_prev.isg_osm_id = nhv.prev_isg_osm_id



-----------------------------------------------------Inserting Augmented values------------------------------------------------------------------

--INSERT INTO tm_new_data.nys_hour_8_volume_240524

--CREATE  TABLE tm_new_data.nys_hour_8_volume_240524 AS
WITH cte AS (
SELECT nhv.isg_osm_id, nhv.next_isg_osm_id ,nhv.prev_isg_osm_id ,nhv.highway_group_id ,nhv.next_highway_group_id ,
nhv.prev_highway_group_id ,nhv.cur_fc ,nhv.next_fc ,nhv.prev_fc ,nhv.highway_id ,nhv.next_highway_id ,nhv.prev_highway_id ,
nhv.countyid ,nhv.next_countyid ,nhv.prev_countyid ,nhv.region ,nhv.next_region ,nhv.prev_region ,nhv.roadwaytype_id ,nhv.next_roadwaytype_id ,nhv.prev_roadwaytype_id ,
nhv.next_connected_count ,nhv.prev_connected_count ,nhv.next_ncc ,nhv.next_pcc,nhv.prev_ncc,nhv.prev_pcc,nhv.cur_lanes ,nhv.next_lanes ,nhv.prev_lanes,nhv.cur_maxspeed ,
nhv.next_maxspeed ,nhv.prev_maxspeed ,nhv.cur_length ,nhv.next_length ,nhv.prev_length ,nhv.cur_final_place ,nhv.next_final_place ,nhv.prev_final_place,
COALESCE (aug_cur.volume::int,nhv.volume) AS volume,
COALESCE (aug_next.volume::int,nhv.next_volume) as next_volume,
COALESCE (aug_prev.volume::int,nhv.prev_volume) as prev_volume,
nhv.geom,
COALESCE(aug_cur."source",nhv."source") AS source,
COALESCE(aug_cur.original_ids,nhv.original_ids ) AS original_ids,
COALESCE(aug_cur.single_filled,nhv.single_filled) as single_filled, 
COALESCE(aug_cur.iteration,nhv.iteration) as iteration
FROM   
tm_new_data.nys_hour_8_volume_240524 nhv 
LEFT JOIN 
(SELECT * FROM tm_new_data.nys_augmentation_hour_8_volume_231211 nahv WHERE "source" IN ('RMBA') ) aug_cur
ON  aug_cur.isg_osm_id=nhv.isg_osm_id 
LEFT JOIN 
(SELECT * FROM tm_new_data.nys_augmentation_hour_8_volume_231211 nahv WHERE "source" IN ('RMBA') ) aug_next
ON aug_next.isg_osm_id = nhv.next_isg_osm_id 
LEFT JOIN
(SELECT * FROM tm_new_data.nys_augmentation_hour_8_volume_231211 nahv WHERE "source" IN ('RMBA') ) aug_prev
ON aug_prev.isg_osm_id = nhv.prev_isg_osm_id 
)
SELECT * FROM cte 
  
--DROP TABLE tm_new_data.nys_hour_8_volume_240524

--ALTER TABLE tm_new_data.nys_hour_8_volume_240524_new RENAME TO nys_hour_8_volume_240524

CREATE INDEX  ON tm_new_data.nys_hour_8_volume_240524 USING btree (isg_osm_id);
CREATE INDEX  ON tm_new_data.nys_hour_8_volume_240524 USING btree (next_isg_osm_id);
CREATE INDEX  ON tm_new_data.nys_hour_8_volume_240524 USING btree (prev_isg_osm_id);
CREATE INDEX  ON tm_new_data.nys_hour_8_volume_240524 USING btree (next_connected_count);
CREATE INDEX  ON tm_new_data.nys_hour_8_volume_240524 USING btree (prev_connected_count);

SELECT * FROM tm_new_data.nys_hour_8_volume_240524 nhv WHERE volume>0  

SELECT * FROM tm_new_data.nys_hour_8_volume_240321 nhv WHERE volume>0

SELECT * FROM tm_new_data.nys_hour_8_volume_240524 nhv WHERE iteration=2

SELECT * FROM tm_new_data.ny_connected_osm_master_231211

SELECT DISTINCT isg_osm_id,highway_id ,region ,volume  FROM tm_new_data.nys_hour_8_volume_240524 WHERE volume>0

-------------------------------------------------------MULTI-------------------------------------------------------------------------------------------
SELECT DISTINCT isg_osm_id,highway_id,volume,"source",multi_next_ids AS connected_ids FROM (
	SELECT A.*,B.multi_next_ids,B.single_next_ids FROM (
		SELECT * FROM tm_new_data.nys_hour_8_volume_240626 nhv WHERE volume>0
	)A,tm_new_data.ny_connected_osm_master_231211 B
	WHERE A.isg_osm_id=B.current_id AND multi_next_ids!='{}' AND single_next_ids='{}'
)foo
WHERE multi_next_ids!='{}' AND single_next_ids='{}'

----------------------------------------------------------------------------------------------------------------------------------------------------

SELECT * FROM gis_tables.nys_hour_8_volume

SELECT DISTINCT isg_osm_id,prev_volume ,prev_isg_osm_id,prev_connected_count,prev_ncc 
FROM tm_new_data.nys_hour_8_volume_240524 WHERE volume = -1 AND prev_connected_count > 1 AND prev_volume>0
--GROUP BY isg_osm_id,prev_volume ,prev_isg_osm_id,prev_connected_count,prev_ncc
--HAVING count(DISTINCT prev_isg_osm_id)=sum(DISTINCT prev_connected_count) AND sum(DISTINCT prev_ncc)=1 

select distinct isg_osm_id,sum(prev_volume) as sum_volume,array_agg(prev_isg_osm_id) prev_ids
from
    (select distinct isg_osm_id,prev_volume ,prev_isg_osm_id,prev_connected_count,prev_ncc 
    from tm_new_data.nys_hour_8_volume_240524 where volume = -1 and prev_connected_count > 1)foo 
where prev_volume != -1 group by isg_osm_id  having count(distinct prev_isg_osm_id) = sum(distinct prev_connected_count) and sum(distinct prev_ncc) = 1

    
-----------------------------------------Single Connected GEH calculation--------------------------------------------------------------


SELECT *,tm.geh_check(volume::int,single.conn_vol_array[1]::int) AS geh FROM (
	SELECT DISTINCT cur.isg_osm_id, ARRAY[cur.next_isg_osm_id] AS conn_isg_osm_array,cur.next_connected_count AS connected_count,cur.source,ARRAY[conn.SOURCE] AS conn_src_array,cur.volume,
	ARRAY[cur.next_volume] AS conn_vol_array,cur.next_volume AS total_conn_vol,'single' AS conn_tag,'Orginal' AS iteration
	FROM tm_new_data.nys_hour_8_volume_240626 cur,tm_new_data.nys_hour_8_volume_240626 conn
	WHERE cur.isg_osm_id=conn.isg_osm_id
	AND cur.volume>0 AND cur.next_volume >0  AND cur.next_connected_count =1 AND cur.next_pcc=1
)single



-----------------------------------------Multi Connected GEH calculation-------------------------------------------------------------------------------

SELECT *,tm.geh_check(volume::int,total_conn_vol::int) AS geh FROM (
	SELECT DISTINCT isg_osm_id,array_agg(next_isg_osm_id)OVER (PARTITION BY isg_osm_id) AS conn_isg_osm_array ,connected_count,"source",
	array_agg("next_source") OVER (PARTITION BY isg_osm_id) AS conn_src_array,volume,
	array_agg(next_volume) OVER (PARTITION BY isg_osm_id) AS conn_vol_array,
	sum(next_volume) OVER (PARTITION BY isg_osm_id) AS total_conn_vol,'multi_next' AS conn_tag,'Orginal' AS iteration
	FROM (
		SELECT DISTINCT cur.isg_osm_id, cur.next_isg_osm_id,cur.next_connected_count AS connected_count,cur.source,conn.source as next_source,cur.volume, cur.next_volume,cur.iteration  
		FROM tm_new_data.nys_hour_8_volume_240626 cur,tm_new_data.nys_hour_8_volume_240626 conn
		WHERE cur.isg_osm_id=conn.isg_osm_id
		AND cur.volume>0 AND cur.next_volume >0  AND cur.next_connected_count > 1 AND cur.next_pcc=1 
	)multi_next
	UNION
	SELECT DISTINCT isg_osm_id,array_agg(prev_isg_osm_id)OVER (PARTITION BY isg_osm_id) AS conn_isg_osm_array ,connected_count,"source",
	array_agg("prev_source") OVER (PARTITION BY isg_osm_id) AS conn_src_array,volume,
	array_agg(prev_volume) OVER (PARTITION BY isg_osm_id) AS conn_vol_array,
	sum(prev_volume) OVER (PARTITION BY isg_osm_id) AS total_conn_vol,'multi_prev' AS conn_tag,'Orginal' AS iteration
	FROM (
		SELECT DISTINCT cur.isg_osm_id, cur.prev_isg_osm_id,cur.prev_connected_count AS connected_count,cur.source,conn.source as prev_source,cur.volume, cur.prev_volume,cur.iteration  
		FROM tm_new_data.nys_hour_8_volume_240626 cur,tm_new_data.nys_hour_8_volume_240626 conn
		WHERE cur.isg_osm_id=conn.isg_osm_id
		AND cur.volume>0 AND cur.prev_volume >0  AND cur.prev_connected_count > 1 AND cur.prev_ncc=1 
	)multi_prev
)multi
--WHERE CARDINALITY(conn_vol_array)=connected_count

------------------------------------------------------------------------------------------------------------

SELECT DISTINCT * FROM tm_new_data.nys_hour_8_multi_single_conn nhmsc WHERE iteration=2 AND "source" ='MFA' ORDER BY geh DESC


WHERE connected_count=array_length(conn_src_array,1) AND geh>25 --conn_count>=2 AND conn_count >array_length(array_agg,1) AND array_length(array_agg,1)>1 -- AND geh >25



SELECT * FROM tm_new_data.nys_hour_8_volume_240521_xgb_unseen_adjancent_geh_check WHERE geh > 25 AND isg_osm_id IN (
	SELECT isg_osm_id FROM tm_new_data.nys_hour_8_volume_050624_rf_prev_OHE_unseen_adjancent_geh_check WHERE geh<5
)

SELECT * FROM tm_new_data.nys_hour_8_volume_050624_rf_prev_OHE_unseen_adjancent_geh_check 

SELECT * FROM tm_new_data.nys_hour_8_volume_050624_rf_prev_OHE_unseen_adjancent_geh_check WHERE isg_osm_id IN (
	SELECT isg_osm_id FROM tm_new_data.nys_hour_8_volume_240521_xgb_unseen_adjancent_geh_check WHERE geh > 25 AND isg_osm_id IN (
		SELECT isg_osm_id FROM tm_new_data.nys_hour_8_volume_050624_rf_prev_OHE_unseen_adjancent_geh_check WHERE geh<5
	)
)
AND (SOURCE IN ('ML') OR 'ML'=ANY (conn_src_array))


SELECT * FROM tm_new_data.nys_hour_8_volume_050624_rf_prev_OHE_unseen_adjancent_geh_check WHERE isg_osm_id IN (1046460982001)

SELECT * FROM tm_new_data.nys_hour_8_volume_240521_xgb_unseen_adjancent_geh_check WHERE isg_osm_id IN (684436068001)

------old  working
SELECT * FROM tm_new_data.nys_hour_8_volume_240521_xgb_unseen_adjancent_geh_check WHERE geh<5
AND (SOURCE NOT IN ('ML') AND 'ML'=ANY(conn_src_array)) AND connected_count >1


SELECT * FROM tm_new_data.osm_ris_master_bidir_230622 ormb WHERE isg_osm_id IN (247570669002)

SELECT * FROM tm_new_data.osm_atr_master_bidir_221124 oamb WHERE osmid IN (247570669002)

SELECT * FROM tm_new_data.nys_hour_8_volume_rf_prev_OHE_unseen_adjancent_geh_check_240611 WHERE geh>25 --AND connected_count>1
AND ("source" IN ('ML') OR 'ML'=ANY(conn_src_array))

SELECT * FROM tm_new_data.osm_roadtype_master_231211 orm WHERE roadwaytype_id=13

WITH cte AS (
	SELECT DISTINCT vol.*,cap_cur.capacity,cap_next.capacity AS next_capacity ,
	cap_prev.capacity  AS prev_capacity,vol.prev_volume
	FROM tm_new_data.rf_unseen_adjacent_240611 vol 
	LEFT JOIN gis_tables.nys_osm_hvc_data_240430 cap_cur ON vol.isg_osm_id =cap_cur.isg_osm_id 
	LEFT JOIN gis_tables.nys_osm_hvc_data_240430 cap_next ON vol.next_isg_osm_id=cap_next.isg_osm_id 
	LEFT JOIN gis_tables.nys_osm_hvc_data_240430 cap_prev ON vol.prev_isg_osm_id=cap_prev.isg_osm_id
	WHERE vol.isg_osm_id IN (
		SELECT isg_osm_id FROM tm_new_data.nys_hour_8_volume_rf_prev_OHE_unseen_adjancent_geh_check_240611 WHERE geh>25 --AND connected_count>1
	)
)
SELECT * FROM cte

SELECT DISTINCT isg_osm_id FROM cte WHERE roadwaytype_id=16 OR next_roadwaytype_id=16 OR prev_roadwaytype_id=16

SELECT DISTINCT isg_osm_id FROM cte WHERE (next_connected_count=1 AND prev_connected_count=2) OR (next_connected_count=2 AND prev_connected_count=1)


SELECT tm.geh_check(3184,3345)

SELECT * FROM tm_new_data.nys_hour_8_volume_rf_prev_OHE_unseen_adjancent_geh_check_240611 WHERE geh>25 AND connected_count>1 
AND ("source" IN ('ML') OR 'ML'=ANY(conn_src_array))

SELECT 4251+577

SELECT * FROM gis_tables.hour_8_volume_fc_1_2_3_rf_ohe_next_240611 hvfron 

SELECT * FROM tm_new_data."Unseen_Prediction_new_approch_OHE_240611"

SELECT * FROM tm_new_data.prev_cur_model_data_240611
WHERE prev_connected_count >1  AND prev_volume >0 AND volume<0 AND CARDINALITY(prev_vol_filter_arr)>1

--------------------------Good Scenarios---------------
32131901002




SELECT a.isg_osm_id ,a.conn_link_array ,a.connected_count ,a."source" ,a.conn_src_array ,a.volume ,b.volume  AS rf_volume,
a.total_conn_vol ,b.total_conn_vol  AS rf_total_conn_volume,a.conn_vol_array ,b.conn_vol_array  AS rf_conn_vol_array,a.conn_tag ,a.geh,b.geh AS rf_geh 
FROM tm_new_data.nys_hour_8_xgb_ohe_adjacent_geh_check_240619 a , tm_new_data.nys_hour_8_volume_rf_prev_OHE_unseen_adjancent_geh_check_240611 b
WHERE a.isg_osm_id =b.isg_osm_id AND (a.geh>25 OR b.geh>25) AND a.connected_count > 1

SELECT count(DISTINCT isg_osm_id) FROM tm_new_data.nys_hour_8_xgb_prev_ohe_unseen_predictions_240619

SELECT count(DISTINCT isg_osm_id) FROM tm_new_data."Unseen_Prediction_new_approch_OHE_240611"

SELECT * FROM tm_new_data.nys_hour_8_xgb_ohe_240619

SELECT * FROM tm_new_data.nys_hour_8_xgb_ohe_adjacent_geh_check_240619 WHERE geh>25 --AND CARDINALITY (conn_vol_array)=connected_count 

SELECT * FROM tm_new_data.nysdot_sc_combined_data_230414 nscd 

SELECT * FROM tm_new_data.osm_ris_master_bidir_230622_conf_changes_240624 ormbcc ,tm_new_data.sc_combined_data_with_osm_231109_wd_geh_lvl1 scdwowgl 
WHERE ormbcc.rc_station =scdwowgl.rc_station 

SELECT * FROM  tm_new_data.tm_clean_data_wd_231109

----------------------------------------------osm_county mapping---------------------
--create table tm_new_data.nys_osm_county_mapping as
with county_table as(
select distinct  osm.isg_osm_id,countyid 
from  osm_master.osm_city_county_state_mapping cnt,
osm_master.street_route_osm_bidir_extracted osm
where statecode  = 'NY' and state = 'NY' and osm.ori_isg_osm_id = cnt.isg_osm_id)
select * from county_table where isg_osm_id in(
select isg_osm_id from county_table group by isg_osm_id having count(distinct countyid)=1)


with county_table as(
select distinct  osm.isg_osm_id,countyid 
from  osm_master.osm_city_county_state_mapping cnt,
osm_master.street_route_osm_bidir_extracted osm
where statecode  = 'NY' and state = 'NY' and osm.ori_isg_osm_id = cnt.isg_osm_id)
insert into tm_new_data.nys_osm_county_mapping
select distinct isg_osm_id,countyid from osm_master.osm_city_county_state_link_geohash_mapping where isg_osm_id in(
select distinct isg_osm_id from osm_master.osm_city_county_state_link_geohash_mapping where isg_osm_id in(
select isg_osm_id from county_table group by isg_osm_id having count(distinct countyid)>1) and statecode = 'NY' group by isg_osm_id having count(distinct countyid)=1)

--insert into tm_new_data.nys_osm_county_mapping
select isg_osm_id,countyid from(
select *, row_number()over(partition by isg_osm_id order by intersection_length desc) as raw_num from(
with multy_county_table as
(with county_table as(
select distinct  osm.isg_osm_id,countyid 
from  osm_master.osm_city_county_state_mapping cnt,
osm_master.street_route_osm_bidir_extracted osm
where statecode  = 'NY' and state = 'NY' and osm.ori_isg_osm_id = cnt.isg_osm_id)
select distinct isg_osm_id,countyid, st_setsrid(st_geomfromewkt(geom_wkt),4326) as geom from osm_master.osm_city_county_state_link_geohash_mapping where isg_osm_id in(
select distinct isg_osm_id from osm_master.osm_city_county_state_link_geohash_mapping where isg_osm_id in(
select isg_osm_id from county_table group by isg_osm_id having count(distinct countyid)>1) and statecode = 'NY' group by isg_osm_id having count(distinct countyid)>1))
select distinct mct.isg_osm_id,mct.geom,carto.countyid, carto.geom,ST_Length(ST_Intersection(carto.geom,mct.geom),true) as intersection_length from multy_county_table mct,
osm_master.carto_city_county_state_master carto
where mct.countyid = carto.countyid and st_intersects(carto.geom, mct.geom))foo)foo where raw_num = 1


--insert into tm_new_data.nys_osm_county_mapping
select distinct isg_osm_id ,countyid,st_setsrid(st_geomfromewkt(geom_wkt),4326) from osm_master.osm_city_county_state_link_geohash_mapping
 where statecode = 'NY' and isg_osm_id in(
select isg_osm_id from osm_master.osm_city_county_state_link_geohash_mapping where statecode = 'NY' and countyid not between 33 and 94
except 
select isg_osm_id from tm_new_data.nys_osm_county_mapping
)
and isg_osm_id in(select current_id from tm_new_data.prev_next_group_id_raw_data_231206 pngird)


--insert into tm_new_data.nys_osm_county_mapping
select distinct isg_osm_id ,carto.countyid from osm_master.osm_city_county_state_link_geohash_mapping mct,
osm_master.carto_city_county_state_master carto
where
mct.statecode = 'NY' and carto.statecode = 'NY' and isg_osm_id in(
select isg_osm_id from osm_master.osm_city_county_state_link_geohash_mapping where statecode = 'NY' and countyid not between 33 and 94
except 
select isg_osm_id from tm_new_data.nys_osm_county_mapping
)
and isg_osm_id in(select current_id from tm_new_data.prev_next_group_id_raw_data_231206 pngird) 
and st_dwithin(carto.geom,st_setsrid(st_geomfromewkt(mct.geom_wkt),4326),0.0009)

SELECT *,
CASE 
	WHEN volume>0 THEN 'Original' ELSE NULL 
END AS "source" , 
CASE 
	WHEN volume>0 THEN 0 ELSE NULL
END AS iteration,
CAST (NULL AS int8)  AS original_ids,
NULL AS single_filled
FROM tm_new_data.nys_hour_8_volume_original_240626

SELECT * FROM tm_new_data.nys_hour_8_volume_original_240626

SELECT * FROM pg_catalog.pg_stat_activity at
WHERE state ='active'--pid IN (10194,9130) 


SELECT pg_terminate_backend(528) 

select distinct isg_osm_id as removal_ids_next_geh from tm_new_data.nys_hour_8_volume_240626_adjacent_geh_check nhvagc 
where conn_tag in('single','multi_next')

select distinct isg_osm_id,next_connected_count,source,highway_id,volume,single_next_ids,
                    multi_next_ids,single_prev_ids,multi_prev_ids  from
                    (select A.*,B.* from 
                        (select * from tm_new_data.nys_hour_8_volume_240626 where volume > 0 and next_pcc = 1) A
                        ,tm_new_data.ny_connected_osm_master_231211 B
                    where A.isg_osm_id = B.current_id 
                    )foo
UNION 
select distinct isg_osm_id,next_connected_count,source,highway_id,volume,single_next_ids,
                    multi_next_ids,single_prev_ids,multi_prev_ids  from
                    (select A.*,B.* from 
                        (select * from tm_new_data.nys_hour_8_volume_240626 where volume > 0 and next_pcc = 1) A
                        ,tm_new_data.ny_connected_osm_master_231211 B
                    where A.isg_osm_id = B.current_id 
                    )foo

select distinct conn_isg_osm_array[1] as removal_ids_prev_geh from tm_new_data.nys_hour_8_volume_240626_adjacent_geh_check where conn_tag = 'single'
union
select distinct isg_osm_id as removal_ids_prev_geh from tm_new_data.nys_hour_8_volume_240626_adjacent_geh_check
where source = 'multi_prev'


select distinct isg_osm_id,highway_id,volume,source,
                         single_next_ids as connected_ids from
                    (select A.*,B.single_next_ids from 
                        (select * from tm_new_data.nys_hour_8_volume_240626 where volume > 0 and next_volume < 0 ) A
                        ,tm_new_data.ny_connected_osm_master_231211 B
                    where A.isg_osm_id = B.current_id
                    )foo where single_next_ids !='{}'
UNION 
select distinct isg_osm_id,highway_id,volume,source,
                         single_prev_ids as connected_ids from
                    (select A.*,B.single_prev_ids from 
                        (select * from tm_new_data.nys_hour_8_volume_240626 where volume > 0 and prev_volume < 0 ) A
                        ,tm_new_data.ny_connected_osm_master_231211 B
                    where A.isg_osm_id = B.current_id
                    )foo where single_prev_ids !='{}'
                    
SELECT * FROM tm_new_data.ny_connected_osm_master_231211



SELECT * FROM tm_new_data.nys_hour_8_volume_240620_xgb_unseen_adjancent_geh_check WHERE geh>25

SELECT * FROM tm_new_data.nys_hour_8_volume_240620_xgb WHERE SOURCE ='ML'


----------------------------------------------------MUlti GEH Calcalution after Conflation changes---------------------------------------------------------------------

--CREATE TABLE tm_new_data.nys_hour_8_volume_240626_adjacent_geh_check AS 
--INSERT INTO tm_new_data.nys_hour_8_volume_240626_adjacent_geh_check
SELECT *,tm.geh_check(volume::int,total_conn_vol::int) AS geh FROM (
    SELECT DISTINCT isg_osm_id,array_agg(next_isg_osm_id)OVER (PARTITION BY isg_osm_id) AS conn_isg_osm_array ,connected_count,"source",
    array_agg("next_source") OVER (PARTITION BY isg_osm_id) AS conn_src_array,volume,
    sum(next_volume) OVER (PARTITION BY isg_osm_id) AS total_conn_vol,
    array_agg(next_volume) OVER (PARTITION BY isg_osm_id) AS conn_vol_array,
    'multi_next' AS conn_tag,'Single_Aug_1' as iteration
    FROM (
        SELECT DISTINCT cur.isg_osm_id, cur.next_isg_osm_id,cur.next_connected_count AS connected_count,cur.source,conn.source as next_source,cur.volume, cur.next_volume,cur.iteration  
        FROM tm_new_data.nys_hour_8_volume_240626 cur,tm_new_data.nys_hour_8_volume_240626 conn
        WHERE cur.next_isg_osm_id=conn.isg_osm_id
        AND cur.volume>0 AND cur.next_volume >0  AND cur.next_connected_count > 1 AND cur.next_pcc=1 AND cur.SOURCE='MBA'
    )multi_next
    UNION
    SELECT DISTINCT isg_osm_id,array_agg(prev_isg_osm_id)OVER (PARTITION BY isg_osm_id) AS conn_isg_osm_array ,connected_count,"source",
    array_agg("prev_source") OVER (PARTITION BY isg_osm_id) AS conn_src_array,volume,
    sum(prev_volume) OVER (PARTITION BY isg_osm_id) AS total_conn_vol,
    array_agg(prev_volume) OVER (PARTITION BY isg_osm_id) AS conn_vol_array,
    'multi_prev' AS conn_tag,'Single_Aug_1' as iteration
    FROM (
        SELECT DISTINCT cur.isg_osm_id, cur.prev_isg_osm_id,cur.prev_connected_count AS connected_count,cur.source,conn.source as prev_source,cur.volume, cur.prev_volume,cur.iteration  
        FROM tm_new_data.nys_hour_8_volume_240626 cur,tm_new_data.nys_hour_8_volume_240626 conn
        WHERE cur.prev_isg_osm_id=conn.isg_osm_id
        AND cur.volume>0 AND cur.prev_volume >0  AND cur.prev_connected_count > 1 AND cur.prev_ncc=1 AND cur.SOURCE='MBA'
    )multi_prev
)multi

WHERE connected_count=CARDINALITY (conn_src_array)

----------------------------------------------------Single GEH Calcalution after Conflation changes---------------------------------------------------------------------

--INSERT INTO tm_new_data.nys_hour_8_volume_240626_adjacent_geh_check
SELECT DISTINCT geh.isg_osm_id,ARRAY[next_isg_osm_id] AS conn_link_array, next_connected_count AS connected_count,
geh.source, 
ARRAY[nxt.source] AS conn_src_array,
next_volume AS total_conn_vol,
volume, ARRAY[next_volume] AS conn_vol_array, 
'single' AS conn_tag,'Single_Aug_1' AS iteration, geh
FROM
	(SELECT isg_osm_id, next_isg_osm_id,source ,next_connected_count,volume,next_volume, tm.geh_check(volume,next_volume) AS geh  
	FROM tm_new_data.nys_hour_8_volume_240626 WHERE volume>0 AND next_volume>0 AND next_connected_count =1 AND next_pcc = 1 ) geh,
(SELECT isg_osm_id,source FROM tm_new_data.nys_hour_8_volume_240626  WHERE prev_connected_count =1 ) nxt
WHERE  geh.next_isg_osm_id = nxt.isg_osm_id 


-------------------------------------------------------GIS Plotting-----------------------------------------------

--DROP TABLE IF EXISTS gis_tables.hour_8_volume_xgb_geh_lt_25;
--CREATE TABLE gis_tables.hour_8_volume_xgb_geh_lt_25 AS 
SELECT DISTINCT vol.isg_osm_id,cur_fc,highway_id,countyid,region,roadwaytype_id,
next_connected_count,prev_connected_count,cur_lanes,cur_maxspeed,cur_final_place,vol.volume,vol."source",geom
from tm_new_data.nys_hour_8_volume_240620_xgb vol LEFT JOIN tm_new_data.nys_hour_8_volume_240620_xgb_unseen_adjancent_geh_check geh
ON vol.isg_osm_id =geh.isg_osm_id 
WHERE vol.volume >0
AND (geh.geh<25 OR geh.geh IS NULL )

CREATE INDEX ON gis_tables.hour_8_volume_xgb_geh_lt_25 (isg_osm_id)

select tm.geh_check(2755,2255)

-------------------------------------------------------------------------------------------------------------------------------------------------------


SELECT * FROM tm_new_data.nys_hour_8_volume_240626_adjacent_geh_check nhvagc --WHERE   geh>25 AND connected_count =CARDINALITY(conn_src_array) --AND conn_tag!='single'

SELECT * FROM tm_new_data.nys_hour_8_volume_240626 nhv WHERE "source" = 'MBA' 


SELECT * FROM tm_new_data.nys_hour_8_volume_240620_xgb_unseen_adjancent_geh_check WHERE geh>25 --AND connected_count=CARDINALITY(conn_src_array)

SELECT * FROM tm_new_data.nys_hour_8_volume_240620_xgb WHERE isg_osm_id  NOT IN (
	SELECT isg_osm_id FROM tm_new_data.nys_hour_8_volume_240620_xgb_unseen_adjancent_geh_check WHERE geh>25
)

SELECT * FROM tm_new_data.nys_hour_8_volume_240620_xgb WHERE isg_osm_id  IN (
	SELECT isg_osm_id FROM tm_new_data.nys_hour_8_volume_240620_xgb_unseen_adjancent_geh_check WHERE geh<25
)



-----------------------------------------------------------------------------------------------------------------------------------------


SELECT isg_osm_id,SOURCE,volume,original_ids,iteration FROM  
(with single_augmentation
as
((
  WITH first_match_position AS 
(
   select a.isg_osm_id,a.highway_id,a.volume,a.source,a.tag,a.connected_ids,
   COALESCE(
            (SELECT idx FROM unnest(a.connected_ids) WITH ORDINALITY AS u(value, idx)
             WHERE value IN (SELECT isg_osm_id FROM tm_new_data.nys_hour_8_volume_240626 WHERE volume > 0 ) ORDER BY idx LIMIT 1
             ),array_length(a.connected_ids, 1) + 1
           ) AS first_match_idx
    FROM
	(
		SELECT DISTINCT isg_osm_id,highway_id,volume,source,'next' AS tag,single_next_ids as connected_ids 
		FROM 
		(
		    SELECT A.*,B.single_next_ids FROM (SELECT * FROM tm_new_data.nys_hour_8_volume_240626 WHERE volume > 0 AND next_volume < 0 ) A,
		    tm_new_data.ny_connected_osm_master_231211 B
		    WHERE A.isg_osm_id = B.current_id
		)next_id 
		WHERE CARDINALITY(single_next_ids)>0
	) a
)
SELECT
    unnest(case when first_match_idx <= array_length(connected_ids, 1) then connected_ids[1:first_match_idx - 1] else connected_ids end) as isg_osm_id,
    'SFA' as source,volume,array[isg_osm_id] as original_ids, 
    1 as iteration
FROM
    first_match_position )
union
(
 WITH first_match_position AS 
(
   select a.isg_osm_id,a.highway_id,a.volume,a.source,a.tag,a.connected_ids,
   COALESCE(
            (SELECT idx FROM unnest(a.connected_ids) WITH ORDINALITY AS u(value, idx)
             WHERE value IN (SELECT isg_osm_id FROM tm_new_data.nys_hour_8_volume_240626 WHERE volume > 0 ) ORDER BY idx LIMIT 1
             ),array_length(a.connected_ids, 1) + 1
           ) AS first_match_idx
    FROM
	(
		SELECT DISTINCT isg_osm_id,highway_id,volume,source,'prev' AS tag,single_prev_ids as connected_ids 
		FROM 
		(
		    SELECT A.*,B.single_prev_ids FROM (SELECT * FROM tm_new_data.nys_hour_8_volume_240626 WHERE volume > 0 AND prev_volume < 0 ) A,
		    tm_new_data.ny_connected_osm_master_231211 B
		    WHERE A.isg_osm_id = B.current_id
		)next_id 
		WHERE CARDINALITY(single_prev_ids)>0
	) a
)
SELECT
    unnest(case when first_match_idx <= array_length(connected_ids, 1) then connected_ids[1:first_match_idx - 1] else connected_ids end) as isg_osm_id,
    'SBA' as source,volume,array[isg_osm_id] as original_ids, 
    1 as iteration
FROM
    first_match_position
))
select * from (
select *,row_number() over(partition by isg_osm_id,source order by source desc) as priority_id 
from single_augmentation
 )foo where priority_id = 1) bar

 
 
 
