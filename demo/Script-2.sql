SELECT * FROM tm_new_data.osm_atr_master_bidir_221124 oamb LIMIT 10

SELECT * FROM tm_new_data.osm_ris_master_bidir_230622 ormb  LIMIT 10

SELECT * FROM tm_new_data.osm_mta_master_bidir_221124 ommb LIMIT 10

SELECT * FROM tm_new_data.short_counts_data LIMIT 10

SELECT * FROM tm_new_data.nys_hour_8_volume_240321 LIMIT 100

SELECT * FROM tm_new_data.ny_connected_osm_master_230829 LIMIT 100

------------------------------------------------------------------
SELECT * FROM  tm_new_data.nys_hour_8_volume_240401_xgb_unseen  
WHERE conn_isg_osm_id IN (40337385001)				
------------------------------------------------------------------

SELECT count(*),count(*) FILTER (WHERE geh > 5),count(*) FILTER (WHERE geh < 10) FROM tm_new_data.nys_hour_8_volume_240401_xgb_unseen_adjancent_geh_check


-----------------------------------volume and capacity--------------------------------------------------------------------------
with vol AS
(select distinct  isg_osm_id,next_isg_osm_id , next_connected_count, next_pcc, volume, next_volume from tm_new_data.nys_hour_8_volume_240321 nhvx 
where next_connected_count =2 and next_pcc =1 and next_volume >0 and volume>0)
select * from vol
where isg_osm_id in(select isg_osm_id from vol group by isg_osm_id having count(*) = 2) -------------------------seen data

SELECT * FROM gis_tables.nys_osm_hvc_data nohd  
WHERE isg_osm_id IN (20226325002,572765101001,20226325001)


SELECT * FROM tm_new_data.nys_hour_8_volume_240401_xgb_unseen_adjancent_geh_check nhvxuagc --------------------------------unseen data
WHERE (SOURCE = 'ML'OR 'ML' = ANY(conn_src_array))  AND conn_tag='multi_next' AND isg_osm_id IN(
	SELECT isg_osm_id FROM tm_new_data.nys_hour_8_volume_240401_xgb_unseen
	WHERE conn_count = 2 AND conn_ncc_pcc = 1
)

SELECT tm.geh_check(590,866)
---------------------------------------------------------------------------------------------------------------------------------


SELECT * FROM gis_tables.nys_osm_hvc_data nohd 
WHERE isg_osm_id IN (
	SELECT isg_osm_id  FROM tm_new_data.nys_hour_8_volume_240401_xgb_unseen_adjancent_geh_check nhvxuagc --------------------------------unseen data
	WHERE (SOURCE = 'ML'OR 'ML' = ANY(conn_src_array)) AND geh > 25 AND conn_tag='multi_next' AND isg_osm_id IN(
		SELECT isg_osm_id FROM tm_new_data.nys_hour_8_volume_240401_xgb_unseen
		WHERE conn_count = 2 AND conn_ncc_pcc = 1
	)
)


WITH cte AS (
	SELECT * FROM tm_new_data.nys_hour_8_volume_240401_xgb_unseen_adjancent_geh_check
	WHERE (source = 'ML' or  'ML' = any(conn_src_array))
	and geh>25
	ORDER BY geh DESC 
)
SELECT * FROM cte --WHERE isg_osm_id=20653720001

SELECT * FROM tm_new_data.nys_hour_8_volume_240321 
WHERE ((next_connected_count=2 AND next_pcc =1) OR (prev_connected_count=2 AND prev_ncc=1)) AND volume>0 AND next_volume >1 AND prev_volume >1

SELECT * FROM tm_new_data.nys_hour_8_volume_240401_xgb_unseen_adjancent_geh_check
WHERE (source = 'ML' or  'ML' = any(conn_src_array))


SELECT * FROM tm_new_data.nys_hour_8_volume_240401_xgb_unseen_adjancent_geh_check 
WHERE (SOURCE = 'ML' OR 'ML'=ANY(conn_src_array)) 
AND geh > 25 AND conn_tag  IN ('single') AND volume < total_conn_vol 
ORDER BY geh DESC 

SELECT * FROM  tm_new_data.nys_hour_8_volume_240401_xgb_unseen
WHERE isg_osm_id IN (374192778003)

-----------------------------------------------------------------------------------------------
SELECT * FROM tm_new_data.osm_ris_master_bidir_230622 ormb WHERE isg_osm_id IN (995972687001)

SELECT * FROM tm.ris_master_temp1 rmt 
WHERE rc_station ='43_0030'
----------------------------------------------------------------------------------------------

SELECT * FROM tm_new_data.osm_atr_master_bidir_221124 oamb WHERE osmid =669411785002

SELECT * FROM tm_new_data.tm_clean_data_wd_231109 tcdw  WHERE isg_osm_id IN (38272643003) AND "hour"=9

SELECT * FROM tm_new_data.sc_combined_data_with_osm_231109 scdwo  WHERE isg_osm_id IN (38272643003)  AND "hour"=8

SELECT * FROM tm_ss.highway_group_temp_231220

SELECT * FROM tm_new_data.highway_master_240321

SELECT a.isg_osm_id,conn_isg_osm_id,countyid,geh FROM  tm_new_data.nys_hour_8_volume_240401_xgb_unseen a INNER JOIN  tm_new_data.nys_hour_8_volume_240401_xgb_unseen_adjancent_geh_check b
ON a.isg_osm_id =b.isg_osm_id 
WHERE b.geh>25 AND a.countyid=84


SELECT DISTINCT a.isg_osm_id FROM  tm_new_data.nys_hour_8_volume_240401_xgb_unseen a INNER JOIN  tm_new_data.nys_hour_8_volume_240401_xgb_unseen_adjancent_geh_check b
ON a.isg_osm_id =b.isg_osm_id 
WHERE b.geh>25 AND a.countyid=84


SELECT a.isg_osm_id,conn_link_array,a.volume,total_conn_vol,conn_vol_array,geh FROM  tm_new_data.nys_hour_8_volume_240401_xgb_unseen a INNER JOIN  tm_new_data.nys_hour_8_volume_240401_xgb_unseen_adjancent_geh_check b
ON a.isg_osm_id =b.isg_osm_id AND conn_isg_osm_id =ANY  (conn_link_array)
WHERE b.geh>25 AND a.countyid=84



SELECT * FROM gis_tables.nys_hour_8_models_c84 nhmc 


SELECT * FROM tm_new_data.urban_rural_county_fc_mapping_240412 nrcm 

--ALTER TABLE tm_new_data.nys_region_county_master_230712 
--ADD COLUMN areatype varchar(5)

--UPDATE  tm_new_data.nys_region_county_master_230712 nrcm
--SET areatype=urcfm.areatype
--FROM tm_new_data.urban_rural_county_fc_mapping_240412 urcfm  
--WHERE nrcm.countyid = urcfm.countyid 




SELECT  nrcm.region, nrcm.countyid, nrcm.county_name, urcfm.county_name,geom, urcfm.areatype FROM tm_new_data.nys_region_county_master_230712 nrcm INNER JOIN tm_new_data.urban_rural_county_fc_mapping_240412 urcfm 
ON nrcm.countyid = urcfm.countyid 

SELECT fc,areatype,count(*) FROM gis_tables.nys_osm_hvc_data_240419 nohd 
WHERE volume >1
GROUP BY fc,areatype 


