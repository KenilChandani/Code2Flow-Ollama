------------------------------------------------------------------Base Table creation after conflation changes-------------------------------------------------
--CREATE TABLE tm_new_data.nys_hour_8_volume_original_240626
WITH conn_count AS(
	SELECT cur.current_id  AS isg_osm_id,
	count(cur.next_id) OVER(PARTITION BY cur.current_id,cur.prev_id) AS next_connected_count,
	count(cur.prev_id) OVER(PARTITION BY cur.current_id,cur.next_id) AS prev_connected_count
	FROM tm_new_data.prev_next_group_id_raw_data_231206 cur
) 
SELECT 
cur.current_id  AS isg_osm_id,
COALESCE (cur.next_id,-1) AS next_isg_osm_id,
COALESCE (cur.prev_id,-1) AS prev_isg_osm_id,
cur_gid.gid AS highway_group_id,
COALESCE (next_gid.gid,-1) AS next_highway_group_id,
COALESCE (prev_gid.gid,-1) AS prev_highway_group_id,
cur_fp.fc AS cur_fc,
COALESCE (next_fp.fc,-1) AS next_fc,
COALESCE (prev_fp.fc,-1) AS prev_fc,
cur_hw.highway_id AS highway_id,
COALESCE (next_hw.highway_id,-1) AS next_highway_id,
COALESCE (prev_hw.highway_id,-1) AS prev_highway_id,
cur_county.countyid AS countyid,
COALESCE (next_county.countyid,-1) AS next_countyid,
COALESCE (prev_county.countyid,-1) AS prev_countyid,
cur_reg.region AS region,
COALESCE (next_reg.region,-1) AS next_region,
COALESCE (prev_reg.region,-1) AS prev_region,
cur_rtype.roadwaytype_id AS cur_roadwaytype_id,
COALESCE (next_rtype.roadwaytype_id,-1) AS next_roadwaytype_id,
COALESCE (prev_rtype.roadwaytype_id,-1) AS prev_roadwaytype_id,
c.next_connected_count,c.prev_connected_count,
COALESCE (n.next_connected_count,-1) AS next_ncc,
COALESCE (n.prev_connected_count,-1) AS next_pcc,
COALESCE (p.next_connected_count,-1) AS prev_ncc,
COALESCE (p.prev_connected_count,-1) AS prev_pcc,
cur_fp.isg_lanes AS cur_lanes,
COALESCE (next_fp.isg_lanes,-1) AS next_lanes,
COALESCE (prev_fp.isg_lanes,-1) AS prev_lanes,
cur_fp.maxspeed AS cur_maxspeed,
COALESCE (next_fp.maxspeed,-1) AS next_maxspeed,
COALESCE (prev_fp.maxspeed,-1) AS prev_maxspeed,
cur_fp.final_place AS cur_final_place,
COALESCE (next_fp.final_place) AS next_final_place,
COALESCE (prev_fp.final_place) AS prev_final_place,
COALESCE (cur_vol.avg_volume,-1) AS volume,
COALESCE (next_vol.avg_volume,-1) AS next_volume,
COALESCE (prev_vol.avg_volume,-1) AS prev_volume,
cur.geom 
--"source",replacement_step,original_ids,single_filled,iteration
FROM tm_new_data.prev_next_group_id_raw_data_231206 cur
LEFT JOIN tm_new_data.osm_roadtype_master_231211 cur_rtype ON cur.current_id =cur_rtype.isg_osm_id
LEFT JOIN tm_new_data.osm_roadtype_master_231211 next_rtype ON cur.next_id =next_rtype.isg_osm_id
LEFT JOIN tm_new_data.osm_roadtype_master_231211 prev_rtype ON cur.prev_id =prev_rtype.isg_osm_id
LEFT JOIN conn_count c ON cur.current_id=c.isg_osm_id
LEFT JOIN conn_count n ON cur.next_id=n.isg_osm_id
LEFT JOIN conn_count p ON cur.prev_id =p.isg_osm_id 
LEFT JOIN tm_new_data.nys_osm_group_data_230829_final cur_gid ON cur.current_id =cur_gid.osm_id
LEFT JOIN tm_new_data.nys_osm_group_data_230829_final next_gid ON cur.next_id =next_gid.osm_id
LEFT JOIN tm_new_data.nys_osm_group_data_230829_final prev_gid ON cur.prev_id =prev_gid.osm_id
LEFT JOIN tm_new_data.nys_osm_county_mapping cur_county ON cur.current_id =cur_county.isg_osm_id
LEFT JOIN tm_new_data.nys_osm_county_mapping next_county ON cur.next_id =next_county.isg_osm_id
LEFT JOIN tm_new_data.nys_osm_county_mapping prev_county ON cur.prev_id =prev_county.isg_osm_id
LEFT JOIN tm_new_data.nys_region_county_master_230712 cur_reg ON cur_county.countyid=cur_reg.countyid
LEFT JOIN tm_new_data.nys_region_county_master_230712 next_reg ON next_county.countyid=next_reg.countyid
LEFT JOIN tm_new_data.nys_region_county_master_230712 prev_reg ON prev_county.countyid=prev_reg.countyid
LEFT JOIN tm_new_data.highway_master_240321 cur_hw ON cur.cur_highway = cur_hw.highway
LEFT JOIN tm_new_data.highway_master_240321 next_hw ON cur.next_highway = next_hw.highway
LEFT JOIN tm_new_data.highway_master_240321 prev_hw ON cur.prev_highway = prev_hw.highway
LEFT JOIN gis_tables.nys_osm_lanes_speed_details_speed_2 cur_fp ON cur.current_id=cur_fp.isg_osm_id
LEFT JOIN gis_tables.nys_osm_lanes_speed_details_speed_2 next_fp ON cur.next_id=next_fp.isg_osm_id
LEFT JOIN gis_tables.nys_osm_lanes_speed_details_speed_2 prev_fp ON cur.prev_id=prev_fp.isg_osm_id
LEFT JOIN (SELECT isg_osm_id,avg_volume FROM tm_new_data.tm_clean_data_wd_231109 WHERE HOUR=8 AND avg_volume>0) cur_vol ON cur.current_id =cur_vol.isg_osm_id
LEFT JOIN (SELECT isg_osm_id,avg_volume FROM tm_new_data.tm_clean_data_wd_231109 WHERE HOUR=8 AND avg_volume>0) next_vol ON cur.next_id =next_vol.isg_osm_id
LEFT JOIN (SELECT isg_osm_id,avg_volume FROM tm_new_data.tm_clean_data_wd_231109 WHERE HOUR=8 AND avg_volume>0) prev_vol ON cur.prev_id =prev_vol.isg_osm_id

CREATE INDEX ON tm_new_data.nys_hour_8_volume_original_240626 USING btree(isg_osm_id)
CREATE INDEX ON tm_new_data.nys_hour_8_volume_original_240626 USING btree(next_isg_osm_id)
CREATE INDEX ON tm_new_data.nys_hour_8_volume_original_240626 USING btree(prev_isg_osm_id)

-------------------------------OPTIMIZED QUERY----------------------------------------------------------

--CREATE TABLE tm_new_data.nys_hour_8_volume_original_240626
WITH conn_count AS(
	SELECT cur.current_id  AS isg_osm_id,
	count(DISTINCT cur.next_id) FILTER (WHERE cur.next_id <> -1) AS next_connected_count,
	count(DISTINCT cur.prev_id) FILTER (WHERE cur.prev_id <> -1) AS prev_connected_count
	FROM tm_new_data.prev_next_group_id_raw_data_231206 cur
	GROUP BY cur.current_id 
) 
SELECT 
cur.current_id  AS isg_osm_id,
COALESCE (cur.next_id,-1) AS next_isg_osm_id,
COALESCE (cur.prev_id,-1) AS prev_isg_osm_id,
cur_gid.gid AS highway_group_id,
COALESCE (next_gid.gid,-1) AS next_highway_group_id,
COALESCE (prev_gid.gid,-1) AS prev_highway_group_id,
cur_fp.fc AS cur_fc,
COALESCE (next_fp.fc,-1) AS next_fc,
COALESCE (prev_fp.fc,-1) AS prev_fc,
cur_hw.highway_id AS highway_id,
COALESCE (next_hw.highway_id,-1) AS next_highway_id,
COALESCE (prev_hw.highway_id,-1) AS prev_highway_id,
cur_county.countyid AS countyid,
COALESCE (next_county.countyid,-1) AS next_countyid,
COALESCE (prev_county.countyid,-1) AS prev_countyid,
cur_reg.region AS region,
COALESCE (next_reg.region,-1) AS next_region,
COALESCE (prev_reg.region,-1) AS prev_region,
cur_rtype.roadwaytype_id AS cur_roadwaytype_id,
COALESCE (next_rtype.roadwaytype_id,-1) AS next_roadwaytype_id,
COALESCE (prev_rtype.roadwaytype_id,-1) AS prev_roadwaytype_id,
CASE
	WHEN c.next_connected_count=0 THEN -1 ELSE c.next_connected_count
END AS next_connected_count,
CASE
	WHEN c.prev_connected_count=0 THEN -1 ELSE c.prev_connected_count
END AS prev_connected_count,
COALESCE (n.next_connected_count,-1) AS next_ncc,
COALESCE (n.prev_connected_count,-1) AS next_pcc,
COALESCE (p.next_connected_count,-1) AS prev_ncc,
COALESCE (p.prev_connected_count,-1) AS prev_pcc,
cur_fp.isg_lanes AS cur_lanes,
COALESCE (next_fp.isg_lanes,-1) AS next_lanes,
COALESCE (prev_fp.isg_lanes,-1) AS prev_lanes,
cur_fp.maxspeed AS cur_maxspeed,
COALESCE (next_fp.maxspeed,-1) AS next_maxspeed,
COALESCE (prev_fp.maxspeed,-1) AS prev_maxspeed,
cur_fp.final_place AS cur_final_place,
COALESCE (next_fp.final_place,'-1') AS next_final_place,
COALESCE (prev_fp.final_place,'-1') AS prev_final_place,
COALESCE (cur_vol.avg_volume,-1) AS volume,
COALESCE (next_vol.avg_volume,-1) AS next_volume,
COALESCE (prev_vol.avg_volume,-1) AS prev_volume,
cur.geom 
FROM tm_new_data.prev_next_group_id_raw_data_231206 cur
LEFT JOIN tm_new_data.osm_roadtype_master_231211 cur_rtype ON cur.current_id =cur_rtype.isg_osm_id
LEFT JOIN tm_new_data.osm_roadtype_master_231211 next_rtype ON cur.next_id =next_rtype.isg_osm_id
LEFT JOIN tm_new_data.osm_roadtype_master_231211 prev_rtype ON cur.prev_id =prev_rtype.isg_osm_id
LEFT JOIN conn_count c ON cur.current_id=c.isg_osm_id
LEFT JOIN conn_count n ON cur.next_id=n.isg_osm_id
LEFT JOIN conn_count p ON cur.prev_id =p.isg_osm_id 
LEFT JOIN tm_new_data.nys_osm_group_data_230829_final cur_gid ON cur.current_id =cur_gid.osm_id
LEFT JOIN tm_new_data.nys_osm_group_data_230829_final next_gid ON cur.next_id =next_gid.osm_id
LEFT JOIN tm_new_data.nys_osm_group_data_230829_final prev_gid ON cur.prev_id =prev_gid.osm_id
LEFT JOIN tm_new_data.nys_osm_county_mapping cur_county ON cur.current_id =cur_county.isg_osm_id
LEFT JOIN tm_new_data.nys_osm_county_mapping next_county ON cur.next_id =next_county.isg_osm_id
LEFT JOIN tm_new_data.nys_osm_county_mapping prev_county ON cur.prev_id =prev_county.isg_osm_id
LEFT JOIN tm_new_data.nys_region_county_master_230712 cur_reg ON cur_county.countyid=cur_reg.countyid
LEFT JOIN tm_new_data.nys_region_county_master_230712 next_reg ON next_county.countyid=next_reg.countyid
LEFT JOIN tm_new_data.nys_region_county_master_230712 prev_reg ON prev_county.countyid=prev_reg.countyid
LEFT JOIN tm_new_data.highway_master_240321 cur_hw ON cur.cur_highway = cur_hw.highway
LEFT JOIN tm_new_data.highway_master_240321 next_hw ON cur.next_highway = next_hw.highway
LEFT JOIN tm_new_data.highway_master_240321 prev_hw ON cur.prev_highway = prev_hw.highway
LEFT JOIN gis_tables.nys_osm_lanes_speed_details_speed_2 cur_fp ON cur.current_id=cur_fp.isg_osm_id
LEFT JOIN gis_tables.nys_osm_lanes_speed_details_speed_2 next_fp ON cur.next_id=next_fp.isg_osm_id
LEFT JOIN gis_tables.nys_osm_lanes_speed_details_speed_2 prev_fp ON cur.prev_id=prev_fp.isg_osm_id
LEFT JOIN (SELECT isg_osm_id,avg_volume FROM tm_new_data.tm_clean_data_wd_231109 WHERE HOUR=8 AND avg_volume>0) cur_vol ON cur.current_id =cur_vol.isg_osm_id
LEFT JOIN (SELECT isg_osm_id,avg_volume FROM tm_new_data.tm_clean_data_wd_231109 WHERE HOUR=8 AND avg_volume>0) next_vol ON cur.next_id =next_vol.isg_osm_id
LEFT JOIN (SELECT isg_osm_id,avg_volume FROM tm_new_data.tm_clean_data_wd_231109 WHERE HOUR=8 AND avg_volume>0) prev_vol ON cur.prev_id =prev_vol.isg_osm_id

CREATE INDEX ON tm_new_data.nys_hour_8_volume_original_240626 USING btree(isg_osm_id)
CREATE INDEX ON tm_new_data.nys_hour_8_volume_original_240626 USING btree(next_isg_osm_id)
CREATE INDEX ON tm_new_data.nys_hour_8_volume_original_240626 USING btree(prev_isg_osm_id)


--------------------------------------------------nys_hour_8_volume_240626----------------------------------------------------------

--CREATE TABLE tm_new_data.nys_hour_8_volume_240626
SELECT *,
CASE 
	WHEN volume>0 THEN 'Original' ELSE NULL 
END AS "source" , 
CASE 
	WHEN volume>0 THEN 0 ELSE NULL
END AS iteration,
NULL AS original_ids,
NULL AS single_filled
FROM tm_new_data.nys_hour_8_volume_original_240626

CREATE INDEX ON tm_new_data.nys_hour_8_volume_240626 USING btree(isg_osm_id)
CREATE INDEX ON tm_new_data.nys_hour_8_volume_240626 USING btree(next_isg_osm_id)
CREATE INDEX ON tm_new_data.nys_hour_8_volume_240626 USING btree(prev_isg_osm_id)

