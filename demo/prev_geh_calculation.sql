WITH all_avail AS (
	SELECT DISTINCT isg_osm_id,prev_connected_count,SOURCE,highway_id,volume,single_next_ids,multi_next_ids,single_prev_ids,multi_prev_ids 
	FROM (
		SELECT A.*,B.* FROM (
			SELECT * FROM tm_new_data.nys_hour_8_volume_240626 where volume > 0  AND prev_ncc = 1--AND prev_connected_count=1
		)A,tm_new_data.ny_connected_osm_master_231211 B
		WHERE A.isg_osm_id=B.current_id
	)foo
),
cal_geh AS (	
	SELECT *,(SELECT EXISTS(SELECT 1 FROM all_avail WHERE isg_osm_id=ANY(single_next_ids))) AS common_ids 
	FROM (
		SELECT * FROM all_avail
		WHERE isg_osm_id NOT IN (
			SELECT DISTINCT conn_isg_osm_array[1] AS removal_ids_prev_geh FROM tm_new_data.nys_hour_8_volume_240626_adjacent_geh_check 
			WHERE conn_tag ='single' 
			UNION 
			SELECT DISTINCT isg_osm_id AS removal_ids_prev_geh FROM tm_new_data.nys_hour_8_volume_240626_adjacent_geh_check 
			WHERE conn_tag = 'multi_prev' 
		) 
	)foo
),
vol AS (
	SELECT DISTINCT a.isg_osm_id,a.volume,a.SOURCE FROM tm_new_data.nys_hour_8_volume_240626 a WHERE  a.volume>0
)
SELECT *,tm.geh_check(volume,total_conn_vol::int) FROM (
	SELECT a.isg_osm_id,array_agg(b.isg_osm_id) AS conn_id,a.prev_connected_count AS connected_count,a.SOURCE,array_agg(b.SOURCE) ,
	a.volume,sum(b.volume) AS total_conn_vol,array_agg(b.volume) AS conn_vol_array,'multi_prev' AS conn_tag,'Original' AS iteration 
	FROM cal_geh a JOIN all_avail b ON b.isg_osm_id=ANY(a.multi_prev_ids)
	--JOIN vol c ON c.isg_osm_id = ANY(a.multi_prev_ids::bigint[])
	--WHERE  common_ids IS FALSE
	GROUP BY a.isg_osm_id,a.prev_connected_count,a.SOURCE,a.volume,a.multi_prev_ids
	HAVING (CARDINALITY(array_agg(b.isg_osm_id))=CARDINALITY(a.multi_prev_ids) AND  CARDINALITY(a.multi_prev_ids)>1) OR 
	(CARDINALITY(array_agg(b.isg_osm_id))<CARDINALITY(a.multi_prev_ids) AND  CARDINALITY(a.multi_prev_ids)>1 AND CARDINALITY(array_agg(b.isg_osm_id))>0 AND sum(b.volume)>a.volume)
)foo

SELECT * FROM tm_new_data.adjacent nhvagc WHERE geh > 25