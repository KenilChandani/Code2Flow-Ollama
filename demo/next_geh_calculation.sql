                                    

SELECT DISTINCT isg_osm_id,next_connected_count,SOURCE,highway_id,volume,single_next_ids,multi_next_ids,single_prev_ids,multi_prev_ids 
FROM (
	SELECT A.*,B.* FROM (
		SELECT * FROM tm_new_data.nys_hour_8_volume_240626 where volume > 0 and next_pcc = 1
	)A,tm_new_data.ny_connected_osm_master_231211 B
	WHERE A.isg_osm_id=B.current_id
)foo
WHERE isg_osm_id NOT IN (
	SELECT DISTINCT isg_osm_id FROM tm_new_data.nys_hour_8_volume_240626_adjacent_geh_check 
	WHERE conn_tag IN ('single','multi_next')
	)
------------------------------------------------------------------------------------------------------------------------------------------
	
WITH all_avail AS (
	SELECT DISTINCT isg_osm_id,next_connected_count,SOURCE,highway_id,volume,single_next_ids,multi_next_ids,single_prev_ids,multi_prev_ids 
	FROM (
		SELECT A.*,B.* FROM (
			SELECT * FROM tm_new_data.nys_hour_8_volume_240626 where volume > 0 AND  next_pcc = 1 --AND next_connected_count=1
		)A,tm_new_data.ny_connected_osm_master_240626 B
		WHERE A.isg_osm_id=B.current_id
	)foo
),
cal_geh AS (	
	SELECT *--,(SELECT EXISTS(SELECT 1 FROM all_avail WHERE isg_osm_id=ANY(single_next_ids))) AS common_ids 
	FROM (
		SELECT * FROM all_avail
		WHERE isg_osm_id NOT IN (
			SELECT DISTINCT isg_osm_id FROM tm_new_data.nys_hour_8_volume_240626_adjacent_geh_check 
			WHERE conn_tag IN ('single','multi_next')
		) 
	)foo
),
vol AS (
	SELECT DISTINCT a.isg_osm_id,a.volume,a.SOURCE FROM tm_new_data.nys_hour_8_volume_240626 a WHERE  a.volume>0
)
--SELECT * FROM cal_geh
SELECT *,tm.geh_check(volume,total_conn_volume::int) FROM(
	SELECT DISTINCT  a.isg_osm_id,array_agg( c.isg_osm_id) AS conn_id,a.next_connected_count,a.SOURCE,array_agg(c.SOURCE) AS conn_src_array,
	a.volume,sum(c.volume) AS total_conn_volume,array_agg( c.volume) AS conn_vol_array,'multi_next' AS conn_tag,'Original' AS iteration 
	FROM cal_geh a --JOIN all_avail b ON b.isg_osm_id = ANY(a.multi_next_ids::bigint[])
	JOIN vol c ON c.isg_osm_id = ANY(a.multi_next_ids::bigint[])
	GROUP BY a.isg_osm_id,a.next_connected_count,a.SOURCE,a.volume,a.multi_next_ids
	HAVING (CARDINALITY(array_agg(c.isg_osm_id))=CARDINALITY(a.multi_next_ids) AND  CARDINALITY(a.multi_next_ids)>1) OR 
	(CARDINALITY(array_agg(c.isg_osm_id))<CARDINALITY(a.multi_next_ids) AND  CARDINALITY(a.multi_next_ids)>1 AND CARDINALITY(array_agg(c.isg_osm_id))>0 AND sum(c.volume)>a.volume)
)foo 



--------------------------------------------------------------EXPERIMENT---------------------------------------------------------------------------------------------
 
--SELECT DISTINCT conn.current_id ,single_next_ids,multi_next_ids,vol_single_next.isg_osm_id AS single_osm_id,vol_multi_next.isg_osm_id AS multi_osm_id 
--FROM 
--tm_new_data.ny_connected_osm_master_231211 conn,
--tm_new_data.nys_hour_8_volume_240626 vol_cur,
--tm_new_data.nys_hour_8_volume_240626 vol_single_next,
--tm_new_data.nys_hour_8_volume_240626 vol_multi_next
--WHERE conn.current_id=vol_cur.isg_osm_id AND 
--((vol_multi_next.isg_osm_id=ANY (conn.multi_next_ids) AND vol_multi_next.volume>0) OR (vol_single_next.isg_osm_id=ANY(conn.single_next_ids) AND vol_single_next.volume>0)) AND 
--vol_cur.volume>0 
-- 
--
--SELECT * FROM pg_stat_activity
--WHERE state ='active'
--
--
--SELECT pg_terminate_backend(20378)
--
--WITH all_avail AS (
--	SELECT DISTINCT isg_osm_id,next_connected_count,SOURCE,highway_id,volume,single_next_ids,multi_next_ids,single_prev_ids,multi_prev_ids 
--	FROM (
--		SELECT A.*,B.* FROM (
--			SELECT * FROM tm_new_data.nys_hour_8_volume_240626 where volume > 0 AND  next_pcc = 1
--		)A,tm_new_data.ny_connected_osm_master_240626 B
--		WHERE A.isg_osm_id=B.current_id
--	)foo
--),
--cal_geh AS (	
--	SELECT *--,(SELECT EXISTS(SELECT 1 FROM all_avail WHERE isg_osm_id=ANY(single_next_ids))) AS common_ids 
--	FROM (
--		SELECT * FROM all_avail
--		WHERE isg_osm_id NOT IN (
--			SELECT DISTINCT isg_osm_id FROM tm_new_data.nys_hour_8_volume_240626_adjacent_geh_check 
--			WHERE conn_tag IN ('single','multi_next')
--		) 
--	)foo
--	WHERE isg_osm_id=959159711001
--),
--vol AS (
--	SELECT DISTINCT a.isg_osm_id,a.volume,a.SOURCE FROM tm_new_data.nys_hour_8_volume_240626 a WHERE  a.volume>0
--)
--SELECT *,tm.geh_check(volume,total_conn_volume::int) FROM(
--	SELECT DISTINCT  a.isg_osm_id,array_agg( c.isg_osm_id) AS conn_id,a.next_connected_count,a.SOURCE,array_agg(c.SOURCE) AS conn_src_array,
--	a.volume,sum(c.volume) AS total_conn_volume,array_agg( c.volume) AS conn_vol_array,'multi_next' AS conn_tag,'Original' AS iteration 
--	FROM cal_geh a JOIN all_avail b ON b.isg_osm_id = ANY(a.multi_next_ids::bigint[])
--	JOIN vol c ON c.isg_osm_id = ANY(a.multi_next_ids::bigint[])
--	GROUP BY a.isg_osm_id,a.next_connected_count,a.SOURCE,a.volume,a.multi_next_ids
--	HAVING (CARDINALITY(array_agg(b.isg_osm_id))=CARDINALITY(a.multi_next_ids) AND  CARDINALITY(a.multi_next_ids)>1) OR 
--	(CARDINALITY(array_agg(b.isg_osm_id))<CARDINALITY(a.multi_next_ids) AND  CARDINALITY(a.multi_next_ids)>1 AND CARDINALITY(array_agg(b.isg_osm_id))>0 AND sum(b.volume)>a.volume)
--)foo 
--
--
--
--
--
--SELECT DISTINCT  a.isg_osm_id,array_agg( b.isg_osm_id) AS conn_id,a.next_connected_count,a.SOURCE,array_agg(b.SOURCE) AS conn_src_array,
--a.volume,sum(b.volume) AS total_conn_volume,array_agg( b.volume) AS conn_vol_array,'multi_next' AS conn_tag,'Original' AS iteration 
--FROM cal_geh a --JOIN all_avail b ON b.isg_osm_id = ANY(a.multi_next_ids::bigint[])
--JOIN vol b ON b.isg_osm_id =ANY(a.multi_next_ids::bigint[])
--GROUP BY a.isg_osm_id,a.next_connected_count,a.SOURCE,a.volume,a.multi_next_ids
--HAVING (CARDINALITY(array_agg(b.isg_osm_id))=CARDINALITY(a.multi_next_ids) AND  CARDINALITY(a.multi_next_ids)>1) OR 
--(CARDINALITY(array_agg(b.isg_osm_id))<CARDINALITY(a.multi_next_ids) AND  CARDINALITY(a.multi_next_ids)>1 AND CARDINALITY(array_agg(b.isg_osm_id))>0 AND sum(b.volume)>a.volume)
--
--
--
--
--
--SELECT DISTINCT  a.isg_osm_id,array_agg( b.isg_osm_id) AS conn_id,a.next_connected_count,a.SOURCE,array_agg(b.SOURCE) AS conn_src_array,
--a.volume,sum(c.volume) AS total_conn_volume,array_agg( c.volume) AS conn_vol_array,'multi_next' AS conn_tag,'Original' AS iteration 
--	FROM cal_geh a JOIN all_avail b ON b.isg_osm_id = ANY(a.multi_next_ids::bigint[])
--	JOIN vol c ON c.isg_osm_id =ANY(a.multi_next_ids::bigint[])
--	GROUP BY a.isg_osm_id,a.next_connected_count,a.SOURCE,a.volume,a.multi_next_ids
--	HAVING (CARDINALITY(array_agg(b.isg_osm_id))=CARDINALITY(a.multi_next_ids) AND  CARDINALITY(a.multi_next_ids)>1) OR 
--	(CARDINALITY(array_agg(b.isg_osm_id))<CARDINALITY(a.multi_next_ids) AND  CARDINALITY(a.multi_next_ids)>1 AND CARDINALITY(array_agg(b.isg_osm_id))>0 AND sum(c.volume)>a.volume)
--UNION 
--SELECT a.isg_osm_id,array_agg(b.isg_osm_id) AS conn_id,a.next_connected_count AS connected_count,a.SOURCE,array_agg(b.SOURCE) ,
--	a.volume,sum(b.volume) AS total_conn_vol,array_agg(b.volume) AS conn_vol_array,'multi_next' AS conn_tag,'Original' AS iteration 
--	FROM cal_geh a JOIN all_avail b ON b.isg_osm_id=ANY(a.multi_next_ids::bigint[]) 
--	GROUP BY a.isg_osm_id,a.next_connected_count,a.SOURCE,a.volume,a.multi_next_ids
--	HAVING (CARDINALITY(array_agg(b.isg_osm_id))=CARDINALITY(a.multi_next_ids) AND  CARDINALITY(a.multi_next_ids)>1) OR 
--	(CARDINALITY(array_agg(b.isg_osm_id))<CARDINALITY(a.multi_next_ids) AND  CARDINALITY(a.multi_next_ids)>1 AND CARDINALITY(array_agg(b.isg_osm_id))>0 AND sum(b.volume)>a.volume)
--
--
--
--
--SELECT *,tm.geh_check(volume,total_conn_volume::int) FROM(
--	SELECT DISTINCT  a.isg_osm_id,array_agg( c.isg_osm_id) AS conn_id,a.next_connected_count,a.SOURCE,array_agg(c.SOURCE) AS conn_src_array,
--	a.volume,sum(c.volume) AS total_conn_volume,array_agg( c.volume) AS conn_vol_array,'multi_next' AS conn_tag,'Original' AS iteration 
--	FROM cal_geh a JOIN all_avail b ON b.isg_osm_id = ANY(a.multi_next_ids::bigint[])
--	JOIN vol c ON c.isg_osm_id = ANY(a.multi_next_ids::bigint[])
--	GROUP BY a.isg_osm_id,a.next_connected_count,a.SOURCE,a.volume,a.multi_next_ids
--	HAVING (CARDINALITY(array_agg(b.isg_osm_id))=CARDINALITY(a.multi_next_ids) AND  CARDINALITY(a.multi_next_ids)>1) OR 
--	(CARDINALITY(array_agg(b.isg_osm_id))<CARDINALITY(a.multi_next_ids) AND  CARDINALITY(a.multi_next_ids)>1 AND CARDINALITY(array_agg(b.isg_osm_id))>0 AND sum(b.volume)>a.volume)
--)foo 
--
--
--
--SELECT * FROM (
--	SELECT DISTINCT  a.isg_osm_id, c.isg_osm_id AS conn_id,a.next_connected_count,a.SOURCE,c.SOURCE AS conn_src_array,
--	a.volume,c.volume AS total_conn_volume,c.volume AS conn_vol_array,'multi_next' AS conn_tag,'Original' AS iteration 
--	FROM cal_geh a JOIN all_avail b ON b.isg_osm_id = ANY(a.multi_next_ids::bigint[])
--	JOIN vol c ON c.isg_osm_id = ANY(a.multi_next_ids::bigint[])
--	UNION 
--	SELECT DISTINCT  a.isg_osm_id, c.isg_osm_id AS conn_id,a.next_connected_count,a.SOURCE,c.SOURCE AS conn_src_array,
--	a.volume,c.volume AS total_conn_volume,c.volume AS conn_vol_array,'multi_next' AS conn_tag,'Original' AS iteration 
--	FROM cal_geh a --JOIN all_avail b ON b.isg_osm_id = ANY(a.multi_next_ids::bigint[])
--	JOIN vol c ON c.isg_osm_id = ANY(a.multi_next_ids::bigint[])
--)foo
