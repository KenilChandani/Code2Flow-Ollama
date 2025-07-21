--Prediction values with Augmented table
--CREATE TABLE tm_new_data.nys_hour_8_xgb_ohe_240619 AS 
SELECT DISTINCT  
nhv.isg_osm_id,nhv.next_isg_osm_id,nhv.prev_isg_osm_id,
nhv.highway_group_id,nhv.next_highway_group_id,nhv.prev_highway_group_id,
nhv.cur_fc,nhv.next_fc,nhv.prev_fc,
nhv.highway_id,nhv.next_highway_id,nhv.prev_highway_id,
nhv.countyid,nhv.next_countyid,nhv.prev_countyid,
nhv.region,nhv.next_region,nhv.prev_region,
nhv.roadwaytype_id,nhv.next_roadwaytype_id,nhv.prev_roadwaytype_id,
nhv.cur_lanes, nhv.next_lanes, nhv.prev_lanes, 
nhv.cur_maxspeed, nhv.next_maxspeed, nhv.prev_maxspeed, 
nhv.cur_final_place, nhv.next_final_place, nhv.prev_final_place,
nhv.next_connected_count,nhv.prev_connected_count,
next_ncc,nhv.next_pcc,nhv.prev_ncc,prev_pcc,
COALESCE(aug_cur.predicted_volume::int,nhv.volume) AS volume,
COALESCE(aug_next.predicted_volume::int,nhv.next_volume) AS next_volume,
COALESCE(aug_prev.predicted_volume::int,nhv.prev_volume) AS prev_volume,
nhv.geom,
COALESCE(aug_cur."source",nhv."source") AS SOURCE
FROM   
tm_new_data.nys_hour_8_volume_240321 nhv 
LEFT JOIN 
(SELECT *,'ML' AS SOURCE FROM tm_new_data.nys_hour_8_xgb_prev_ohe_unseen_predictions_240619) aug_cur
ON aug_cur.isg_osm_id = nhv.isg_osm_id 
LEFT JOIN 
(SELECT *, 'ML' AS SOURCE FROM tm_new_data.nys_hour_8_xgb_prev_ohe_unseen_predictions_240619) aug_next
ON aug_next.isg_osm_id = nhv.next_isg_osm_id 
LEFT JOIN 
(SELECT *,'ML' AS SOURCE FROM  tm_new_data.nys_hour_8_xgb_prev_ohe_unseen_predictions_240619) aug_prev
ON  aug_prev.isg_osm_id = nhv.prev_isg_osm_id;

CREATE INDEX  ON tm_new_data.nys_hour_8_xgb_ohe_240619 USING btree (isg_osm_id);
CREATE INDEX  ON tm_new_data.nys_hour_8_xgb_ohe_240619 USING btree (next_isg_osm_id);
CREATE INDEX  ON tm_new_data.nys_hour_8_xgb_ohe_240619 USING btree (prev_isg_osm_id);
CREATE INDEX  ON tm_new_data.nys_hour_8_xgb_ohe_240619 USING btree (next_connected_count);
CREATE INDEX  ON tm_new_data.nys_hour_8_xgb_ohe_240619 USING btree (prev_connected_count);





tm_new_data.rf_unseen_adjacent_240611

--Adjacent GEH check for multi-previous
--CREATE TABLE tm_new_data.nys_hour_8_xgb_ohe_adjacent_geh_check_240619 AS 
select *,'multi_prev' as conn_tag, tm.geh_check(volume,total_conn_vol::int) as geh from(
select isg_osm_id, array_cat(array[prev_id_1] ,array_agg(prev_id_2)) as conn_link_array,prev_connected_count as connected_count,
source,array_cat(array[prev_src_1] ,array_agg(prev_src_2)) as conn_src_array,
volume, prev_vol_1 + sum(prev_vol_2) as total_conn_vol,array_cat(array[prev_vol_1] ,array_agg(prev_vol_2)) as conn_vol_array
from(
select *,row_number() over(partition by prev_id_1,prev_vol_1,prev_id_2) as row_num,rank() over(partition by isg_osm_id order by prev_id_1) as rnk from(
--with vol_table as(
with vol_table as
(select distinct cur.isg_osm_id, cur.prev_isg_osm_id,cur.prev_connected_count,cur.source,conn.source as prev_source,cur.volume, cur.prev_volume
from tm_new_data.nys_hour_8_xgb_ohe_240619 cur,
tm_new_data.nys_hour_8_xgb_ohe_240619 conn
where cur.prev_isg_osm_id = conn.isg_osm_id
and cur.volume>0 and cur.prev_volume>0 and cur.prev_connected_count >1 and cur.prev_ncc = 1) 
--select * from vol_table where isg_osm_id in(
--select isg_osm_id from vol_table group by isg_osm_id,prev_connected_count having count(distinct prev_isg_osm_id) = prev_connected_count))
select distinct a.isg_osm_id,a.prev_isg_osm_id as prev_id_1,b.prev_isg_osm_id as prev_id_2,a.prev_connected_count,
a.source, a.prev_source as prev_src_1, b.prev_source as prev_src_2,
a.volume, a.prev_volume as prev_vol_1,b.prev_volume as prev_vol_2
from vol_table a,
vol_table b
where a.isg_osm_id = b.isg_osm_id and a.prev_isg_osm_id != b.prev_isg_osm_id)foo)foo
where rnk = 1
group by isg_osm_id,prev_id_1,prev_connected_count,source,prev_src_1,volume,prev_vol_1,row_num)foo



--Adjacent GEH check for multi-next
--INSERT INTO tm_new_data.nys_hour_8_xgb_ohe_adjacent_geh_check_240619
select *,'multi_next' as conn_tag, tm.geh_check(volume,total_conn_vol::int) as geh from(
select isg_osm_id, array_cat(array[next_id_1] ,array_agg(next_id_2)) as conn_link_array,next_connected_count as connected_count,
source,array_cat(array[next_src_1] ,array_agg(next_src_2)) as conn_src_array,
volume, next_vol_1 + sum(next_vol_2) as total_conn_vol,array_cat(array[next_vol_1] ,array_agg(next_vol_2)) as conn_vol_array
from(
select *,row_number() over(partition by next_id_1,next_vol_1,next_id_2) as row_num,rank() over(partition by isg_osm_id order by next_id_1) as rnk from(
--with vol_table as(
with vol_table as
(select distinct cur.isg_osm_id, cur.next_isg_osm_id,cur.next_connected_count,cur.source,conn.source as next_source,cur.volume, cur.next_volume
from tm_new_data.nys_hour_8_xgb_ohe_240619 cur,
tm_new_data.nys_hour_8_xgb_ohe_240619 conn
where cur.next_isg_osm_id = conn.isg_osm_id
and cur.volume>0 and cur.next_volume>0 and cur.next_connected_count >1 and cur.next_pcc = 1) 
--select * from vol_table where isg_osm_id in(
--select isg_osm_id from vol_table group by isg_osm_id,next_connected_count having count(distinct next_isg_osm_id) = next_connected_count))
select distinct a.isg_osm_id,a.next_isg_osm_id as next_id_1,b.next_isg_osm_id as next_id_2,a.next_connected_count,
a.source, a.next_source as next_src_1, b.next_source as next_src_2,
a.volume, a.next_volume as next_vol_1,b.next_volume as next_vol_2
from vol_table a,
vol_table b
where a.isg_osm_id = b.isg_osm_id and a.next_isg_osm_id != b.next_isg_osm_id)foo)foo
where rnk = 1
group by isg_osm_id,next_id_1,next_connected_count,source,next_src_1,volume,next_vol_1,row_num)foo


----Adjacent GEH check for single connected
--INSERT INTO tm_new_data.nys_hour_8_xgb_ohe_adjacent_geh_check_240619
select distinct geh.isg_osm_id,array[next_isg_osm_id] as conn_link_array, next_connected_count as connected_count,
geh.source, 
array[nxt.source] as conn_src_array, 
volume, next_volume as total_conn_vol, array[next_volume] asconn_vol_array, 
'single' as conn_tag, geh
from
    (select isg_osm_id, next_isg_osm_id,source ,next_connected_count,volume,next_volume, tm.geh_check(volume,next_volume) as geh  
    from tm_new_data.nys_hour_8_xgb_ohe_240619 where volume>0 and next_volume>0 and next_connected_count =1 and next_pcc = 1) geh,
(select isg_osm_id,source from tm_new_data.nys_hour_8_xgb_ohe_240619  where prev_connected_count =1 ) nxt
where  geh.next_isg_osm_id = nxt.isg_osm_id --and (geh.source = 'ML' or nxt.source = 'ML')


