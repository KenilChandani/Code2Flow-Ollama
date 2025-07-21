--CREATE  TABLE tm_new_data.nys_hour_8_volume_240524 AS
--WITH cte AS (
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

CREATE INDEX  ON tm_new_data.nys_hour_8_volume_240524 USING btree (isg_osm_id);
CREATE INDEX  ON tm_new_data.nys_hour_8_volume_240524 USING btree (next_isg_osm_id);
CREATE INDEX  ON tm_new_data.nys_hour_8_volume_240524 USING btree (prev_isg_osm_id);
CREATE INDEX  ON tm_new_data.nys_hour_8_volume_240524 USING btree (next_connected_count);
CREATE INDEX  ON tm_new_data.nys_hour_8_volume_240524 USING btree (prev_connected_count);
