--WITH single_augmentation AS (
--	WITH conn_table AS(
--		SELECT DISTINCT isg_osm_id,highway_id,volume,source,'next' AS tag,single_next_ids as connected_ids 
--		FROM (
--			SELECT A.*,B.single_next_ids FROM (
--				SELECT * FROM tm_new_data.nys_hour_8_volume_240626 WHERE volume > 0 AND next_volume < 0 ) A,tm_new_data.ny_connected_osm_master_231211 B
--				WHERE A.isg_osm_id = B.current_id
--			)next_id 
--			WHERE CARDINALITY(single_next_ids)>0
--	)
--	SELECT UNNEST (connected_ids[:min_pos-1]) AS isg_osm_id,highway_id,volume,SOURCE,
--	CASE 
--		WHEN tag='next' THEN 'SFA' ELSE 'SBA'	
--	END AS aug_source,
--	isg_osm_id AS connected_ids,min_pos --,UNNEST (connected_ids[:min_pos-1]) AS filled_ids
--	FROM(
--		SELECT *,min(pos) OVER(PARTITION BY isg_osm_id) min_pos 
--		FROM (
--			SELECT conn_table.*,array_position(conn_table.connected_ids,vol.isg_osm_id) AS pos,vol.isg_osm_id AS conn_id 
--			FROM conn_table,tm_new_data.nys_hour_8_volume_240626 vol
--			WHERE  vol.volume >0 AND vol.isg_osm_id = ANY(conn_table.connected_ids)
--		) foo
--	)foo1
--)
--UPDATE tm_new_data.nys_hour_8_volume_dummy_240626 nhv
--SET volume=updt.aug_vol,SOURCE=updt.aug_source,iteration=1,original_ids=updt.isg_osm_id,single_filled=null_conn_id
--FROM updt
--WHERE nhv.isg_osm_id=updt.filled_ids 




--create table tm_new_data.nys_augmentation_hour_8_volume_240701 as
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
 )foo where priority_id = 1)

 
------------------------------------------------------SINGLE AUGMENTATION FUNCTION-------------------------------------------------------------------------------- 

CREATE OR REPLACE FUNCTION tm_new_data.single_augmentation(original_table CHARACTER VARYING ,conn_table CHARACTER VARYING ,augmented_table CHARACTER VARYING ,iteration integer)
	RETURNS TEXT 
	LANGUAGE plpgsql
AS $function$ 
DECLARE 
	cnt int;
	itr int := iteration;
BEGIN
	RAISE NOTICE 'Augmenting';
	EXECUTE ' INSERT INTO ' ||augmented_table|| '
		select isg_osm_id,source,volume,original_ids,iteration from (
			WITH single_augmentation
			as
			((
			  WITH first_match_position AS 
			(
			   select a.isg_osm_id,a.highway_id,a.volume,a.source,a.tag,a.connected_ids,
			   COALESCE(
			            (SELECT idx FROM unnest(a.connected_ids) WITH ORDINALITY AS u(value, idx)
			             WHERE value IN (SELECT isg_osm_id FROM ' ||original_table||' WHERE volume > 0 ) ORDER BY idx LIMIT 1
			             ),array_length(a.connected_ids, 1) + 1
			           ) AS first_match_idx
			    FROM
				(
					SELECT DISTINCT isg_osm_id,highway_id,volume,source,''next'' AS tag,single_next_ids as connected_ids 
					FROM 
					(
					    SELECT A.*,B.single_next_ids FROM (SELECT * FROM ' ||original_table|| ' WHERE volume > 0 AND next_volume < 0 ) A, '
					    ||conn_table|| ' B
					    WHERE A.isg_osm_id = B.current_id
					)next_id 
					WHERE CARDINALITY(single_next_ids)>0
				) a
			)
			SELECT
			    unnest(case when first_match_idx <= array_length(connected_ids, 1) then connected_ids[1:first_match_idx - 1] else connected_ids end) as isg_osm_id,
			    ''SFA'' as source,volume,array[isg_osm_id] as original_ids, 
			    '||itr||' as iteration
			FROM
			    first_match_position )
			union
			(
			 WITH first_match_position AS 
			(
			   select a.isg_osm_id,a.highway_id,a.volume,a.source,a.tag,a.connected_ids,
			   COALESCE(
			            (SELECT idx FROM unnest(a.connected_ids) WITH ORDINALITY AS u(value, idx)
			             WHERE value IN (SELECT isg_osm_id FROM ' ||original_table|| ' WHERE volume > 0 ) ORDER BY idx LIMIT 1
			             ),array_length(a.connected_ids, 1) + 1
			           ) AS first_match_idx
			    FROM
				(
					SELECT DISTINCT isg_osm_id,highway_id,volume,source,''prev'' AS tag,single_prev_ids as connected_ids 
					FROM 
					(
					    SELECT A.*,B.single_prev_ids FROM (SELECT * FROM ' ||original_table|| ' WHERE volume > 0 AND prev_volume < 0 ) A, '
					    ||conn_table|| ' B
					    WHERE A.isg_osm_id = B.current_id
					)next_id 
					WHERE CARDINALITY(single_prev_ids)>0
				) a
			)
			SELECT
			    unnest(case when first_match_idx <= array_length(connected_ids, 1) then connected_ids[1:first_match_idx - 1] else connected_ids end) as isg_osm_id,
			    ''SBA'' as source,volume,array[isg_osm_id] as original_ids, 
			    '||itr||' as iteration
			FROM
			    first_match_position
			))
			select * from (
			select *,row_number() over(partition by isg_osm_id,source order by source desc) as priority_id 
			from single_augmentation
			 )foo where priority_id = 1) as bar;';
				    
	GET DIAGNOSTICS cnt = row_count;
	RAISE NOTICE '% rows will augment',cnt;

	RETURN 'Process Done';
END;
$function$
;


SELECT tm_new_data.single_augmentation(
	'tm_new_data.nys_hour_8_volume_240626',
	'tm_new_data.ny_connected_osm_master_231211',
	'tm_new_data.nys_augmentation_hour_8_volume_240701',2
)


