with vol_table as
(SELECT distinct isg_osm_id,next_isg_osm_id,next_connected_count,volume,next_volume
FROM tm_new_data.nys_hour_8_volume_240626 WHERE next_connected_count >1 and isg_osm_id in(
select current_id from tm_new_data.ny_connected_osm_master_231211
where CARDINALITY(single_next_ids)=0 and CARDINALITY(multi_next_ids)>1)
),
prv_vol_fill as
	(select isg_osm_id,array_agg(next_isg_osm_id) as next_osm_array,volume,sum(next_volume) as sum_next_vol from vol_table 
		where next_volume>0 and volume>0
		group by isg_osm_id,next_connected_count,volume 
		having count(next_isg_osm_id) = next_connected_count-1),
cur_vol_fill as(
select isg_osm_id,array_agg(next_isg_osm_id) as next_osm_array,volume,sum(next_volume) as sum_next_vol from vol_table 
		where next_volume>0 and volume < 0
		group by isg_osm_id,next_connected_count,volume 
		having count(next_isg_osm_id) = next_connected_count)
select * from (
	select 
	distinct 
	vt.next_isg_osm_id as isg_osm_id,
	'MFA' as source,
	pvf.volume-pvf.sum_next_vol as volume,
	array_append(next_osm_array,pvf.isg_osm_id) as original_ids,
	1 as iteration
	from 
	vol_table vt,
	prv_vol_fill pvf
	where vt.isg_osm_id = pvf.isg_osm_id and vt.next_volume<0
	union 
	select 
	distinct 
	vt.isg_osm_id as isg_osm_id,
	'MFA' as source,
	cvf.sum_next_vol as volume,
	next_osm_array as original_ids,
	1 as iteration
	from 
	vol_table vt,
	cur_vol_fill cvf
	where vt.isg_osm_id = cvf.isg_osm_id) foo 
where volume>0


-----------------------------------------MULTI-FORWARD AUGMENTATION FUNCTION-------------------------------------------------------------------

CREATE OR REPLACE FUNCTION tm_new_data.multi_forward_augmentation(original_table CHARACTER VARYING,conn_table CHARACTER VARYING ,augmented_table CHARACTER VARYING,iteration integer)
	RETURNS TEXT 
	LANGUAGE plpgsql
AS $function$
DECLARE 
	cnt int;
	itr int :=iteration;
BEGIN 
	RAISE NOTICE 'Augmenting';
	EXECUTE 'INSERT INTO ' ||augmented_table|| 
		' with vol_table as
			(SELECT distinct isg_osm_id,next_isg_osm_id,next_connected_count,volume,next_volume
			FROM '||original_table|| ' WHERE next_connected_count >1 and isg_osm_id in(
			select current_id from ' ||conn_table||
			' where CARDINALITY(single_next_ids)=0 and CARDINALITY(multi_next_ids)>1)
			),
			prv_vol_fill as
				(select isg_osm_id,array_agg(next_isg_osm_id) as next_osm_array,volume,sum(next_volume) as sum_next_vol from vol_table 
					where next_volume>0 and volume>0
					group by isg_osm_id,next_connected_count,volume 
					having count(next_isg_osm_id) = next_connected_count-1),
			cur_vol_fill as(
			select isg_osm_id,array_agg(next_isg_osm_id) as next_osm_array,volume,sum(next_volume) as sum_next_vol from vol_table 
					where next_volume>0 and volume < 0
					group by isg_osm_id,next_connected_count,volume 
					having count(next_isg_osm_id) = next_connected_count)
			select * from (
				select 
				distinct 
				vt.next_isg_osm_id as isg_osm_id,
				''MFA'' as source,
				pvf.volume-pvf.sum_next_vol as volume,
				array_append(next_osm_array,pvf.isg_osm_id) as original_ids,
				'||itr||' as iteration
				from 
				vol_table vt,
				prv_vol_fill pvf
				where vt.isg_osm_id = pvf.isg_osm_id and vt.next_volume<0
				union 
				select 
				distinct 
				vt.isg_osm_id as isg_osm_id,
				''MFA'' as source,
				cvf.sum_next_vol as volume,
				next_osm_array as original_ids,
				'||itr||' as iteration
				from 
				vol_table vt,
				cur_vol_fill cvf
				where vt.isg_osm_id = cvf.isg_osm_id) foo 
			where volume>0';
				
	GET DIAGNOSTICS cnt=row_count;
	RAISE NOTICE '% rows augmented',cnt;

	RETURN 'Process Done';
END;
$function$
;

SELECT tm_new_data.multi_forward_augmentation(
	'tm_new_data.nys_hour_8_volume_240626',
	'tm_new_data.ny_connected_osm_master_231211',
	'tm_new_data.nys_augmentation_hour_8_volume_240701',2
)

SELECT * FROM tm_new_data.nys_augmentation_hour_8_volume_240701 WHERE iteration=2 AND source='MFA'