create table tm_new_data.nys_osm_county_mapping as
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

insert into tm_new_data.nys_osm_county_mapping
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