--CREATE TABLE gis_tables.nys_osm_hvc_data_240611 AS 
SELECT isg_osm_id, region, countyid, fc, highway, maxspeed, final_place, isg_lanes, length, areatype, hvp, 
	CASE 
		WHEN highway='motorway' THEN 'Freeway'
		WHEN highway='trunk' THEN 'Multilane'
		WHEN highway = 'primary' AND areatype='Urban' THEN 'Primary_Urban'
		WHEN highway = 'primary' AND areatype='Rural' THEN 'Primary_Rural'
		WHEN highway LIKE '%_link' THEN 'Ramp'
	END AS r_type, 
	CASE 
		WHEN highway='motorway' THEN round(((2200+(10*(LEAST(70,maxspeed)-50)))/(1+(hvp::numeric / 100)))*isg_lanes,0)
		WHEN highway ='trunk' THEN 
			CASE 
				WHEN maxspeed<=60  THEN round(((1000+(20*maxspeed))*isg_lanes)/(1+(hvp::NUMERIC/100)),0) 
				ELSE round((2200*isg_lanes)/(1+(hvp::NUMERIC/100)),0)
			END
		WHEN highway IN ('primary','primary_link')  AND areatype='Urban' THEN 950*isg_lanes
		WHEN highway IN ('primary','primary_link') AND areatype='Rural' THEN 745*isg_lanes
		WHEN highway IN ('motorway_link','trunk_link') THEN  
				CASE WHEN isg_lanes = 1 THEN 
					CASE 
						WHEN maxspeed<30 THEN 1800	
						WHEN maxspeed BETWEEN 30 AND 50 THEN 1900
						WHEN maxspeed > 50 AND maxspeed <=65 THEN 2000
						ELSE 2100
					END
					ELSE 
					CASE
						WHEN maxspeed<30 THEN 2700	
						WHEN maxspeed BETWEEN 30 AND 50 THEN 3000
						WHEN maxspeed > 50 AND maxspeed <=65 THEN 3300
						ELSE 3600
					END
				END 
	END AS capacity
	, geom, highway_group_id, next_connected_count, prev_connected_count, volume FROM gis_tables.nys_osm_hvc_data_240419 nohd 
WHERE fc IN (1,2,3)	
--CREATE INDEX ON gis_tables.nys_osm_hvc_data_240611(isg_osm_id)
