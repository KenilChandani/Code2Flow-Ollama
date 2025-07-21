SELECT ST_Translate(
    ST_SetSRID(ST_MakePoint(longitude ::double PRECISION , latitude::double precision), 4326), -- lon and lat are your coordinates
    0.0001, -- distance in meters to move along X-axis
    0.0001   -- distance in meters to move along Y-axis
) ,*
--SELECT *
FROM ds_tt.lat_long_mk

SELECT st_makeline(start_point,end_point) FROM ds_tt.lat_long_mk llm  

SELECT * FROM ds_tt.lat_long_mk llm 

SELECT geom FROM  ds_tt.ny_pollution_mk ayk

SELECT st_translate(
	st_setsrid(st_makepoint(ayk.Longitude ::double PRECISION ,ayk.Latitude ::double PRECISION),4326),0.000,0.000 
),* FROM  ds_tt.ny_pollution_mk ayk

POINT (-78.90403747558594 36.032955169677734)
POINT (-78.8197021484375 35.86520004272461)
POINT (-78.8197021484375 35.86520004272461)
POINT (-78.90403747558594 36.032955169677734)
POINT (-78.8197021484375 35.86520004272461)



SELECT st_makeline() FROM  ds_tt.ny_pollution_mk ayk

SELECT * FROM ds_tt.niagra_county_osm nco 

SELECT * FROM ds_tt.niagra_county_osm nco 
WHERE nco."name" IN  ('North Ridge Road','Ransomville Road')

SELECT * FROM ds_tt.niagra_county_osm nco 
WHERE nco."row_number" IN  (58,59,60,61,62,63,109,110,111,112,114)

SELECT * FROM ds_tt.niagra_county_osm nco 
WHERE nco."row_number" IN (109,110,111,112,114)





		
--114,110,59,62
SELECT
	g
FROM
	(
	SELECT
		st_endpoint(geom) AS g
	FROM
		ds_tt.niagra_county_osm nco
	WHERE
		nco."row_number" IN (114, 108, 59, 62)) AS x
		

SELECT st_makepolygon(ST_GeomFromText('LINESTRING(-78.90977068658154 43.23861530037167,-78.90949360576253 43.23903332105273,-78.9090306692515 43.23835827308329,-78.90951030458503 43.23800029437591,-78.90977068658154 43.23861530037167)'))

WITH a AS (
	SELECT nco."row_number",st_endpoint(geom) AS a FROM ds_tt.niagra_county_osm nco WHERE nco."row_number" IN (114, 108, 59, 62)
)

SELECT geom FROM ds_tt.niagra_county_osm nco WHERE nco."row_number" =114 OR nco."row_number" =108 OR nco."row_number" =59 OR nco."row_number" =62

SELECT
	st_area(poly)
FROM
	(
	SELECT
		st_makepolygon(ST_GeomFromText('LINESTRING(-78.90977068658154 43.23861530037167,-78.90949360576253 43.23903332105273,-78.9090306692515 43.23835827308329,-78.90951030458503 43.23800029437591,-78.90977068658154 43.23861530037167)')) AS poly
	) AS foo
	
SELECT
	st_union(g)
FROM
	(
	SELECT
		geom AS g
	FROM
		ds_tt.niagra_county_osm nco
	WHERE
		nco."row_number" IN (114, 108, 59, 62)) AS x	
