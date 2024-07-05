-- Join the cleaned wikidata to all the GeoNames data
select
	q.id,
	q.label as archiveName,
	q.point as archivePoint,
	q.cityGeoName as cityGeoNameID,
	ST_GEOMFROMTEXT(q.cityPointGeoNames) as cityPoint,
	q.geoCityNameToponym as cityName,
	q.cityAdminName1,
	q.cityAdminName2,
	q.cityAdminName3,
	q.geoCountryName as country,
	q.continentWikiLabel as continent
from (
	select 
		w.*, 
		gc.toponymName as geoCityNameToponym, 
		gc.asciiName as geoCityNameAscii, 
		gc.lat as cityLat, 
		gc.lng as cityLng, 
		CONCAT('POINT(', gc.lng, ' ', gc.lat,')') as cityPointGeoNames,
		gc.adminName1 as cityAdminName1, 
		gc.adminName2 as cityAdminName2, 
		gc.adminName3 as cityAdminName3, 
		gc.countryName as geoCountryName
	from (
		select *
		from copy_wiki
		where label not REGEXP ('^Q[0-9]')
	) w 
	left join geonames_cities gc on w.cityGeoName = gc.id
) q
order by continent, country, cityName