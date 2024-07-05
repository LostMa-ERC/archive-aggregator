-- Create a temporary selection of Paris cities
drop temporary table if exists paris;
create temporary table paris as 
select * from geonames_cities gc 
where toponymName REGEXP ('Paris [0-9]{1,2}')
or toponymName like 'Paris';

-- Save the GeoNames ID of the best Paris record
set @p1 := (select id from paris where toponymName like 'Paris');
select @p1;

-- Copy the wikidata table
drop table if exists working_wikidata ;
create table copy_wiki as select * from wikidata w ;

-- Change the Paris foreign keys in the copied wikidata table
update working_wiki w
inner join (
	select w.* 
	from working_wiki w 
	where exists (
		select * 
		from paris p
		where w.cityGeoName = p.id
	)
) p on w.id = p.id
set w.cityGeoName = @p1;