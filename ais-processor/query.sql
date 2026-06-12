# clean data
UPDATE public.ais_vesselinzone
SET "tsOut" = now()
WHERE mmsi IN (
	SELECT mmsi --,* 
	FROM public.ais_vesselinzone
	WHERE zone = 10 AND "tsOut" IS NULL 
		AND mmsi IN (
			SELECT mmsi
			FROM public.ais_vesselinzone
			WHERE zone = 2 AND "tsOut" IS NULL 	
		)
	ORDER BY "tsDetected"
) 
AND zone = 11
AND "tsOut" IS NULL 
AND "tsDetected" < '2025-09-13 07:38:15.177'


SELECT vz."tsDetected" AS datetime_entry, 
	vz.mmsi, 
	vz."navStatusDesc", 
	vz.longitude, 
	vz.latitude,
	CASE 
		WHEN vz.zone = 10 THEN 'TSS-Northbound'
		WHEN vz.zone = 11 THEN 'TSS-Southbound'
	END AS direction,
	vl.imoshipno, 
	vl.shipname, 
	vl.callsign, 
	vl.flagname, 
	vl.shiptype,
	vl.draught,
	vp.sog,
	vp.cog,
	vp.rot,
	vp."trueHeading",
	vs.destination
FROM public.ais_vesselinzone vz
LEFT JOIN public.ais_lloyds vl ON vl.maritimemobileserviceidentitymmsinumber = vz.mmsi
INNER JOIN public.ais_position vp ON vp.mmsi = vz.mmsi
INNER JOIN public.ais_static vs ON vs.mmsi = vz.mmsi
WHERE vz."tsOut" IS null 
	AND vz.zone IN (10, 11)
ORDER BY datetime_entry


SELECT * 
FROM public.db_health
order by ts desc
limit 10


WITH vesselzone AS (
	SELECT *,
		row_number() OVER (PARTITION BY vz."tsDetected", vz.mmsi, vz.zone ORDER BY vz."tsOut" DESC) AS rowcount
	FROM public.ais_vesselinzone vz
	WHERE vz.zone IN (10, 11)
)
select * from vesselzone 
where vesselzone.rowcount > 1
--where mmsi = 305458968
--order by mmsi, "tsOut"
delete 
from public.ais_vesselinzone
where id in (select id from vesselzone where vesselzone.rowcount > 1)


select * 
from public.ais_position
order by ts desc
limit 100


select * 
from public.ais_static
order by ts desc
limit 100


select *
from public.ais_vesselinzone
order by "tsDetected" desc
limit 100
