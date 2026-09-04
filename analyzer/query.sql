        WITH base AS (
            SELECT vz.id, vz."tsDetected", vz.mmsi, vz."navStatus", vz."navStatusDesc", 
                vz.longitude, vz.latitude, vz."tsCurrent", vz."tsOut", vz.zone,
                CASE
                    WHEN vz.cog IS NULL THEN vp.cog 
                    ELSE vz.cog
                END AS cog,
                CASE
                    WHEN vz.sog IS NULL THEN vp.sog 
                    ELSE vz.sog
                END AS sog,		
                CASE
                    WHEN vz.rot IS NULL THEN vp.rot 
                    ELSE vz.rot
                END AS rot,	
                CASE
                    WHEN vz."trueHeading" IS NULL THEN vp."trueHeading" 
                    ELSE vz."trueHeading"
                END AS "trueHeading",			
                CASE
                    WHEN vz.imo IS NULL THEN sd.imo 
                    ELSE vz.imo
                END AS imo,
				CASE 
					WHEN LENGTH(vz.imo::text) > 7 OR vl.imoshipno IS NULL THEN vz.imo
					ELSE vl.imoshipno 
				END AS lloyds_imo,
                CASE
                    WHEN vz."shipName" IS NULL THEN sd."shipName"
                    ELSE vz."shipName"
                END AS "shipName",
                sd."shipName" AS staticshipname,
                CASE
                    WHEN vz."shipType" IS NULL THEN sd."shipType"
                    ELSE vz."shipType"
                END AS "shipType",	
                CASE
                    WHEN vz."shipTypeDesc" IS NULL THEN sd."shipTypeDesc"
                    ELSE vz."shipTypeDesc"
                END AS "shipTypeDesc",		
                CASE
                    WHEN vz."callsign" IS NULL THEN sd.callsign
                    ELSE vz."callsign"
                END AS callsign,
                CASE
                    WHEN vz."destination" IS NULL THEN sd.destination
                    ELSE vz."destination"
                END AS destination,	
                CASE
                    WHEN vz."draught" IS NULL THEN sd.draught
                    ELSE vz."draught"
                END AS draught,		
                vl.flagname,
                LAG("tsOut") OVER (PARTITION BY vz.mmsi ORDER BY vz."tsDetected") AS exist_sector_ts	
            FROM public.ais_vesselinzone vz
            JOIN public.ais_position vp ON vp.mmsi = vz.mmsi
			JOIN public.ais_static sd ON sd.mmsi = vz.mmsi
            LEFT JOIN public.ais_lloyds vl ON vl.maritimemobileserviceidentitymmsinumber = vz.mmsi
            WHERE vz.zone >= 20
            AND (
                vz."tsDetected" >= NOW() - INTERVAL '15 DAYS'
                OR vz."tsOut" IS NULL
                OR vz."tsCurrent" >= NOW() - INTERVAL '15 DAYS'
            )
        ),
        with_prev AS (
            SELECT
                b.*,
                LAG(COALESCE(b."tsOut", b."tsCurrent")) OVER (
                    PARTITION BY b.mmsi ORDER BY b."tsDetected"
                ) AS prev_ts_out,
                LAG(b.zone) OVER (PARTITION BY b.mmsi ORDER BY b."tsDetected") AS prev_zone
            FROM base b
        ),
        with_steps AS (
            SELECT
                w.*,
                w.zone - w.prev_zone AS zone_step,
                LAG(w.zone - w.prev_zone) OVER (PARTITION BY w.mmsi ORDER BY w."tsDetected") AS prev_zone_step
            FROM with_prev w
        ),
        with_route_start AS (
            SELECT
                s.*,
                CASE
                    WHEN s.prev_zone IS NULL THEN 1
                    WHEN NOT (
                        s.prev_ts_out = s."tsDetected"
                        OR (
                            ABS(s.zone - s.prev_zone) <= 1
                            AND s."tsDetected" - s.prev_ts_out <= INTERVAL '10 minutes'
                        )
                    ) THEN 1
                    WHEN s.zone_step = 0 THEN
                        CASE WHEN s.prev_zone_step < 0 THEN 0 ELSE 1 END
                    WHEN s.prev_zone_step IS NULL OR s.prev_zone_step = 0 THEN 0
                    WHEN SIGN(s.zone_step) = SIGN(s.prev_zone_step) THEN 0
                    ELSE 1
                END AS is_route_start
            FROM with_steps s
        ),
        with_route AS (
            SELECT
                r.*,
                SUM(r.is_route_start) OVER (PARTITION BY r.mmsi ORDER BY r."tsDetected") AS route_id
            FROM with_route_start r
        ),
        route_bounds AS (
            SELECT
                route_id,
                mmsi,
                (ARRAY_AGG(zone ORDER BY "tsDetected"))[1] AS first_zone,
                (ARRAY_AGG(zone ORDER BY "tsDetected" DESC))[1] AS last_zone,
                (ARRAY_AGG(prev_zone ORDER BY "tsDetected"))[1] AS zone_before_route,
                (ARRAY_AGG(cog ORDER BY "tsDetected" DESC))[1] AS route_cog,
                (ARRAY_AGG("tsOut" ORDER BY "tsDetected" DESC))[1] IS NULL AS route_still_open
            FROM with_route
            GROUP BY route_id, mmsi
        ),
        route_direction AS (
            SELECT
                route_id,
                mmsi,
                COALESCE(
                    CASE
                        WHEN last_zone > first_zone THEN 'Eastbound'
                        WHEN last_zone < first_zone THEN 'Westbound'
                        WHEN zone_before_route IS NOT NULL AND first_zone > zone_before_route THEN 'Eastbound'
                        WHEN zone_before_route IS NOT NULL AND first_zone < zone_before_route THEN 'Westbound'
                    END,
                    CASE
                        WHEN route_still_open AND route_cog >= 80 AND route_cog <= 260 THEN 'Eastbound'
                        WHEN route_still_open AND route_cog IS NOT NULL
                            AND ((route_cog >= 0 AND route_cog < 80) OR (route_cog > 260 AND route_cog <= 360))
                        THEN 'Westbound'
                    END
                ) AS direction
            FROM route_bounds
        )
        SELECT
            --wr.id,
            wr."tsDetected" AS datetime_entry,
            wr.mmsi,
            wr.imo AS imoshipno,
            wr."shipName" AS ontsshipname,
			wr.staticshipname AS shipname,
            wr.callsign,
            wr.flagname,
            --wr."navStatus",
            wr."navStatusDesc",
            CASE
                WHEN wr."shipTypeDesc" LIKE '%Bulk%' AND wr."draught" >= 15 THEN 'Deep Draught Bulk Carrier'
                WHEN wr."shipTypeDesc" LIKE '%Bulk%' AND wr."draught" < 15 THEN 'Bulk Carrier'
                WHEN wr."shipTypeDesc" LIKE '%Container%' AND wr."draught" >= 15 THEN 'Deep Draught Container'
                WHEN wr."shipTypeDesc" LIKE '%Container%' AND wr."draught" < 15 THEN 'Container'
                WHEN wr."shipTypeDesc" LIKE '%LNG%' OR wr."shipTypeDesc" LIKE '%LPG%' THEN 'LNG/LPG'
                WHEN wr."shipTypeDesc" LIKE '%Bunkering Tanker%' THEN 'Oil Tanker'
                WHEN wr."shipTypeDesc" LIKE '%Chemical Tanker%' THEN 'Chemical Tanker'
                WHEN wr."shipTypeDesc" LIKE '%Tanker%' AND wr."draught" >= 15 THEN 'Deep Draught Tanker'
                WHEN wr."shipTypeDesc" LIKE '%Tanker%' AND wr."draught" < 15 THEN 'Tanker'
                WHEN wr."shipTypeDesc" LIKE '%Cargo%' THEN 'General Cargo Ship'
                WHEN wr."shipTypeDesc" LIKE '%Offshore%' THEN 'Offshore Support Vessel'
                WHEN wr."shipTypeDesc" LIKE '%Specialized%' THEN 'Specialized Support Vessel'
                WHEN wr."shipTypeDesc" LIKE '%Tug%' THEN 'Tug'
                WHEN wr."shipTypeDesc" LIKE '%Passenger%' THEN 'Passenger Ship'
                WHEN wr."shipTypeDesc" LIKE '%Ro-Ro%' THEN 'Ro-Ro'
                WHEN wr."shipTypeDesc" LIKE '%Fish%' THEN 'Fishing Vessel'
                WHEN wr."shipTypeDesc" LIKE '%Dredg%' THEN 'Dredgers'
                WHEN wr."shipTypeDesc" LIKE '%Research%' THEN 'Research Survey Vessel'
                WHEN wr."shipTypeDesc" LIKE '%Livestock%' THEN 'Livestock Carrier'
                WHEN wr."shipType" = 35 THEN 'Military'
                ELSE 'Others'
            END AS shiptype,	
            wr.longitude,
            wr.latitude,
            --wr."tsCurrent",
            wr."tsOut" AS datetime_out,
            CASE
                WHEN wr.zone = 21 THEN 'Sector 1'
                WHEN wr.zone = 22 THEN 'Sector 2'
                WHEN wr.zone = 23 THEN 'Sector 3'
                WHEN wr.zone = 24 THEN 'Sector 4'
                WHEN wr.zone = 25 THEN 'Sector 5'
                WHEN wr.zone = 26 THEN 'Sector 6'
            END AS sector,
            wr.cog,
            wr.sog,
            wr.rot,
            wr."trueHeading",
			CASE
            	WHEN rd.direction IS NULL THEN 'Others'
				ELSE rd.direction
			END AS direction,
            wr.draught,
            wr.destination	
        FROM with_route wr
        INNER JOIN route_direction rd
            ON wr.route_id = rd.route_id
            AND wr.mmsi = rd.mmsi
        --WHERE wr."tsDetected" IS DISTINCT FROM wr.exist_sector_ts
		WHERE (wr.exist_sector_ts IS NULL
		   or (wr."tsDetected" < wr.exist_sector_ts - interval '3 minutes'
		   or wr."tsDetected" > wr.exist_sector_ts + interval '3 minutes'))
		   AND (wr.imo = CASE WHEN wr.lloyds_imo IS NOT NULL THEN wr.lloyds_imo ELSE wr.imo END)	
        ORDER BY wr.mmsi, wr."tsDetected";


--Interesting MMSI::
--1. 341395002 - imo from lloyds only 9106699 but system is 910669900 (ENAV, PNAV, LLI, KPLER)
--2. 564149000 - system has multiple fake imo (9371685, 9371687), the actual real imo is 9421269
--3. 636026025 - lloyds does not have imo for this mmsi. the imo = 9839296 