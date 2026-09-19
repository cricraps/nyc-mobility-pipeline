-- TAXI ZONES DATA QUALITY CHECK --

-- STATUS DETERMINATION GUIDE:
-- PASS: No issues detected
-- WARN: Acceptable placeholder values ('Unknown', 'N/A') present, but <= 5% of total rows
-- FAIL: One of the following conditions met:
--       - Critical issues: missing PKs, invalid types, out-of-range values, truly invalid categories
--       - Placeholder values exceed 5% threshold
--       - Any truly unrecognized/invalid values found (not 'Unknown'/'N/A')

-- KEY RULES:
-- - 'Unknown' and 'N/A' values are acceptable placeholders → WARN (if <= 5%)
-- - Any percentage > 5% → FAIL (even for placeholders)
-- - Truly invalid values (not in expected list, excluding placeholders) → always FAIL

WITH raw AS (
    SELECT 
        LocationID, 
        Borough, 
        Zone, 
        service_zone
    FROM nyc_mobility.raw.taxi_zones
),
 
clean AS (
    SELECT 
        location_id, 
        borough, 
        zone, 
        service_zone
    FROM nyc_mobility.clean.taxi_zones
),
 
flagged AS (
    SELECT
        location_id, 
        borough, 
        zone, 
        service_zone,
        CASE 
            WHEN borough IN ('Unknown', 'N/A') THEN 'placeholder' 
            ELSE 'valid' 
        END AS borough_flag,
        CASE 
            WHEN zone IN ('Unknown', 'N/A') THEN 'placeholder' 
            ELSE 'valid' 
        END AS zone_flag,
        CASE 
            WHEN service_zone IN ('Unknown', 'N/A') THEN 'placeholder' 
            ELSE 'valid' 
        END AS service_zone_flag
    FROM clean
),
 
totals AS (
    SELECT COUNT(*) AS n 
    FROM clean
)
 
-- location_id — completeness (missing pk = fail)
SELECT
    'location_id' AS column, 
    'completeness' AS data_quality_check,
    COUNT_IF(c.location_id IS NULL) AS failed_rows,
    MAX(t.n) AS total_rows,
    ROUND(COUNT_IF(c.location_id IS NULL) * 100.0 / MAX(t.n), 2) AS percentage,
    CASE 
        WHEN COUNT_IF(c.location_id IS NULL) = 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END AS status
FROM clean c 
CROSS JOIN totals t
 
-- location_id — uniqueness (duplicate pk = fail)
UNION ALL
SELECT
    'location_id', 
    'uniqueness',
    COUNT(*) - COUNT(DISTINCT c.location_id),
    MAX(t.n),
    ROUND((COUNT(*) - COUNT(DISTINCT c.location_id)) * 100.0 / MAX(t.n), 2),
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT c.location_id) THEN 'PASS' 
        ELSE 'FAIL' 
    END
FROM clean c 
CROSS JOIN totals t
 
-- location_id — validity (fails raw type cast)
UNION ALL
SELECT
    'location_id', 
    'validity',
    COUNT_IF(TRY_CAST(r.LocationID AS INT) IS NULL),
    MAX(t.n),
    ROUND(COUNT_IF(TRY_CAST(r.LocationID AS INT) IS NULL) * 100.0 / MAX(t.n), 2),
    CASE 
        WHEN COUNT_IF(TRY_CAST(r.LocationID AS INT) IS NULL) = 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END
FROM raw r 
CROSS JOIN totals t
 
-- location_id — accuracy (outside the real TLC-published range)
UNION ALL
SELECT
    'location_id', 
    'accuracy',
    COUNT_IF(c.location_id < 1 OR c.location_id > 265),
    MAX(t.n),
    ROUND(COUNT_IF(c.location_id < 1 OR c.location_id > 265) * 100.0 / MAX(t.n), 2),
    CASE 
        WHEN COUNT_IF(c.location_id < 1 OR c.location_id > 265) = 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END
FROM clean c 
CROSS JOIN totals t
 
-- borough — completeness (null NOT confined to known placeholder ids = fail)
UNION ALL
SELECT
    'borough', 
    'completeness',
    COUNT_IF(c.borough IS NULL AND c.location_id NOT IN (264, 265)),
    MAX(t.n),
    ROUND(COUNT_IF(c.borough IS NULL AND c.location_id NOT IN (264, 265)) * 100.0 / MAX(t.n), 2),
    CASE 
        WHEN COUNT_IF(c.borough IS NULL AND c.location_id NOT IN (264, 265)) = 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END
FROM clean c 
CROSS JOIN totals t
 
-- borough — validity (placeholder values = WARN)
UNION ALL
SELECT
    'borough', 
    'validity',
    COUNT_IF(f.borough IN ('Unknown', 'N/A', 'N/a')),
    MAX(t.n),
    ROUND(COUNT_IF(f.borough IN ('Unknown', 'N/A', 'N/a')) * 100.0 / MAX(t.n), 2),
    CASE 
        WHEN COUNT_IF(f.borough IN ('Unknown', 'N/A', 'N/a')) * 100.0 / MAX(t.n) > 5 THEN 'FAIL'
        WHEN COUNT_IF(f.borough IN ('Unknown', 'N/A', 'N/a')) > 0 THEN 'WARN' 
        ELSE 'PASS' 
    END
FROM flagged f 
CROSS JOIN totals t
 
-- borough — validity (truly invalid values = FAIL)
UNION ALL
SELECT
    'borough', 
    'validity',
    COUNT_IF(f.borough IS NOT NULL AND f.borough NOT IN ('Manhattan','Brooklyn','Queens','Bronx','Staten Island','EWR','Unknown','N/A', 'N/a')),
    MAX(t.n),
    ROUND(COUNT_IF(f.borough IS NOT NULL AND f.borough NOT IN ('Manhattan','Brooklyn','Queens','Bronx','Staten Island','EWR','Unknown','N/A', 'N/a')) * 100.0 / MAX(t.n), 2),
    CASE 
        WHEN COUNT_IF(f.borough IS NOT NULL AND f.borough NOT IN ('Manhattan','Brooklyn','Queens','Bronx','Staten Island','EWR','Unknown','N/A', 'N/a')) > 0 THEN 'FAIL' 
        ELSE 'PASS' 
    END
FROM flagged f 
CROSS JOIN totals t
 
-- zone — completeness (null NOT confined to known placeholder ids = fail)
UNION ALL
SELECT
    'zone', 
    'completeness',
    COUNT_IF(c.zone IS NULL AND c.location_id NOT IN (264, 265)),
    MAX(t.n),
    ROUND(COUNT_IF(c.zone IS NULL AND c.location_id NOT IN (264, 265)) * 100.0 / MAX(t.n), 2),
    CASE 
        WHEN COUNT_IF(c.zone IS NULL AND c.location_id NOT IN (264, 265)) = 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END
FROM clean c 
CROSS JOIN totals t
 
-- zone — uniqueness (duplicate names — known/expected, so warn not fail)
UNION ALL
SELECT
    'zone', 
    'uniqueness',
    COUNT(*) - COUNT(DISTINCT f.zone) - COUNT_IF(f.zone IS NULL),
    MAX(t.n),
    ROUND((COUNT(*) - COUNT(DISTINCT f.zone) - COUNT_IF(f.zone IS NULL)) * 100.0 / MAX(t.n), 2),
    CASE 
        WHEN (COUNT(*) - COUNT(DISTINCT f.zone) - COUNT_IF(f.zone IS NULL)) <= 3 THEN 'PASS' 
        ELSE 'WARN' 
    END
FROM flagged f 
CROSS JOIN totals t
 
-- zone — validity ('Unknown'/'N/A' placeholder = WARN, never FAIL; no fixed enum to check against otherwise)
UNION ALL
SELECT
    'zone', 
    'validity',
    COUNT_IF(f.zone_flag = 'placeholder'),
    MAX(t.n),
    ROUND(COUNT_IF(f.zone_flag = 'placeholder') * 100.0 / MAX(t.n), 2),
    CASE 
        WHEN COUNT_IF(f.zone_flag = 'placeholder') * 100.0 / MAX(t.n) > 5 THEN 'FAIL'
        WHEN COUNT_IF(f.zone_flag = 'placeholder') > 0 THEN 'WARN' 
        ELSE 'PASS' 
    END
FROM flagged f 
CROSS JOIN totals t
 
-- service_zone — completeness (null NOT confined to known placeholder ids = fail)
UNION ALL
SELECT
    'service_zone', 
    'completeness',
    COUNT_IF(c.service_zone IS NULL AND c.location_id NOT IN (264, 265)),
    MAX(t.n),
    ROUND(COUNT_IF(c.service_zone IS NULL AND c.location_id NOT IN (264, 265)) * 100.0 / MAX(t.n), 2),
    CASE 
        WHEN COUNT_IF(c.service_zone IS NULL AND c.location_id NOT IN (264, 265)) = 0 THEN 'PASS' 
        ELSE 'FAIL' 
    END
FROM clean c 
CROSS JOIN totals t
 
-- service_zone — validity (placeholder values = WARN)
UNION ALL
SELECT
    'service_zone', 
    'validity',
    COUNT_IF(f.service_zone IN ('Unknown', 'N/A', 'N/a')),
    MAX(t.n),
    ROUND(COUNT_IF(f.service_zone IN ('Unknown', 'N/A', 'N/a')) * 100.0 / MAX(t.n), 2),
    CASE 
        WHEN COUNT_IF(f.service_zone IN ('Unknown', 'N/A', 'N/a')) * 100.0 / MAX(t.n) > 5 THEN 'FAIL'
        WHEN COUNT_IF(f.service_zone IN ('Unknown', 'N/A', 'N/a')) > 0 THEN 'WARN' 
        ELSE 'PASS' 
    END
FROM flagged f 
CROSS JOIN totals t
 
-- service_zone — validity (truly invalid values = FAIL)
UNION ALL
SELECT
    'service_zone', 'validity',
    COUNT_IF(f.service_zone IS NOT NULL AND f.service_zone NOT IN ('Yellow Zone','Boro Zone','Airports','EWR','Unknown','N/A', 'N/a')),
    MAX(t.n),
    ROUND(COUNT_IF(f.service_zone IS NOT NULL AND f.service_zone NOT IN ('Yellow Zone','Boro Zone','Airports','EWR','Unknown','N/A', 'N/a')) * 100.0 / MAX(t.n), 2),
    CASE 
        WHEN COUNT_IF(f.service_zone IS NOT NULL AND f.service_zone NOT IN ('Yellow Zone','Boro Zone','Airports','EWR','Unknown','N/A', 'N/a')) > 0 THEN 'FAIL' 
        ELSE 'PASS' 
    END
FROM flagged f 
CROSS JOIN totals t;