WITH data_lm_raw AS (
SELECT

waybill_no,
arrival_time,
th_arrival,
destination_district,
destination_city,
destination_province,
CONCAT(destination_province,"-",destination_city,"-",destination_district) concat_district,

-- FROM `datamart_idexp.dashboard_productivity_lm`
FROM `dev_idexp.dummy_arrival_93days`
WHERE DATE(arrival_time) BETWEEN '2024-03-01' AND '2024-04-30'

),

count_arrival_all AS (

    SELECT

    DATE(arrival_time) arrival_date,
    th_arrival,
    destination_district,
    concat_district,
    COUNT(waybill_no) total_arrival_all,
    0 AS total_arrival_district,

    FROM data_lm_raw

    GROUP BY 1,2,3,4
),

count_arrival_district AS (

    SELECT

    DATE(arrival_time) arrival_date,
    th_arrival,
    destination_district,
    concat_district,
    0 AS total_arrival_all,
    COUNT(waybill_no) total_arrival_district,

    FROM data_lm_raw

    WHERE concat_district IN (
    "GORONTALO-GORONTALO UTARA-ANGGREK",
"GORONTALO-GORONTALO UTARA-ATINGGOLA",
"GORONTALO-GORONTALO UTARA-BIAU",
"GORONTALO-GORONTALO UTARA-GENTUMA RAYA",
"GORONTALO-GORONTALO UTARA-KWANDANG"
    )

    GROUP BY 1,2,3,4
),

gabung_all AS (

    SELECT

    arrival_date,
    SUM(total_arrival_all) total_arrival_all,
    SUM(total_arrival_district) total_arrival_district,

FROM (
    SELECT

    arrival_date,
    total_arrival_all,
    total_arrival_district,

    FROM count_arrival_all UNION ALL

    SELECT

arrival_date,
    total_arrival_all,
    total_arrival_district,

    FROM count_arrival_district
)
GROUP BY 1
)

-- SELECT * FROM count_arrival_all
SELECT * FROM gabung_all
-- SELECT * FROM count_arrival_district
ORDER BY arrival_date
