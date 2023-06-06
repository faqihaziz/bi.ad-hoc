WITH mentah as (
    SELECT oo.*,
       ww.* EXCEPT(No_Waybill,update_time,division,sales_name),

    CASE WHEN oo.Order_Status = 'Cancel Order' THEN 'Cancel Order'
    WHEN ww.Waybill_Status IS NULL THEN 'Not Picked Up'
    WHEN ww.Waybill_Status IN ('Signed') OR ww.Waktu_POD IS NOT NULL THEN 'Delivered'
    WHEN  ww.Waybill_Status IN ('Return Received') OR ww.Waktu_Return_POD IS NOT NULL THEN 'Returned'
    WHEN ww.problem_reason LIKE '%bea cukai%' OR ww.problem_reason LIKE '%Rejected by customs%' THEN 'Undelivered'
    WHEN ww.problem_reason IN ('Kemasan paket rusak','Paket rusak/pecah', 'Kerusakan pada resi / informasi resi tidak jelas','Damaged parcels','Information on AWB is unclear/damage','Packaging is damage') THEN 'Undelivered'
    WHEN ww.problem_reason IN ('Paket hilang atau tidak ditemukan', 'Parcels is lost or cannot be found','Package is lost') then 'Undelivered'
    WHEN ww.Waybill_Status <> 'Return Received' AND ( ww.No_Waybill IS NOT NULL AND ww.Waktu_Confirm_Return is not null AND ww.Waktu_Return_POD IS NULL and ww.Waktu_POD is null) THEN 'Return Process'
    ELSE 'Delivery Process' END AS Last_Status,
--ww.Return_Status = 'Retur' OR      
FROM `dev_idexp.test_orderpickup_kpi_bd_v2` oo
LEFT OUTER JOIN `dev_idexp.test_delivery_kpi_seller_bd_v2` ww on oo.No_Waybill = ww.No_Waybill
),


-- GROUP BY 1,2,3,4,5
-- -- ORDER BY Lead_Time ASC

hitung AS (

    SELECT
    CASE WHEN No_Waybill IS NOT NULL THEN 1
    ELSE 0 END AS count_volume,
    FORMAT_DATE("%b %Y", DATE(Waktu_POD)) AS month_pod,
    Origin_Province, Origin_City,
Destination_Province, 
Destination_City, 
DATE(Pickup_Time) pickup_time,
DATE(Waktu_POD) waktu_pod,
-- APPROX_QUANTILES(Lead_Time, 100)[OFFSET(90)] AS percentile_90,
Lead_Time


    FROM mentah
)

SELECT 
month_pod,
Origin_Province, Origin_City,
Destination_Province, 
Destination_City, 
-- SUM(count_volume) AS monthly_volume,
-- MAX (Lead_Time) AS Max_Lead_Time,
APPROX_QUANTILES(Lead_Time, 100)[OFFSET(90)] AS percentile_90,
-- ROUND(SUM(Lead_Time) / ROUND(SUM(count_volume))) AS daily_volume,



FROM hitung

WHERE DATE(Waktu_POD) BETWEEN '2023-02-01' AND '2023-02-27'
AND Origin_Province IN ('DKI JAKARTA')

GROUP BY 1,2,3,4,5
