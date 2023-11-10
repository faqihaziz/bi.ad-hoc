WITH sending_th_ori AS (
  
  SELECT

  -- waybill_no,
  th_ori_sending,
  th_ori_sending_time,
  month_th_ori_sending,
  th_ori_bag_no,
  -- th_ori_bagging_alias,


  FROM (

    SELECT

    wl.waybill_no,
    wl.operation_branch_name AS th_ori_sending,
    DATE(wl.record_time, 'Asia/Jakarta') AS th_ori_sending_time,
    FORMAT_DATE("%b %Y", DATE(wl.record_time,'Asia/Jakarta')) month_th_ori_sending,
    bag_no AS th_ori_bag_no,
    CASE
      WHEN bag_no LIKE '%B%' THEN 1
      WHEN bag_no IS NULL THEN 0
      ELSE 0 END AS th_ori_bagging_alias,

FROM `datawarehouse_idexp.waybill_waybill_line` wl

WHERE DATE(record_time,'Asia/Jakarta') BETWEEN '2023-09-01' AND '2023-10-29'
      AND operation_type = '04'
      AND SUBSTR(next_location_name,1,2) IN ('MH','DC')
      AND SUBSTR(operation_branch_name,1,2) = 'TH'

QUALIFY ROW_NUMBER() OVER (PARTITION BY wl.waybill_no,wl.operation_branch_name ORDER BY wl.waybill_no, wl.record_time ASC)=1
)
QUALIFY ROW_NUMBER() OVER (PARTITION BY th_ori_bag_no)=1
),

sending_dest AS (
  
  SELECT

  -- waybill_no,
  mh_dest_sending,
  mh_dest_sending_time,
  month_mh_dest_sending,
  mh_dest_bag_no,
  -- mh_dest_bagging_alias,

  
  FROM (
    
    SELECT
    wl.waybill_no,
    wl.operation_branch_name AS mh_dest_sending,
    DATE(wl.record_time, 'Asia/Jakarta') AS mh_dest_sending_time,
    FORMAT_DATE("%b %Y", DATE(wl.record_time,'Asia/Jakarta')) AS month_mh_dest_sending,
    bag_no AS mh_dest_bag_no,
    CASE
      WHEN bag_no LIKE '%B%' THEN 1
      WHEN bag_no IS NULL THEN 0
      ELSE 0 END AS mh_dest_bagging_alias,

FROM `datawarehouse_idexp.waybill_waybill_line` wl

WHERE DATE(record_time,'Asia/Jakarta') BETWEEN '2023-09-01' AND '2023-10-29'
      AND operation_type = '04'
      AND SUBSTR(operation_branch_name,1,2) IN ('MH','DC')
      AND SUBSTR(next_location_name,1,2) IN ('TH','VH','VT')
      
  QUALIFY ROW_NUMBER() OVER (PARTITION BY wl.waybill_no ORDER BY wl.record_time DESC)=1
)
QUALIFY ROW_NUMBER() OVER (PARTITION BY mh_dest_bag_no)=1
)

-- SELECT 

-- th_ori_sending,
-- month_th_ori_sending,
-- SUM(th_ori_bagging_alias) total_th_ori_bagging,
-- mh_dest_sending,
-- month_mh_dest_sending,
-- SUM(mh_dest_bagging_alias) total_mh_dest_bagging,



-- FROM (

--   SELECT

--   -- a.waybill_no,
--   a.th_ori_sending,
--   th_ori_sending_time,
--   a.month_th_ori_sending,
--   a.th_ori_bag_no,
--   a.th_ori_bagging_alias,
--   b.mh_dest_sending,
--   b.mh_dest_sending_time,
--   b.month_mh_dest_sending,
--   b.mh_dest_bag_no,
--   b.mh_dest_bagging_alias,

-- FROM sending_th_ori a
-- LEFT JOIN sending_dest b ON a.waybill_no = b.waybill_no
-- )
-- GROUP BY 1,2,4,5

-------------- hitung baggiing sending th origin------------------------
-- SELECT

-- th_ori_sending,
-- month_th_ori_sending,
-- SUM(th_ori_bagging_alias) total_th_ori_bagging,

-- FROM (

--   SELECT *,
--   CASE
--       WHEN th_ori_bag_no LIKE '%B%' THEN 1
--       -- WHEN th_ori_bag_no IS NOT NULL THEN 1
--       WHEN th_ori_bag_no IS NULL THEN 0
--       ELSE 0 END AS th_ori_bagging_alias,

-- FROM sending_th_ori
-- -- ORDER BY th_ori_sending ASC, th_ori_sending_time ASC
-- ORDER BY th_ori_bag_no ASC
-- )

-- GROUP BY 1,2

-- WHERE th_ori_bagging_alias = 0

--------------- hitung bagging sending mh dest --------------------

SELECT 

mh_dest_sending,
month_mh_dest_sending,
SUM(mh_dest_bagging_alias) total_mh_dest_bagging,

FROM (

  SELECT *,
  CASE
      WHEN mh_dest_bag_no LIKE '%B%' THEN 1
      WHEN mh_dest_bag_no IS NULL THEN 0
      ELSE 0 END AS mh_dest_bagging_alias,

  FROM sending_dest
)
GROUP BY 1,2
