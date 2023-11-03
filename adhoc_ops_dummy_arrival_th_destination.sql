---dummy_arrival_th_destination---

WITH waybill_line AS (WITH PrevNextBranches AS (
                                                SELECT
                                                    currenttab.waybill_no,
                                                    option.option_name AS operation_type,
                                                    currenttab.operation_branch_name AS operation_branch_name,
                                                    currenttab.record_time AS record_time,
                                                    LAG(currenttab.operation_branch_name,1) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_branch_name_1,
                                                    LAG(currenttab.operation_branch_name,2) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_branch_name_2,
                                                    LEAD(currenttab.operation_branch_name) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_branch_name,
                                                    MAX(currenttab.operation_branch_name) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time DESC) AS last_location,
                                                FROM
                                                    `datawarehouse_idexp.dm_waybill_waybill_line` AS currenttab
                                                LEFT JOIN
                                                    `datawarehouse_idexp.system_option` AS option
                                                ON
                                                    currenttab.operation_type = option.option_value 
                                                    AND option.type_option = 'operationType'
                                                WHERE DATE(currenttab.record_time,'Asia/Jakarta') BETWEEN '2023-08-01' AND CURRENT_DATE() -->= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))
                                                    AND currenttab.deleted = '0'
                                                )
                                                SELECT *, 
                                                FROM (
                                                SELECT
                                                p.waybill_no,
                                                p.operation_type,
                                                COALESCE(p.previous_branch_name_2, "N/A") AS previous_branch_name_2,
                                                COALESCE(p.previous_branch_name_1, "N/A") AS previous_branch_name_1,
                                                p.operation_branch_name,
                                                p.record_time,
                                                COALESCE(p.next_branch_name, "N/A") AS next_branch_name,
                                                CASE WHEN SUBSTR(previous_branch_name_1,1,2) IN ('MH','DC') 
                                                    AND SUBSTR(operation_branch_name,1,2) IN ('TH','VT','VH') THEN 'Last Mile'
                                                    WHEN SUBSTR(previous_branch_name_2,1,2) IN ('MH','DC') AND SUBSTR(previous_branch_name_1,1,2) NOT IN ('MH','DC') 
                                                    AND SUBSTR(operation_branch_name,1,2) IN ('TH','VT','VH') THEN 'Last Mile'
                                                    WHEN SUBSTR(previous_branch_name_1,1,2) IN ('TH') 
                                                    AND SUBSTR(operation_branch_name,1,2) IN ('VT', 'VH') THEN 'Last Mile'
                                                    ELSE NULL END AS last_mile_or_not,
                                                FROM
                                                PrevNextBranches AS p
                                                WHERE operation_branch_name <> previous_branch_name_1
                                                -- OR previous_branch_name_1 <> previous_branch_name_2
                                                OR operation_branch_name <> next_branch_name
                                                )
                                                WHERE last_mile_or_not = 'Last Mile')
,arrival_lm AS (
                        SELECT DISTINCT
                                                ww.waybill_no, 
                                                DATETIME(ww.pickup_record_time,'Asia/Jakarta') AS pickup_record_time,
                                                ww.pickup_branch_name,
                                                ww.sender_city_name AS sender_city_name,
                                                ww.recipient_province_name AS destination_province,
                                                ww.recipient_city_name AS destination_city,
                                                ww.recipient_district_name AS destination_district,
                                                kn.kanwil_name AS kanwil_name,
                                                rd3.option_name AS express_type,
                                                rd4.option_name AS waybill_source,
                                                rd5.option_name AS service_type,
                                                ww.vip_customer_name,

                                                -- tambahan kolom
                                                ww.recipient_name,
                                                ww.recipient_address,
                                                ww.recipient_cellphone,
                                                IF(ww.cod_amount > 0, 'COD', 'Non-COD') AS cod_flag,

                                                CASE WHEN ww.pickup_branch_name = ww.pod_branch_name THEN ww.pickup_branch_name
                                                  WHEN ww.pickup_branch_name = ww.delivery_branch_name THEN ww.pickup_branch_name
                                                  ELSE sc.operation_branch_name END AS th_arrival,
                                                CASE WHEN ww.pickup_branch_name = ww.pod_branch_name THEN DATETIME(ww.pickup_record_time,'Asia/Jakarta')
                                                  WHEN ww.pickup_branch_name = ww.delivery_branch_name THEN DATETIME(ww.pickup_record_time,'Asia/Jakarta')
                                                  ELSE DATETIME(sc.record_time, 'Asia/Jakarta') END AS arrival_time,
                                                TIME(sc.record_time, 'Asia/Jakarta') AS arrival_time_hour,
                                                DATETIME(ps.operation_time, 'Asia/Jakarta') AS problem_shipment_time, 
                                                ps.problem_reason,
                                                a1.sorting_code AS adjusted_area_code,
                                                a1.lastmile_duration_hour AS adjusted_area_duration,
                                                IF(TIME(sc.record_time, 'Asia/Jakarta') >= "14:00:00",1,0) AS arrive_past_1400,
                                                -- CASE WHEN SUBSTR(sc.previous_branch_name,1,2) IN ('MH','DC') 
                                                -- AND SUBSTR(sc.operation_branch_name,1,2) IN ('TH','VTH','VH') THEN 'Last Mile'
                                                --       WHEN SUBSTR(sc.previous_branch_name,1,2) IN ('TH') 
                                                -- AND SUBSTR(sc.operation_branch_name,1,3) = 'VTH' THEN 'Last Mile'
                                                --        ELSE NULL END AS last_mile_or_not,
                                                sc.last_mile_or_not,
                                                rd1.option_name AS waybill_status,
                                                CASE WHEN ww.pod_record_time IS NOT NULL THEN ww.pod_branch_name
                                                    WHEN ww.pod_record_time IS NULL THEN ww.delivery_branch_name
                                                    WHEN ww.delivery_branch_name IS NULL THEN sc.operation_branch_name
                                                    END AS delivery_or_pod_branch,
                                                CASE WHEN rr.return_pod_flag = '1' AND rr.return_record_time >= sc.record_time THEN 'Return Received'
                                                WHEN rr.return_pod_flag = '1' AND rr.return_record_time < sc.record_time THEN 'Last Mile Return'
                                                WHEN (rr.return_confirm_status = '01' AND return_void_flag ='0') AND rr.return_confirm_record_time >= sc.record_time THEN 'Return Confirmed'
                                                WHEN (rr.return_confirm_status = '01' AND return_void_flag ='0') AND rr.return_confirm_record_time < sc.record_time THEN 'Last Mile Return'
                                                WHEN (rr.return_confirm_status = '00' AND return_void_flag ='0') AND rr.return_record_time >= sc.record_time  THEN 'Return Registration'
                                                WHEN (rr.return_confirm_status = '00' AND return_void_flag ='0') AND rr.return_record_time < sc.record_time  THEN 'Last Mile Return'
                                                ELSE NULL END AS return_package,
                                                DATETIME(rr.return_record_time,'Asia/Jakarta') AS return_regist_time,
                                                DATETIME(ww.pod_record_time, 'Asia/Jakarta') AS pod_time,
                                                CASE WHEN ww.express_type = '06' THEN DATE(DATE_ADD(DATE(ww.shipping_time,'Asia/Jakarta'), INTERVAL CAST(sla.sla_cargo AS INTEGER) DAY))
                                                    ELSE DATE(DATE_ADD(DATE(ww.shipping_time,'Asia/Jakarta'), INTERVAL CAST(sla.sla AS INTEGER) DAY)) END AS deadline,
                                                CASE WHEN ww.express_type = '06' THEN CAST(sla.sla_cargo AS INTEGER)
                                                    ELSE CAST(sla.sla AS INTEGER) END AS slashopee,
                                                DATETIME(ww.update_time, 'Asia/Jakarta') AS update_time,
                                                FROM `datawarehouse_idexp.waybill_waybill` ww
                                                LEFT OUTER JOIN waybill_line sc ON sc.waybill_no = ww.waybill_no
                                                -- LEFT OUTER JOIN `datawarehouse_idexp.dm_waybill_waybill_line` sc ON sc.waybill_no = ww.waybill_no AND sc.deleted = '0' AND sc.operation_type = '05' --AND SUBSTR(sc.previous_branch_name,1,2) IN ('MH','DC') AND SUBSTR(sc.operation_branch_name,1,2) = 'TH'
                                                -- AND DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY)) --6499455
                                                LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON rr.waybill_no = ww.waybill_no AND ww.deleted = '0' 
                                                AND DATE(rr.shipping_time,'Asia/Jakarta') BETWEEN '2023-08-01' AND CURRENT_DATE() -->= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY)) 
                                                LEFT OUTER JOIN `datawarehouse_idexp.waybill_problem_piece` ps ON ps.waybill_no = ww.waybill_no AND ps.deleted = '0' 
                                                AND DATE(ps.operation_time,'Asia/Jakarta') BETWEEN '2023-08-01' AND CURRENT_DATE() -->= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY)) 
                                                LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd1 ON ww.waybill_status = rd1.option_value AND rd1.type_option = 'waybillStatus' AND rd1.deleted=0
                                                LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd2 ON sc.operation_type = rd2.option_value AND rd2.type_option = 'operationType' AND rd2.deleted=0
                                                LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd3 ON ww.express_type = rd3.option_value AND rd3.type_option = 'expressType' AND rd3.deleted=0
                                                LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd4 ON ww.waybill_source = rd4.option_value AND rd4.type_option = 'waybillSource' AND rd4.deleted=0
                                                LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd5 ON ww.service_type = rd5.option_value AND rd5.type_option = 'serviceType' AND rd5.deleted=0
                                                LEFT OUTER JOIN `datamart_idexp.mapping_kanwil_area` kn ON ww.recipient_province_name = kn.province_name
                                                LEFT OUTER JOIN `datamart_idexp.productivity_adjusted_sla_lastmile_area` a1 ON a1.sorting_code = ww.sorting_code
                                                -- LEFT JOIN `datamart_idexp.masterdata_sla_shopee` sla ON sla.destination_city = ww.recipient_city_name 
                                                -- AND sla.origin_city = ww.sender_city_name
                                                LEFT JOIN `datamart_idexp.masterdata_sla_shopee_and_cargo` sla ON sla.destination_city = ww.recipient_city_name )

        SELECT * FROM arrival_lm
