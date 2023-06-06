With blibli_pickup_performances_new AS (
    ----------------------------data_order_blibli-------------------------------

SELECT
a.waybill_no AS Waybill_No,
a.sender_name as Sender,
a.sender_address AS Sender_Address,
ww.sender_province_name AS Origin_Province,
ww.sender_city_name AS Origin_City,
ww.recipient_province_name AS Dest_Province,
ww.recipient_city_name AS Dest_City,
ww.recipient_district_name AS Dest_District,
t3.option_name as Order_Status,
sr.option_name as Order_Source,

DATETIME(a.input_time, 'Asia/Jakarta') Order_Time,
DATE(a.input_time, 'Asia/Jakarta') Order_Date,
TIME(a.input_time, 'Asia/Jakarta') Input_Time,
-- date_diff(DATE(oe.email_date), DATE(a.input_time, 'Asia/Jakarta'), DAY) AS Diff_email_to_input,

DATETIME(a.pickup_time, 'Asia/Jakarta') Waktu_Pengambilan,
DATE(a.pickup_time, 'Asia/Jakarta') Pickup_Time,
IF(TIME(a.input_time, 'Asia/Jakarta') >= "15:00:00",1,0) AS order_past_1500,
DATE_DIFF(DATE(a.pickup_time, 'Asia/Jakarta'), DATE(a.start_pickup_time, 'Asia/Jakarta'), Day) AS Pickup_Duration_Day,

DATETIME(a.start_pickup_time, 'Asia/Jakarta') start_pickup_time,
DATETIME(a.end_pickup_time, 'Asia/Jakarta') end_pickup_time,

CASE WHEN a.pickup_branch_name IS NOT NULL THEN mb.mitra_by_area
WHEN a.pickup_branch_name IS NULL THEN ma.mitra_by_area 
END AS Pickup_Area,
kw.kanwil_name AS Kanwil_Area,

CASE WHEN a.pickup_branch_name IS NULL THEN a.scheduling_target_branch_name
 WHEN a.pickup_branch_name IS NOT NULL THEN a.pickup_branch_name
END AS Origin_Branch,

--------------------pickup_performance------------------------------------

CASE 
    WHEN DATETIME(a.pickup_time, 'Asia/Jakarta') IS NOT NULL AND t3.option_name IN ('Picked Up','Cancel Order') THEN "No"
    WHEN DATETIME(a.pickup_time, 'Asia/Jakarta') IS NULL AND t3.option_name NOT IN ('Picked Up','Cancel Order') THEN "Yes"
    END AS Is_Backlog,

CASE 
    WHEN DATETIME(a.pickup_time, 'Asia/Jakarta') IS NULL AND t3.option_name NOT IN ('Picked Up','Cancel Order') THEN date_diff(current_date('Asia/Jakarta'), DATE(a.input_time, 'Asia/Jakarta'), DAY)
    WHEN DATETIME(a.pickup_time, 'Asia/Jakarta') IS NULL AND t3.option_name NOT IN ('Picked Up','Cancel Order') 
THEN date_diff(current_date('Asia/Jakarta'), DATE(a.input_time, 'Asia/Jakarta'), DAY)
    END AS Backlog_Pickup_Day,

-------------------------mapping_late_pickup_reason--------------------
CASE 
WHEN t4.register_reason_bahasa IS NULL THEN lpr.late_pickup_reason
ELSE t4.register_reason_bahasa
END AS Pickup_Failure_Reason,

DATETIME(a.pickup_failure_time) Pickup_Failure_Attempt,
  
CASE 
WHEN t4.register_reason_bahasa IS NULL THEN pf2.problem_factor
ELSE pf1.problem_factor
END AS Late_Pickup_Factor,

CASE 
WHEN t4.register_reason_bahasa IS NULL THEN pf2.problem_category
ELSE pf1.problem_category
END AS Late_Pickup_Category,

--------------------------------Shipment Status------------------------------------------------------
CASE WHEN a.order_status = '04' THEN 'Cancel Order'
    WHEN t1.option_name IS NULL OR ww.deleted = '1' THEN 'Not Picked Up'
    WHEN t1.option_name IN ('Signed') OR ww.pod_record_time IS NOT NULL THEN 'Delivered'
    WHEN t1.option_name IN ('Return Received') OR ww.return_pod_record_time IS NOT NULL THEN 'Returned'
    WHEN ps.problem_reason LIKE '%bea cukai%' OR ps.problem_reason LIKE '%Rejected by customs%' THEN 'Paket ditolak bea cukai (red line)'
    WHEN ps.problem_reason IN ('Kemasan paket rusak','Paket rusak/pecah', 'Kerusakan pada resi / informasi resi tidak jelas','Damaged parcels','Information on AWB is unclear/damage','Packaging is damage') THEN 'Damaged'
    WHEN ps.problem_reason IN ('Paket hilang atau tidak ditemukan', 'Parcels is lost or cannot be found','Package is lost') then 'Lost'
    
    WHEN ww.waybill_status <> '06' AND (ww.return_flag = '1' OR ww.return_waybill_no IS NOT NULL) AND rr.return_confirm_record_time IS NOT NULL AND ww.return_pod_record_time IS NULL THEN 'Return Process'

    ELSE 'Delivery Process' END AS Shipment_status,

-----------------------------------Delivery--------------------------------------------------------------------------
DATE(ww.pod_record_time,'Asia/Jakarta') AS POD_Date,
DATETIME(ww.pod_record_time,'Asia/Jakarta') AS POD_Time,
ww.pod_branch_name AS POD_Branch,

-------------mapping_city_category------------------------------
CASE 
WHEN ww.recipient_province_name IN ('DKI JAKARTA') THEN "JAKARTA"
WHEN ww.recipient_province_name NOT IN ('DKI JAKARTA') AND ww.recipient_city_name IN ("BOGOR","KOTA BOGOR","KOTA DEPOK","KOTA TANGERANG",
"TANGERANG","TANGERANG SELATAN","BEKASI","KOTA BEKASI") THEN "BODETABEK"
WHEN ww.recipient_province_name NOT IN ('DKI JAKARTA') AND ww.recipient_city_name NOT IN ("BOGOR","KOTA BOGOR","KOTA DEPOK","KOTA TANGERANG",
"TANGERANG","TANGERANG SELATAN","BEKASI","KOTA BEKASI") THEN "NON-JABODETABEK" 
END AS City_Category,

----------------shopee_delivery_performance-------------------------
cast(t6.sla as INTEGER) AS SLA_Delivery,
DATE(DATE_ADD(ww.shipping_time, INTERVAL (cast(t6.sla as INTEGER)) day)) AS Due_Date_Delivery,

CASE 
    WHEN cast(t6.sla as INTEGER) >= 999 THEN "No SLA (OoC)"
    WHEN cast(t6.sla as INTEGER) IS NULL THEN "Not Late"
    WHEN DATE(ww.pod_record_time,'Asia/Jakarta') > DATE(DATE_ADD(ww.shipping_time, INTERVAL (cast(t6.sla as INTEGER)) day)) THEN "Late"
    WHEN DATE(ww.pod_record_time,'Asia/Jakarta') <= DATE(DATE_ADD(ww.shipping_time, INTERVAL (cast(t6.sla as INTEGER)) day)) THEN "Not Late"
    WHEN DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND CURRENT_DATE('Asia/Jakarta') <= DATE(DATE_ADD(ww.shipping_time, INTERVAL (cast(t6.sla as INTEGER)) day)) THEN "Not Late"
    WHEN DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND CURRENT_DATE('Asia/Jakarta') > DATE(DATE_ADD(ww.shipping_time, INTERVAL (cast(t6.sla as INTEGER)) day)) THEN "Late"
    END AS Delivery_Performance,

   -----------------------mapping_late_delivery_reason---------------------------------
MIN(ps.problem_reason) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time ASC) AS problem_reason,
MIN(DATE(ps.operation_time,'Asia/Jakarta')) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time ASC) AS POS_time,

------------------------------Return-----------------------------------------------
DATETIME(rr.return_confirm_record_time,'Asia/Jakarta') AS return_confirm_record_time,
DATETIME(ww.return_pod_record_time,'Asia/Jakarta') AS return_pod_record_time,



FROM `datawarehouse_idexp.order_order` a
-- LEFT JOIN `datamart_idexp.masterdata_blibli_orderemail` oe ON a.waybill_no = oe.waybill_no 
LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.waybill_waybill` ww ON a.waybill_no = ww.waybill_no AND ww.deleted = '0'
LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.waybill_problem_piece` ps ON ww.waybill_no = ps.waybill_no AND ps.problem_type NOT IN ('02')
LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.waybill_return_bill` rr ON ww.waybill_no = rr.waybill_no AND rr.deleted = '0'
left join `grand-sweep-324604.datawarehouse_idexp.system_option` t3 on a.order_status  = t3.option_value and t3.type_option = 'orderStatus'
left join `grand-sweep-324604.datawarehouse_idexp.system_option` t7 on a.service_type  = t7.option_value and t7.type_option = 'serviceType'
left join `grand-sweep-324604.datawarehouse_idexp.system_option` et on a.express_type  = et.option_value and et.type_option = 'expressType'
left join `grand-sweep-324604.datawarehouse_idexp.system_option` sr on a.order_source  = sr.option_value and sr.type_option = 'orderSource'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON ww.waybill_status = t1.option_value AND t1.type_option = 'waybillStatus'
LEFT join `grand-sweep-324604.datawarehouse_idexp.res_problem_package` t4 on a.pickup_failure_problem_code = t4.code
LEFT JOIN `datamart_idexp.masterdata_backlog_city_to_ma` ma ON a.sender_city_name = ma.destination_city 
LEFT JOIN `datamart_idexp.mitra_late_reason_pickup`lpr ON a.waybill_no = lpr.waybill_no
LEFT JOIN `datamart_idexp.mapping_mitra_by_branch` mb ON a.pickup_branch_name = mb.branch_name
LEFT JOIN `datamart_idexp.masterdata_mapping_problem_factor` pf1 ON a.pickup_failure_problem_code = pf1.code
LEFT JOIN `datamart_idexp.masterdata_mapping_problem_factor` pf2 ON lpr.late_pickup_reason = pf2.register_reason_bahasa
LEFT JOIN `datamart_idexp.mapping_kanwil_area` kw ON a.sender_province_name = kw.province_name
INNER JOIN `datamart_idexp.masterdata_sla_shopee` t6 ON ww.recipient_city_name = t6.destination_city and ww.sender_city_name = t6.origin_city
    -- LEFT JOIN `datamart_idexp.masterdata_backlog_city_to_ma` ma ON ww.recipient_district_name = ma.destination AND ww.recipient_city_name = ma.destination_city 
  -- LEFT JOIN `datamart_idexp.mitra_late_reason_delivery` ldr ON ww.waybill_no = ldr.waybill_no


WHERE 
-- DATE(a.input_time, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -90 DAY))
DATE(ww.shipping_time, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -90 DAY))
AND DATE(ww.pod_record_time, 'Asia/Jakarta') BETWEEN '2023-02-15' AND '2023-02-21'
-- WHERE input_time >= '2022-12-31 17:00:00' and input_time <= '2023-01-30 16:59:59'
-- DATE(a.input_time, 'Asia/Jakarta') BETWEEN '2022-11-01' AND '2023-02-28'
AND sr.option_name NOT IN ('Tokopedia')

-- AND a.waybill_no IN (
-- "IDE700135328259"
-- )

QUALIFY ROW_NUMBER() OVER (PARTITION BY Waybill_No)=1
),

mapping_blibli_pickup AS (
SELECT 
Waybill_No,
Sender,
Sender_Address,
Origin_Province,
Origin_City,
Dest_Province,
Dest_City,
Dest_District,
Pickup_Area,
Kanwil_Area,
Origin_Branch,
-- Email_Date,
Order_Time,
Order_Date,
Input_Time,
-- Status_Create_Order,
-- Diff_email_to_input,
start_pickup_time,
Waktu_Pengambilan,
Pickup_Time,
Pickup_Duration_Day,
Order_Source,
Order_Status,
CASE WHEN Order_Status = 'Cancel Order' THEN 'Cancel Order'
     WHEN Order_Status = 'Picked Up' THEN 'Picked Up'
     ELSE 'Not Picked Up' END AS Pickup_Status,

--------------------Mapping_Pickup_Performance----------------------------
----------------------Mapping_Pickup_Category-------------------------------
CASE
WHEN order_past_1500 = 1 THEN "NextDay Pickup"
WHEN order_past_1500 = 0 THEN "SameDay Pickup"
END AS Pickup_Category,

-----nextday pickup------
CASE 
    WHEN order_past_1500 = 1 AND Pickup_Time IS NOT NULL AND Pickup_Time <= DATE_ADD(Order_Date, INTERVAL 1 DAY) THEN "On Time"
    WHEN order_past_1500 = 1 AND Pickup_Time IS NULL AND Order_Status IN ('Cancel Order') THEN "Cancel Order"
    WHEN order_past_1500 = 1 AND Pickup_Time IS NOT NULL AND Pickup_Time > DATE_ADD(Order_Date, INTERVAL 1 DAY) THEN "Late"
    WHEN order_past_1500 = 1 AND Pickup_Time IS NULL AND CURRENT_DATE('Asia/Jakarta') > DATE_ADD(Order_Date, INTERVAL 1 DAY) THEN "Late"
    WHEN order_past_1500 = 1 AND Pickup_Time IS NULL AND CURRENT_DATE('Asia/Jakarta') <= DATE_ADD(Order_Date, INTERVAL 1 DAY) THEN "Not Picked Up"
-----sameday pickup------
    WHEN order_past_1500 = 0 AND Pickup_Time IS NOT NULL AND Pickup_Time <= Order_Date THEN "On Time"
    WHEN order_past_1500 = 0 AND Pickup_Time IS NULL AND Order_Status IN ('Cancel Order') THEN "Cancel Order"
    WHEN order_past_1500 = 0 AND Pickup_Time IS NOT NULL AND Pickup_Time > Order_Date THEN "Late"
    WHEN order_past_1500 = 0 AND Pickup_Time IS NULL AND CURRENT_DATE('Asia/Jakarta') > Order_Date THEN "Late"
    WHEN order_past_1500 = 0 AND Pickup_Time IS NULL AND CURRENT_DATE('Asia/Jakarta') <= Order_Date THEN "Not Picked Up"    
    -- ELSE "Late"  
    END AS Pickup_Perform,

Is_Backlog,
Backlog_Pickup_Day,
CASE 
    WHEN Backlog_Pickup_Day = 0 THEN "0 Hari"
    WHEN Backlog_Pickup_Day = 1 THEN "1 Hari"
    WHEN Backlog_Pickup_Day = 2 THEN "2 Hari"
    WHEN Backlog_Pickup_Day >= 3 AND Backlog_Pickup_Day <= 14 THEN "3-14 Hari" 
    WHEN Backlog_Pickup_Day >= 15 AND Backlog_Pickup_Day <= 20 THEN "15-20 Hari"
    WHEN Backlog_Pickup_Day >= 21 THEN ">20 Hari"  
    END AS Aging_Category,

Pickup_Failure_Reason AS Late_Pickup_Reason,
Pickup_Failure_Attempt,
Late_Pickup_Factor,
Late_Pickup_Category,
Shipment_status,
-----------------------------Delivery-----------------------------------------
POD_Date,
POD_Time,
POD_Branch,
-------------mapping_city_category------------------------------
City_Category,
-----------------blibli_delivery_performance------------------------
SLA_Delivery,
Due_Date_Delivery,
Delivery_Performance,
-----------------------mapping_late_delivery_reason---------------------------------
POS_time,
CASE 
    WHEN Delivery_Performance = 'Not Late' THEN NULL
    WHEN Delivery_Performance = 'Late' AND problem_reason IN ('Paket dikirim via ekspedisi lain','Pengirim mengantarkan paket ke Drop Point','Pengirim tidak di tempat','Paket sedang disiapkan','Pengirim akan mengantar paket ke cabang','Pengirim tidak ada di lokasi/toko','Alamat pengirim kurang jelas','Pengirim sedang mempersiapkan paket','Pengirim meminta pergantian jadwal','Paket belum ada/belum selesai dikemas','Pengirim tidak dapat dihubungi','Pengirim merasa tidak menggunakan iDexpress','Pengirim sedang libur','Pengirim sebagai dropshipper dan menunggu supplier','Paket pre order','Di luar cakupan area cabang, akan dijadwalkan ke cabang lain') THEN NULL
    WHEN Delivery_Performance = 'Late' AND problem_reason IS NOT NULL THEN problem_reason
    END AS Late_Delivery_Reason, 
-------------------------------Return---------------------------------------
return_confirm_record_time,
return_pod_record_time


FROM blibli_pickup_performances_new
),

deliv_non_tokped AS (
SELECT 
Waybill_No,
Sender,
-- Sender_Address,
-- Pickup_Area,
Origin_Province,
Origin_City,
Dest_Province,
Dest_City,
Dest_District,
Kanwil_Area,
Origin_Branch,
-- Email_Date,
Order_Time,
Order_Date,
Input_Time,
-- Status_Create_Order,
-- Diff_email_to_input,
Pickup_Category,
Waktu_Pengambilan,
Pickup_Time,
Pickup_Duration_Day,
Order_Source,
Order_Status,
Pickup_Status,
Pickup_Perform,
Is_Backlog,
Backlog_Pickup_Day,
Aging_Category,
CASE WHEN Pickup_Perform = "On Time" THEN NULL
-- WHEN Waybill_No IN ("IDB003532256514","IDB001882438972") THEN "Failed API & Late Import, Docking Error"
ELSE Late_Pickup_Reason
END AS Late_Pickup_Reason,
Pickup_Failure_Attempt,
CASE 
    WHEN Pickup_Perform = "On Time" THEN NULL
    WHEN Pickup_Perform = 'Late' AND Late_Pickup_Reason IN ('Late Schedule to Branch','Late Schedule to Courier','Kurir tidak available','Late scan  pickup') THEN 'IDE'
    WHEN Pickup_Perform = 'Late' AND Late_Pickup_Reason IN ('Paket sedang disiapkan') THEN 'Pengirim'
    WHEN Pickup_Perform = 'Late' AND Late_Pickup_Reason IS NULL THEN 'IDE'
    ELSE Late_Pickup_Factor
    END AS Late_Pickup_Factor,
CASE
    WHEN Pickup_Perform = "On Time" THEN NULL
    WHEN Pickup_Perform = 'Late' AND Late_Pickup_Reason IN ('Late Schedule to Branch','Late Schedule to Courier','Kurir tidak available','Late scan pickup') THEN 'Controllable'
    WHEN Pickup_Perform = 'Late' AND Late_Pickup_Reason IN ('Paket sedang disiapkan') THEN 'Uncontrollable'
    WHEN Pickup_Perform = 'Late' AND Late_Pickup_Reason IS NULL THEN 'Controllable'
    ELSE Late_Pickup_Category
    END AS Late_Pickup_Category, 
------------------------Final Pickup Performance--------------------------------
---------------------sameday pickup-------------------------------------
CASE
    WHEN Pickup_Perform = "On Time" THEN "Not Late"
    WHEN Pickup_Perform = 'Late' AND Pickup_Category = "SameDay Pickup" AND Late_Pickup_Reason IN ('Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Pengirim akan mengantar paket ke cabang','Pengirim tidak ada di lokasi/toko','Alamat pengirim kurang jelas','Pengirim sedang mempersiapkan paket','Pengirim meminta pergantian jadwal','Paket belum ada/belum selesai dikemas','Pengirim tidak dapat dihubungi','Pengirim merasa tidak menggunakan iDexpress','Pengirim sedang libur','Pengirim sebagai dropshipper dan menunggu supplier','Paket pre order','Paket sedang disiapkan') AND DATE(Pickup_Failure_Attempt) <= DATE(Order_Date) THEN "Not Late"
    WHEN Pickup_Perform = 'Late' AND Pickup_Category = "SameDay Pickup" AND Late_Pickup_Reason IN ('Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Pengirim akan mengantar paket ke cabang','Pengirim tidak ada di lokasi/toko','Alamat pengirim kurang jelas','Pengirim sedang mempersiapkan paket','Pengirim meminta pergantian jadwal','Paket belum ada/belum selesai dikemas','Pengirim tidak dapat dihubungi','Pengirim merasa tidak menggunakan iDexpress','Pengirim sedang libur','Pengirim sebagai dropshipper dan menunggu supplier','Paket pre order','Paket sedang disiapkan') AND DATE(Pickup_Failure_Attempt) > DATE(Order_Date) THEN "Late"
    WHEN Pickup_Perform = 'Late' AND Pickup_Category = "SameDay Pickup" AND Late_Pickup_Reason NOT IN ('Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Pengirim akan mengantar paket ke cabang','Pengirim tidak ada di lokasi/toko','Alamat pengirim kurang jelas','Pengirim sedang mempersiapkan paket','Pengirim meminta pergantian jadwal','Paket belum ada/belum selesai dikemas','Pengirim tidak dapat dihubungi','Pengirim merasa tidak menggunakan iDexpress','Pengirim sedang libur','Pengirim sebagai dropshipper dan menunggu supplier','Paket pre order','Paket sedang disiapkan') AND (DATE(Pickup_Failure_Attempt) > DATE(Order_Date) OR DATE(Pickup_Failure_Attempt) <= DATE(Order_Date)) THEN "Late"
----------------------------nexday pickup------------------------------------------
    WHEN Pickup_Perform = 'Late' AND Pickup_Category = "NextDay Pickup" AND Late_Pickup_Reason IN ('Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Pengirim akan mengantar paket ke cabang','Pengirim tidak ada di lokasi/toko','Alamat pengirim kurang jelas','Pengirim sedang mempersiapkan paket','Pengirim meminta pergantian jadwal','Paket belum ada/belum selesai dikemas','Pengirim tidak dapat dihubungi','Pengirim merasa tidak menggunakan iDexpress','Pengirim sedang libur','Pengirim sebagai dropshipper dan menunggu supplier','Paket pre order','Paket sedang disiapkan') AND DATE(Pickup_Failure_Attempt) <= DATE_ADD(Order_Date, INTERVAL 1 DAY) THEN "Not Late"
    WHEN Pickup_Perform = 'Late' AND Pickup_Category = "NextDay Pickup" AND Late_Pickup_Reason IN ('Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Pengirim akan mengantar paket ke cabang','Pengirim tidak ada di lokasi/toko','Alamat pengirim kurang jelas','Pengirim sedang mempersiapkan paket','Pengirim meminta pergantian jadwal','Paket belum ada/belum selesai dikemas','Pengirim tidak dapat dihubungi','Pengirim merasa tidak menggunakan iDexpress','Pengirim sedang libur','Pengirim sebagai dropshipper dan menunggu supplier','Paket pre order','Paket sedang disiapkan') AND DATE(Pickup_Failure_Attempt) > DATE_ADD(Order_Date, INTERVAL 1 DAY) THEN "Late"
    WHEN Pickup_Perform = 'Late' AND Pickup_Category = "NextDay Pickup" AND Late_Pickup_Reason NOT IN ('Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Pengirim akan mengantar paket ke cabang','Pengirim tidak ada di lokasi/toko','Alamat pengirim kurang jelas','Pengirim sedang mempersiapkan paket','Pengirim meminta pergantian jadwal','Paket belum ada/belum selesai dikemas','Pengirim tidak dapat dihubungi','Pengirim merasa tidak menggunakan iDexpress','Pengirim sedang libur','Pengirim sebagai dropshipper dan menunggu supplier','Paket pre order','Paket sedang disiapkan') AND (DATE(Pickup_Failure_Attempt) <= DATE_ADD(Order_Date, INTERVAL 1 DAY) OR DATE(Pickup_Failure_Attempt) > DATE_ADD(Order_Date, INTERVAL 1 DAY))THEN "Late"
    ELSE Pickup_Perform
    END AS Final_Pickup_Performance,

Shipment_status,
-----------------------------Delivery-----------------------------------------
POD_Date,
POD_Time,
POD_Branch,
-------------mapping_city_category------------------------------
City_Category,
-----------------blibli_delivery_performance------------------------
SLA_Delivery,
Due_Date_Delivery,
Delivery_Performance,
-----------------------mapping_late_delivery_reason---------------------------------
CASE
WHEN Delivery_Performance = 'Not Late' THEN NULL
ELSE POS_time END AS POS_time,
Late_Delivery_Reason, 
CASE
WHEN Delivery_Performance = 'Not Late' THEN NULL
WHEN Delivery_Performance = 'Late' AND Late_Delivery_Reason IN ('Paket akan diproses dengan nomor resi yang baru','Paket salah dalam proses penyortiran','Paket rusak/pecah','Paket hilang atau tidak ditemukan','Data alamat tidak sesuai dengan kode sortir','Paket hilang ditemukan','Pengemasan paket dengan kemasan rusak','Paket crosslabel','Melewati jam operasional cabang','Kerusakan pada resi / informasi resi tidak jelas','Kemasan paket rusak','Paket akan dikembalikan ke cabang asal','Kerusakan pada label pengiriman','Paket salah sortir/ salah rute','Di luar cakupan area cabang, akan dijadwalkan ke cabang lain','Paket yang diterima dalam keadaan rusak','Late Arrival','Kurir tidak available','Late scan POD') THEN 'IDE'
WHEN Delivery_Performance = 'Late' AND Late_Delivery_Reason IN ('Nomor telepon yang tertera tidak dapat dihubungi','Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Toko atau kantor sudah tutup','Pelanggan tidak di lokasi','Reschedule pengiriman dengan penerima','Pelanggan membatalkan pengiriman','Pelanggan ingin dikirim ke alamat berbeda','Nomor telepon yang tertera tidak dapat dihubungi atau alamat tidak jelas','Penerima menolak menerima paket','Alamat pelanggan salah/sudah pindah alamat','Penerima ingin membuka paket sebelum membayar','Pelanggan libur akhir pekan/libur panjang','Pelanggan menunggu paket yang lain untuk dikirim','Pelanggan berinisiatif mengambil paket di cabang','Kemasan paket tidak sesuai prosedur','Pengirim membatalkan pengiriman','Penerima tidak di tempat',
'Penerima menjadwalkan ulang waktu pengiriman','Penerima pindah alamat','Penerima ingin mengambil paket di cabang','Alamat tidak lengkap','Penerima tidak dikenal','Nomor telepon tidak dapat dihubungi') THEN 'Penerima'
WHEN Delivery_Performance = 'Late' AND Late_Delivery_Reason IN ('Paket ditolak oleh bea cukai (red line)','Terdapat barang berbahaya (Dangerous Goods)','Cuaca buruk / bencana alam','Penundaan jadwal armada pengiriman','Indikasi kecurangan pengiriman','Berat paket tidak sesuai','Paket makanan, disimpan hingga waktu pengiriman yang tepat','Food parcels, kept until proper delivery time','Pengirim tidak dapat dihubungi','bencana alam','Cuaca buruk / Hujan') THEN 'External'
WHEN Delivery_Performance = 'Late' AND Late_Delivery_Reason IS NULL THEN 'IDE'
END AS Late_Factor,

CASE
WHEN Delivery_Performance = 'Late' AND Late_Delivery_Reason IN ('Paket akan diproses dengan nomor resi yang baru','Paket salah dalam proses penyortiran','Paket rusak/pecah','Paket hilang atau tidak ditemukan','Data alamat tidak sesuai dengan kode sortir','Paket hilang ditemukan','Pengemasan paket dengan kemasan rusak','Paket crosslabel','Melewati jam operasional cabang','Kerusakan pada resi / informasi resi tidak jelas','Kemasan paket rusak','Paket akan dikembalikan ke cabang asal','Kerusakan pada label pengiriman','Paket salah sortir/ salah rute','Di luar cakupan area cabang, akan dijadwalkan ke cabang lain','Paket yang diterima dalam keadaan rusak','Late Arrival','Late scan POD') THEN 'Controllable'
WHEN Delivery_Performance = 'Late' AND Late_Delivery_Reason IN ('Nomor telepon yang tertera tidak dapat dihubungi','Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Toko atau kantor sudah tutup','Pelanggan tidak di lokasi','Reschedule pengiriman dengan penerima','Pelanggan membatalkan pengiriman','Pelanggan ingin dikirim ke alamat berbeda','Nomor telepon yang tertera tidak dapat dihubungi atau alamat tidak jelas','Penerima menolak menerima paket','Alamat pelanggan salah/sudah pindah alamat','Penerima ingin membuka paket sebelum membayar','Pelanggan libur akhir pekan/libur panjang','Pelanggan menunggu paket yang lain untuk dikirim','Pelanggan berinisiatif mengambil paket di cabang','Kemasan paket tidak sesuai prosedur','Pengirim membatalkan pengiriman','Penerima tidak di tempat',
'Penerima menjadwalkan ulang waktu pengiriman','Penerima pindah alamat','Penerima ingin mengambil paket di cabang','Alamat tidak lengkap','Penerima tidak dikenal','Nomor telepon tidak dapat dihubungi') THEN 'Uncontrollable'
WHEN Delivery_Performance = 'Late' AND Late_Delivery_Reason IN ('Paket ditolak oleh bea cukai (red line)','Terdapat barang berbahaya (Dangerous Goods)','Cuaca buruk / bencana alam','Penundaan jadwal armada pengiriman','Indikasi kecurangan pengiriman','Berat paket tidak sesuai','Paket makanan, disimpan hingga waktu pengiriman yang tepat','Food parcels, kept until proper delivery time','Pengirim tidak dapat dihubungi','bencana alam','Cuaca buruk / Hujan') THEN 'Uncontrollable'
WHEN Delivery_Performance = 'Late' AND Late_Delivery_Reason IS NULL THEN 'Controllable'
END AS Late_Category,
-------------------------------Return---------------------------------------
return_confirm_record_time,
return_pod_record_time,
------------------------Final Deliv Performance--------------------------------
CASE
    WHEN Delivery_Performance = "Not Late" THEN "Not Late"
    WHEN Delivery_Performance = 'Late' AND Late_Delivery_Reason IN ('Nomor telepon yang tertera tidak dapat dihubungi','Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Toko atau kantor sudah tutup','Pelanggan tidak di lokasi','Reschedule pengiriman dengan penerima','Pelanggan membatalkan pengiriman','Pelanggan ingin dikirim ke alamat berbeda','Nomor telepon yang tertera tidak dapat dihubungi atau alamat tidak jelas','Penerima menolak menerima paket','Alamat pelanggan salah/sudah pindah alamat','Penerima ingin membuka paket sebelum membayar','Pelanggan libur akhir pekan/libur panjang','Pelanggan menunggu paket yang lain untuk dikirim','Pelanggan berinisiatif mengambil paket di cabang','Kemasan paket tidak sesuai prosedur','Pengirim membatalkan pengiriman','Penerima tidak di tempat',
'Penerima menjadwalkan ulang waktu pengiriman','Penerima pindah alamat','Penerima ingin mengambil paket di cabang','Alamat tidak lengkap','Penerima tidak dikenal','Nomor telepon tidak dapat dihubungi') AND DATE(POS_time) <= DATE(Due_Date_Delivery) THEN "Not Late"
    WHEN Delivery_Performance = 'Late' AND Late_Delivery_Reason IN ('Nomor telepon yang tertera tidak dapat dihubungi','Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Toko atau kantor sudah tutup','Pelanggan tidak di lokasi','Reschedule pengiriman dengan penerima','Pelanggan membatalkan pengiriman','Pelanggan ingin dikirim ke alamat berbeda','Nomor telepon yang tertera tidak dapat dihubungi atau alamat tidak jelas','Penerima menolak menerima paket','Alamat pelanggan salah/sudah pindah alamat','Penerima ingin membuka paket sebelum membayar','Pelanggan libur akhir pekan/libur panjang','Pelanggan menunggu paket yang lain untuk dikirim','Pelanggan berinisiatif mengambil paket di cabang','Kemasan paket tidak sesuai prosedur','Pengirim membatalkan pengiriman','Penerima tidak di tempat',
'Penerima menjadwalkan ulang waktu pengiriman','Penerima pindah alamat','Penerima ingin mengambil paket di cabang','Alamat tidak lengkap','Penerima tidak dikenal','Nomor telepon tidak dapat dihubungi') AND DATE(POS_time) > DATE(Due_Date_Delivery) THEN "Late"
    WHEN Delivery_Performance = 'Late' AND Late_Delivery_Reason NOT IN ('Nomor telepon yang tertera tidak dapat dihubungi','Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Toko atau kantor sudah tutup','Pelanggan tidak di lokasi','Reschedule pengiriman dengan penerima','Pelanggan membatalkan pengiriman','Pelanggan ingin dikirim ke alamat berbeda','Nomor telepon yang tertera tidak dapat dihubungi atau alamat tidak jelas','Penerima menolak menerima paket','Alamat pelanggan salah/sudah pindah alamat','Penerima ingin membuka paket sebelum membayar','Pelanggan libur akhir pekan/libur panjang','Pelanggan menunggu paket yang lain untuk dikirim','Pelanggan berinisiatif mengambil paket di cabang','Kemasan paket tidak sesuai prosedur','Pengirim membatalkan pengiriman','Penerima tidak di tempat',
'Penerima menjadwalkan ulang waktu pengiriman','Penerima pindah alamat','Penerima ingin mengambil paket di cabang','Alamat tidak lengkap','Penerima tidak dikenal','Nomor telepon tidak dapat dihubungi') THEN "Late"
WHEN Delivery_Performance = 'Late' AND Late_Delivery_Reason IS NULL THEN "Late"
    END AS Final_Deliv_Performance,



FROM mapping_blibli_pickup),

mapping_deliv_non_tokped AS (

SELECT 
Waybill_No,
POD_Date,
Origin_Province,
Origin_City,
Dest_Province,
Dest_City,
Dest_District,
CASE WHEN Delivery_Performance = 'Not Late' THEN '1' 
WHEN Delivery_Performance = 'Late' THEN NULL END AS Deliv_performance,
CASE WHEN Final_Deliv_Performance = 'Not Late' THEN '1'
WHEN Final_Deliv_Performance = 'Late' THEN NULL
END AS Final_Deliv_Performance,
CASE WHEN return_confirm_record_time IS NOT NULL THEN '1'
ELSE NULL END AS return_or_not



FROM deliv_non_tokped)

SELECT 
POD_Date,
Dest_City,
Dest_District,
COUNT(Deliv_performance) AS Ontime_Deliv,
COUNT(Final_Deliv_Performance) AS Ontime_Deliv_Adjusted,
COUNT(return_or_not) AS return_waybill,
COUNT(Waybill_No) AS Total_waybill,


FROM mapping_deliv_non_tokped

WHERE Dest_City IN ('JAKARTA UTARA')

GROUP BY 1,2,3
ORDER BY POD_Date ASC, Dest_District ASC


-- WHERE Pickup_Category = "NextDay Pickup"
-- AND Pickup_Perform = "Late"
