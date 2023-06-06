SELECT 
       t1.waybill_no AS No_Waybill,
       t1.order_no AS Order_No,
       rd1.option_name AS Sumber_Waybill,
       t1.vip_customer_name AS VIP_Username,
       DATETIME(t1.shipping_time,'Asia/Jakarta') AS Tanggal_Pengiriman, -- index backfill
       t1.pickup_branch_name AS Asal_Cabang, 
       t1.sender_province_name AS Provinsi_Asal, 
       t1.sender_city_name AS Kota_Asal,
       t1.recipient_province_name AS Provinsi_Tujuan,
       t1.recipient_city_name AS Kota_Tujuan,
       t1.recipient_district_name AS Tujuan,
       t1.recipient_name AS Penerima,
       t1.item_name AS Nama_Item,
       t1.item_value AS Nilai_barang,
       rd2.option_name AS Tipe_Ekspres,
       t1.item_actual_weight AS Berat_Aktual,
       t1.item_calculated_weight AS Kalkulasi_Berat,
       t1.cod_amount AS Nominal_COD,
       t1.insurance_amount AS Biaya_Asuransi,
       t1.standard_shipping_fee AS Biaya_Standar_Pengiriman,
       t1.receivable_shipping_fee AS Biaya_Piutang_Pengiriman,
       t1.handling_fee AS Biaya_penanganan,
       t1.other_fee AS Biaya_Lain,
       t1.total_shipping_fee AS Biaya_Total_Pengiriman,
       rd5.option_name AS Status_Pembatalan, -- update
       DATETIME(t1.pod_record_time,'Asia/Jakarta') AS Waktu_Perekaman_POD, -- update
       t1.pod_branch_name AS Cabang_POD, -- update
       rd4.option_name AS Status_POD, --update
     --   DATETIME(return,'Asia/Jakarta') AS Tanggal_Kembali_POD,
       DATETIME(t2.return_record_time, 'Asia/Jakarta') AS Waktu_Pengembalian, -- update
       DATETIME(t2.return_confirm_record_time, 'Asia/Jakarta') AS Waktu_Konfirmasi_Pengembalian, -- update
       DATETIME(t1.return_pod_record_time,'Asia/Jakarta') AS Waktu_POD_pengembalian, -- update
       rd3.option_name as Status_Pengembalian, -- update
       t2.return_shipping_fee AS Biaya_Return_Shipping, -- update
       rd6.option_name AS Tipe_Pembayaran,
       t4.payment_date AS Tanggal_Disetor,
       rd7.return_type AS Deskripsi_Alasan_Retur,
       t2.description AS Deskripsi,
       t1.sender_name AS Pengirim,
       t1.sender_cellphone AS No_HP_Pengirim,
       t1.recipient_cellphone AS No_HP_Penerima,
       t1.claim_value AS Nilai_Klaim,
       t1.pod_courier_name AS Kurir_Pengiriman,
       t1.item_value AS item_value,



       DATETIME(t1.update_time, 'Asia/Jakarta') AS update_time -- update -- index upsert

-- FROM `grand-sweep-324604.datawarehouse_idexp.waybill_waybill` 

FROM `grand-sweep-324604.datawarehouse_idexp.dm_waybill_waybill` t1 -- table upsert

     LEFT JOIN `datawarehouse_idexp.waybill_return_bill` t2 ON t2.waybill_no = t1.waybill_no AND t2.return_waybill_no = t1.return_waybill_no
     LEFT JOIN `datawarehouse_idexp.system_option` rd1 ON t1.waybill_source = rd1.option_value AND rd1.type_option = 'waybillSource'
     LEFT JOIN `datawarehouse_idexp.system_option` rd2 ON t1.express_type = rd2.option_value AND rd2.type_option = 'expressType'
     LEFT JOIN `datawarehouse_idexp.system_option` rd3 ON t1.return_flag = rd3.option_value AND rd3.type_option = 'returnFlag'
     LEFT JOIN `datawarehouse_idexp.system_option` rd4 ON t1.pod_flag = rd4.option_value AND rd4.type_option = 'podFlag'
     LEFT JOIN `datawarehouse_idexp.system_option` rd5 ON t1.void_flag = rd5.option_value AND rd5.type_option = 'voidFlag'
     LEFT JOIN `datawarehouse_idexp.system_option` rd6 ON t1.payment_type = rd6.option_value AND rd6.type_option = 'paymentType'
		 LEFT JOIN `datawarehouse_idexp.return_type` rd7 ON t2.return_type_id = rd7.id AND rd7.deleted=0
     LEFT JOIN `datawarehouse_idexp.branch_virtual_account_detail` t3 ON t3.waybill_no = t1.waybill_no
     LEFT JOIN `datawarehouse_idexp.branch_virtual_account` t4 ON t3.account_number = t4.account_number

-- WHERE DATE(shipping_time,'Asia/Jakarta') >= '2023-01-01' -- filter backfill

WHERE DATE(t1.update_time,'Asia/Jakarta') >= DATE_ADD(CURRENT_DATE('Asia/Jakarta'), INTERVAL -40 DAY) -- filter upsert

AND t1.claim_value IS NOT NULL
AND t1.sender_city_name IN ('BATAM')
