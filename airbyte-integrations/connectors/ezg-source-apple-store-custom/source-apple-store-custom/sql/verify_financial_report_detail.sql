-- DA | Apple | Verify bang financial_report_detail sau sync dau tien
-- BAT BUOC theo playbook SS L: discover/spec pass KHONG dam bao data dung.
-- Thay <dataset> bang dataset dich cua connection.

-- 1) Phat hien row rac / lech cot: PHAI chi ra duy nhat 'valid'
SELECT
  CASE
    WHEN REGEXP_CONTAINS(_Transaction_Date_, r'^[0-9]{2}/[0-9]{2}/[0-9]{4}$') THEN 'valid'
    WHEN REGEXP_CONTAINS(_Transaction_Date_, r'^[A-Z]{2}$')                   THEN 'country_leak'
    WHEN _Transaction_Date_ = 'Total_Rows'                                    THEN 'total_row'
    WHEN _Transaction_Date_ IS NULL OR _Transaction_Date_ = ''                THEN 'empty'
    ELSE CONCAT('other: ', _Transaction_Date_)
  END AS row_type,
  COUNT(*) AS rows
FROM `easygoing-data.<dataset>.appstore_financial_report_detail`
GROUP BY row_type
ORDER BY rows DESC;

-- 2) Preamble gan dung? _Start_Date_/_End_Date_ phai la fiscal window, KHONG phai country/currency
SELECT DISTINCT _report_month_, _Start_Date_, _End_Date_
FROM `easygoing-data.<dataset>.appstore_financial_report_detail`
ORDER BY _report_month_ DESC;

-- 3) Cot so phai cast duoc: PHAI ra 0 dong
SELECT _report_month_, _Country_of_Sale_, _Quantity_, _Extended_Partner_Share_, _Customer_Price_
FROM `easygoing-data.<dataset>.appstore_financial_report_detail`
WHERE SAFE_CAST(_Quantity_ AS INT64) IS NULL
   OR SAFE_CAST(_Extended_Partner_Share_ AS NUMERIC) IS NULL
   OR SAFE_CAST(_Customer_Price_ AS NUMERIC) IS NULL
LIMIT 50;

-- 4) PK chi tiet (15 cot, co _row_number_) phai unique: PHAI ra 0 dong.
--    _row_number_ la chot cuoi -> khong bao gio mat dong. Ra >0 = loi parser sinh trung row_number.
SELECT
  _vendor_id_, _report_month_, _Transaction_Date_, _Settlement_Date_, _Apple_Identifier_,
  _SKU_, _Product_Type_Identifier_, _Country_of_Sale_, _Quantity_, _Extended_Partner_Share_,
  _Sale_or_Return_, _Order_Type_, _Region_, _Promo_Code_, _row_number_,
  COUNT(*) AS n
FROM `easygoing-data.<dataset>.appstore_financial_report_detail`
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
HAVING n > 1
LIMIT 50;

-- 5) Doi soat tong theo country/currency voi bang summary o CUOI FILE GOC
SELECT
  _report_month_,
  _Country_of_Sale_,
  _Partner_Share_Currency_,
  SUM(SAFE_CAST(_Quantity_ AS INT64))                 AS quantity,
  SUM(SAFE_CAST(_Extended_Partner_Share_ AS NUMERIC)) AS extended_partner_share
FROM `easygoing-data.<dataset>.appstore_financial_report_detail`
GROUP BY ALL
ORDER BY _report_month_ DESC, extended_partner_share DESC;

-- 6) Doi soat cheo voi financial_report (FINANCIAL/ZZ) — tong 2 ban phai bang nhau.
--    LUU Y ten cot khac nhau: detail = _Country_of_Sale_ / _Sale_or_Return_
--                             zz     = _Country_Of_Sale_ / _Sales_or_Return_
SELECT
  COALESCE(d._report_month_, f.report_month) AS report_month,
  d.detail_total,
  f.zz_total,
  ROUND(d.detail_total - f.zz_total, 2)      AS diff
FROM (
  SELECT _report_month_, SUM(SAFE_CAST(_Extended_Partner_Share_ AS NUMERIC)) AS detail_total
  FROM `easygoing-data.<dataset>.appstore_financial_report_detail`
  GROUP BY 1
) d
FULL JOIN (
  SELECT FORMAT_DATE('%Y-%m', PARSE_DATE('%m/%d/%Y', _Start_Date_)) AS report_month,
         SUM(SAFE_CAST(_Extended_Partner_Share_ AS NUMERIC))        AS zz_total
  FROM `easygoing-data.<dataset>.financial_report`
  GROUP BY 1
) f ON d._report_month_ = f.report_month
ORDER BY report_month DESC;
