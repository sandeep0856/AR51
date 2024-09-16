SELECT 
--FROM RAW ORDERS
O.ORDERID,
O.ORDERDATE,
O.SHIPDATE,
O.SHIPMODE,
O.ORDERSELLINGPRICE - O.ORDERCOSTPRICE AS ORDERPROFIT,
O.ORDERCOSTPRICE,
O.ORDERSELLINGPRICE,
--FROM RAW CUSTOMER
C.CUSTOMERID,
C.CUSTOMERNAME,
C.SEGMENT,
C.COUNTRY,
--FROM RAW PRODUCT
P.PRODUCTID,
P.CATEGORY,
P.PRODUCTNAME,
P.SUBCATEGORY
FROM {{ ref('raw_orders') }} AS O
LEFT JOIN {{ ref('raw_customer') }} AS C
ON O.CUSTOMERID = C.CUSTOMERID
LEFT JOIN {{ ref('raw_product') }} AS P
ON O.PRODUCTID = P.PRODUCTID

