#' A Cat Function
#'
#' This function allows you to express your love of cats.
#' @param love Do you love cats? Defaults to TRUE.
#' @keywords cats
#' @export
#' @examples
#' cat_function()

cat_function <- function(love=TRUE){
  if(love==TRUE){
    print("I love cats! Seriously I don't")
  }
  else {
    print("I am not a cool person. Right??")
  }
}


#' Load Payments Data since Jan to most recent available
#'
#' This function collects payments data from across several googlesheets, cleans and produces a consistent dataframe for analysis.
#' @param none no parameters
#' @keywords payments googlesheets data
#' @export
#' @examples
#' load_payments_data()
#' 

#' Another payment data extraction function
#'
#' This function pulls paymentsdata from a local sqlite database
#' @keywords helper
#' @export 
#' @examples
#' db <- get_paymentsdata_sqlite(dbname)

get_paymentsdata_sqlite <- function(dbname) {
  library(DBI)
  setwd("~/Dropbox/Jumbotail/AnalysisCodeNotebooks/data")
  mydb <- dbConnect(RSQLite::SQLite(), dbname)
  query = 'SELECT * FROM paymentsdb'
  paymentsdata <- mydb %>% dbGetQuery(query)
  
  paymentsdata <- distinct(paymentsdata)
  
  paymentsdata$paymentdate = ymd("1970-01-01") + paymentsdata$paymentdate
  paymentsdata$deliverydate = ymd("1970-01-01") + paymentsdata$deliverydate
  #paymentsdata <- paymentsdata %>% filter(deliverydate<=today())
  
  return(paymentsdata)
}

#' Another payment data extraction function
#'
#' This function collects new payments data and updates the local sqlite database
#' @param filename String filename of the gsheet that holds the new data
#' @param inpdate String date after which data is considered new
#' @keywords sqlite payments
#' @export 
#' @examples
#' db <- update_pmtsdata_sqlite("filename", inpdate = max(paymentsdata$deliverydate, na.rm=T) )


update_pmtsdata_sqlite <- function(filename, inpdate) {
  inpdate <- as_date(inpdate)
  
  ndata <- getmonthlysheet(filename)
  ndata <- fix_pmtdata(ndata)
  ndata <- ndata %>% filter(deliverydate > inpdate)
  
  # setwd("~/Dropbox/Jumbotail/AnalysisCodeNotebooks")
  # mydb <- dbConnect(RSQLite::SQLite(), "data/paymentsdata.sqlite")
  # mydb %>% dbWriteTable("paymentsdb", ndata, append = T)
  # 
  return(ndata)
}

#' A payment data extraction function
#'
#' This function is a helper function for the load_payments_data function below
#' @param filename String filename of the gsheet that holds the data
#' @keywords helper
#' @export 
#' @examples
#' db <- getmonthlysheet("filename")

getmonthlysheet <- function(filenamevar){
  #filenamevar = "PaymentReconciliationData_Sep17_22_30"
  paymentsgooglesheet <- gs_title(filenamevar)
  pd <- paymentsgooglesheet %>% gs_read(ws="Data Sheet")
  
  pd = select(pd, -starts_with("X")) #drop blank columns
  
  matchterms = "Cheque Replacement|Cheque Bounce|Return ID"
  pd = select(pd, -matches(matchterms)) #drop additional columns
  
  
  pd = pd %>% select(noquote(order(colnames(pd)))) #sortcolsalpha
  
  pd$`Delivery Date` <- parse_date_time(pd$`Delivery Date`, c("ymd", "dmy", "%m/%d/%Y", "%d-%m-%Y"))
  pd$`Payment Date` <- parse_date_time(pd$`Payment Date`, c("ymd", "dmy", "%m/%d/%Y", "%d-%m-%Y"))
  return (pd)
}

#' A date utility function
#'
#' This function is a helper function that swaps months and days of a date
#' @param inpdate String date 
#' @keywords helper
#' @export 
#' @examples
#' swap_month_day(inpdate)

swap_month_day <- function(inpdate) {
  swapdate = as_date("2000-01-01")
  month(swapdate) <- day(inpdate)
  day(swapdate) <- month(inpdate)
  year(swapdate) <- year(inpdate)
  return (swapdate)
}


fix_pmtdata <- function(db) {
  
  #0 Remove Duplicates
  ##D. remove duplicate rows
  db = db %>% distinct()
  
  
  #A. rename cols
  db = db %>% rename(
    cid = CollectionID,
    paymentdate = `Payment Date`,
    deliverydate = `Delivery Date`,
    shipmentid = `Shipment ID`,
    bid = `Store ID`,
    itemid = `Item ID`,
    storename = `Store Name`,
    storestatus = `Store Status`,
    finalstatus = `Final Status`, 
    mrpcashback = `MRP Cashback`,
    weightcashback = `Weight Cashback`, 
    quantitydelivered = `Quantity Delivered`,
    shippingamountcollected = ShippingAmountCollected,
    paymentmode = `Payment Mode`,
    checknumber = ChequeNumber1,
    bank = Bank1,
    dispatchedquantity = `Dispatched Quantity`,
    orderamountcollected = OrderAmountCollected,
    check = Check,
    orderitemquantity = OrderItemQty,
    comments = Comments, 
    partpayment = `Part Payment`,
    sellerid = SellerID
  )
  
  ## B. Correct Data Types
  #correct data types for post Jan data
  db = db %>% mutate(
    paymentdate = parse_date_time(paymentdate, c("ymd", "dmy", "%m/%d/%Y")),
    deliverydate = parse_date_time(deliverydate, c("ymd", "dmy", "%m/%d/%Y")),
    storestatus = factor(storestatus),
    finalstatus= factor(finalstatus),
    mrpcashback= as.integer(mrpcashback),
    weightcashback = as.integer(weightcashback),
    shippingamountcollected = as.numeric(shippingamountcollected),
    orderitemquantity = as.integer(orderitemquantity),
    partpayment = as.factor(if_else(str_detect(partpayment,"."), TRUE, FALSE)),
    sellerid = as.character(sellerid),
    checknumber=as.integer(checknumber),
    dispatchedquantity=as.integer(dispatchedquantity)
  )
  
  
  # C.  cleanup paymentmode
  
  db = db %>% 
    mutate(
      paymentmode = ifelse(paymentmode == "cash", "Cash", paymentmode),
      paymentmode = ifelse(paymentmode == "wallet", "Wallet", paymentmode))
  
  db = db %>% mutate(
    paymentmode = ifelse(paymentmode == "Credit - FundsCorner" | paymentmode == "FundsCorner" | paymentmode == "FundsCorner - PDC", "Credit - FundsCorner - PDC", paymentmode), 
    paymentmode = ifelse(paymentmode == "FundsCorner - Cash", "Credit - FundsCorner - Cash", paymentmode)
  ) 
  
  # db = db %>% 
  #   mutate(
  #     paymentdate = ifelse(paymentdate > today(), swap_month_day(paymentdate), paymentdate),
  #     deliverydate = ifelse(deliverydate > today(), swap_month_day(deliverydate), deliverydate)
  #   ) 
  
  
  # D. make factors
  db = db %>% mutate(
    storestatus = factor(storestatus),
    finalstatus= factor(finalstatus), 
    paymentmode = factor(paymentmode)
  ) 
  
  
  # E. Cleanup
  db = db %>% filter(!is.na(bid))
  
  db = db %>% 
    select(bid, bank:shippingamountcollected, storestatus, weightcashback, storename)
  
  # F. 
  return(db)
  
}



#' Payment data extraction & cleaning function
#'
#' This function visits the monthly reconciliation sheets and collects the data together 
#' @keywords sqlite payments
#' @export 
#' @examples
#' db <- load_payments_data() )

load_payments_data <- function(){
  library(tidyverse)
  library(RPostgreSQL)
  library(lubridate)
  library(stringr)
  library(scales)
  library(googlesheets)
  
  ##A. Get monthly sheets
  
  pd_nov1 <- getmonthlysheet("PaymentReconciliationData_Nov17_01_17")
  
  pd_oct2 <- getmonthlysheet("PaymentReconciliationData_Oct17_18_31")
  paymentsdata <- rbind(pd_oct2, pd_nov1)
  
  pd_oct1 <- getmonthlysheet("PaymentReconciliationData_Oct17_01_17")
  paymentsdata <- rbind(paymentsdata, pd_oct1)
  
  pd_sept2 <- getmonthlysheet("PaymentReconciliationData_Sep17_22_30")
  paymentsdata <- rbind(paymentsdata, pd_sept2)
  
  pd_sept1 <- getmonthlysheet("PaymentReconciliationData_Sep17_1_21")
  paymentsdata <- rbind(paymentsdata, pd_sept1)
  
  pd_aug1 <- getmonthlysheet("PaymentReconciliationData_Aug17_1_26")
  paymentsdata <- rbind(paymentsdata, pd_aug1)
  
  pd_aug2 <- getmonthlysheet("PaymentReconciliationData_Aug17_27_31")
  paymentsdata <- rbind(paymentsdata, pd_aug2)
  
  paymentsdata_jul <- getmonthlysheet("PaymentReconciliationData_Jul17")
  paymentsdata <- rbind(paymentsdata, paymentsdata_jul)
  
  paymentsdata_jun <- getmonthlysheet("PaymentReconciliationData_Jun17")
  paymentsdata <- rbind(paymentsdata, paymentsdata_jun)
  
  paymentsdata_may <- getmonthlysheet("PaymentReconciliationData_May17")
  paymentsdata <- rbind(paymentsdata, paymentsdata_may)
  
  paymentsdata_apr<- getmonthlysheet("PaymentReconciliationData_Apr17")
  paymentsdata <- rbind(paymentsdata, paymentsdata_apr)
  
  paymentsdata_mar <- getmonthlysheet("PaymentReconciliationData_Mar17")
  paymentsdata <- rbind(paymentsdata, paymentsdata_mar)
  
  paymentsdata_feb <- getmonthlysheet("PaymentReconciliationData_Feb17")
  paymentsdata <- rbind(paymentsdata, paymentsdata_feb)
  #
  
  # #test to see if current data and feb data has the same structure, if so bind
  #x = colnames(paymentsdata)
  #y = colnames(pd_sept1)
  # z = colnames(paymentsdata_mar)
  # a = colnames(paymentsdata_apr)
  # b = colnames(paymentsdata_may)
  #View(cbind(x,y))
  
  # #B.bind
  # paymentsdata = rbind(paymentsdata_feb, paymentsdata_mar, paymentsdata_apr, paymentsdata_may,
  #                     paymentsdata_jun, paymentsdata_jul, pd_aug1, pd_aug2, pd_sept1, pd_sept2)
  # 
  #C. remove datasets
  rm(paymentsdata_feb, paymentsdata_mar, paymentsdata_apr, paymentsdata_may,
     paymentsdata_jun, paymentsdata_jul, pd_aug1, pd_aug2, pd_sept1, pd_sept2)
  
  ##D. remove duplicate rows
  paymentsdata = paymentsdata %>% distinct()
  
  ## E. Deal with January
  paymentsgooglesheet_Jan <- gs_title("PaymentReconciliationData_Jan17")
  paymentsdata_Jan <- paymentsgooglesheet_Jan %>% gs_read(ws="Data Sheet")
  paymentsdata_Jan = select(paymentsdata_Jan, -starts_with("X")) #drop blanck columns
  paymentsdata_Jan = select(paymentsdata_Jan, -starts_with("Cod")) #drop extra Jan column named Code
  paymentsdata_Jan = paymentsdata_Jan %>% select(noquote(order(colnames(paymentsdata_Jan)))) #sortcolsalpha
  
  # #check to see if data structures are the same
  # feb = colnames(paymentsdata)
  # jan = colnames(paymentsdata_Jan)
  # z = cbind(jan, feb)
  # z
  
  #typechanges
  paymentsdata_Jan$ChequeNumber1 = as.integer(paymentsdata_Jan$ChequeNumber1) #typechange
  paymentsdata_Jan$`Dispatched Quantity` = as.integer(paymentsdata_Jan$`Dispatched Quantity`)
  paymentsdata_Jan$`Weight Cashback` = as.character(paymentsdata_Jan$`Weight Cashback`)
  paymentsdata_Jan$`Payment Date`= mdy(paymentsdata_Jan$`Payment Date`)
  paymentsdata_Jan$`Delivery Date`= mdy(paymentsdata_Jan$`Delivery Date`)
  
  #rename columns jan
  paymentsdata_Jan = paymentsdata_Jan %>% rename(
    cid = CollectionID,
    paymentdate = `Payment Date`,
    deliverydate = `Delivery Date`,
    shipmentid = `Shipment ID`,
    bid = `Store ID`,
    itemid = `Item ID`,
    storename = `Store Name`,
    storestatus = `Store Status`,
    finalstatus = `Final Status`,
    mrpcashback = `MRP Cashback`,
    weightcashback = `Weight Cashback`,
    quantitydelivered = `Quantity Delivered`,
    shippingamountcollected = ShippingAmountCollected,
    paymentmode = `Payment Mode`,
    checknumber = ChequeNumber1,
    bank = Bank1,
    dispatchedquantity = `Dispatched Quantity`,
    orderamountcollected = OrderAmountCollected,
    check = Check,
    orderitemquantity = OrderItemQty,
    comments = Comments,
    partpayment = `Part Payment`,
    sellerid = SellerID
  )
  
  #correct data types jan
  paymentsdata_Jan = paymentsdata_Jan %>% mutate(
    paymentdate = parse_date_time(paymentdate, c("ymd", "dmy", "%m/%d/%Y")),
    deliverydate = parse_date_time(deliverydate, c("ymd", "dmy", "%m/%d/%Y")),
    storestatus = factor(storestatus),
    finalstatus= factor(finalstatus),
    mrpcashback= as.integer(mrpcashback),
    weightcashback = as.integer(weightcashback),
    shippingamountcollected = as.numeric(shippingamountcollected),
    #paymentmode = factor(paymentmode),
    orderitemquantity = as.integer(orderitemquantity),
    partpayment = as.factor(if_else(str_detect(partpayment,"."), TRUE, FALSE)),
    sellerid = as.character(sellerid),
    checknumber=as.integer(checknumber),
    dispatchedquantity=as.integer(dispatchedquantity)
  )
  
  
  ## F. Rename Columns
  #rename columns for post jan data
  
  paymentsdata = paymentsdata %>% rename(
    cid = CollectionID,
    paymentdate = `Payment Date`,
    deliverydate = `Delivery Date`,
    shipmentid = `Shipment ID`,
    bid = `Store ID`,
    itemid = `Item ID`,
    storename = `Store Name`,
    storestatus = `Store Status`,
    finalstatus = `Final Status`, 
    mrpcashback = `MRP Cashback`,
    weightcashback = `Weight Cashback`, 
    quantitydelivered = `Quantity Delivered`,
    shippingamountcollected = ShippingAmountCollected,
    paymentmode = `Payment Mode`,
    checknumber = ChequeNumber1,
    bank = Bank1,
    dispatchedquantity = `Dispatched Quantity`,
    orderamountcollected = OrderAmountCollected,
    check = Check,
    orderitemquantity = OrderItemQty,
    comments = Comments, 
    partpayment = `Part Payment`,
    sellerid = SellerID
  )
  
  ## G. Correct Data Types
  #correct data types for post Jan data
  paymentsdata = paymentsdata %>% mutate(
    paymentdate = parse_date_time(paymentdate, c("ymd", "dmy", "%m/%d/%Y")),
    deliverydate = parse_date_time(deliverydate, c("ymd", "dmy", "%m/%d/%Y")),
    storestatus = factor(storestatus),
    finalstatus= factor(finalstatus),
    mrpcashback= as.integer(mrpcashback),
    weightcashback = as.integer(weightcashback),
    shippingamountcollected = as.numeric(shippingamountcollected),
    #paymentmode = factor(paymentmode),
    orderitemquantity = as.integer(orderitemquantity),
    partpayment = as.factor(if_else(str_detect(partpayment,"."), TRUE, FALSE)),
    sellerid = as.character(sellerid),
    checknumber=as.integer(checknumber),
    dispatchedquantity=as.integer(dispatchedquantity)
  )
  
  
  # H. bind data sets with January
  paymentsdata = rbind(paymentsdata_Jan, paymentsdata)
  
  # I.  cleanup paymentmode
  
  paymentsdata = paymentsdata %>% 
    mutate(
      paymentmode = ifelse(paymentmode == "cash", "Cash", paymentmode),
      paymentmode = ifelse(paymentmode == "wallet", "Wallet", paymentmode))
  
  paymentsdata = paymentsdata %>% mutate(
    paymentmode = ifelse(paymentmode == "Credit - FundsCorner" | paymentmode == "FundsCorner" | paymentmode == "FundsCorner - PDC", "Credit - FundsCorner - PDC", paymentmode), 
    paymentmode = ifelse(paymentmode == "FundsCorner - Cash", "Credit - FundsCorner - Cash", paymentmode)
  ) 
  
  
  # J. make factors
  paymentsdata = paymentsdata %>% mutate(
    storestatus = factor(storestatus),
    finalstatus= factor(finalstatus), 
    paymentmode = factor(paymentmode)
  ) 
  
  #rm(paymentsdata_Jan)
  
  # K. Cleanup
  paymentsdata = paymentsdata %>% filter(!is.na(bid))
  
  paymentsdata = paymentsdata %>% 
    select(bid, bank:shippingamountcollected, storestatus, weightcashback, storename)
  
  
  #paymentsdata %>% count(paymentmode)
  
  ## L.Complete
  paste("Payments Data Loaded. Earliest delivery date in the data as follows:")
  
  min(paymentsdata$deliverydate, na.rm = T)
  
  paste("Latest date in the data as follows:")
  
  max(paymentsdata$deliverydate, na.rm = T)
  
  # glimpse(paymentsdata)
  # 
  # paymentsdata %>% group_by(paymentmode) %>% summarize(totalcollected = sum(orderamountcollected, na.rm = T)) %>% mutate(freq = totalcollected*100/sum(totalcollected))
  
  # paymentsdata_gmv_customer = paymentsdata %>% 
  #   group_by(paymentdate, bid) %>% 
  #   mutate(dailygmv_customer = sum(orderamountcollected, na.rm=T) + sum(shippingamountcollected,na.rm=T)) %>% ungroup() %>% select(bid, deliverydate, paymentdate, paymentmode, storename, dailygmv_customer) %>% distinct()
  # 
  #paste("paymentsdata_gmv_customer loaded")
  
  return(paymentsdata)
}



#' Load Cheque Data since Jan to most recent available
#'
#' This function collects cheque data from a googlesheet, cleans and returns a consistent dataframe for analysis.
#' @param none no parameters
#' @keywords payments googlesheets data cheques
#' @export
#' @examples
#' load_cheques_data()
#' 
#' 

load_cheques_data <- function() {
  library(tidyverse)
  library(RPostgreSQL)
  library(lubridate)
  library(stringr)
  library(scales)
  library(googlesheets)
  
  checksdata_gs <- gs_title("Cheque Payment & Exposure Tracker") #expect autentication
  
  checkdata_raw <- checksdata_gs %>% gs_read(ws="Master Data")
  
  
  checkdata = checkdata_raw %>% 
    select(-starts_with("X")) %>% 
    filter(!is.na(BID))
  
  
  colnames(checkdata) = c(
    "date",
    "bid",
    "customername",
    "amount",
    "checknumber",
    "bankname",
    "phonenumber",
    "status_final",
    "bouncereason",
    "replacement_days"
  )
  
  checkdata = checkdata %>% mutate (
    date = ymd(date),
    phonenumber = as.character(phonenumber),
    status_final = factor(status_final)
  )
  
  checkdata = checkdata %>% mutate (
    everbounced = ifelse(is.na(bouncereason), "no", "yes"),
    everbounced = as.factor(everbounced)
  )
  
  
  #lastbounce
  temp = checkdata %>% 
    group_by(bid) %>% 
    mutate(
      last_bounce_date = if_else(bouncereason =="Insufficient Funds", date, NULL)
    )
  
  checkdata = temp %>% 
    group_by(bid) %>% 
    mutate (
      last_bounce_date = max(last_bounce_date, na.rm = T)
    ) %>% ungroup()
  
  return(checkdata)
  
}

#' Load Orders Data since April 16, 2016 to recent available
#'
#' This function collects cheque data from a googlesheet, cleans and returns a consistent dataframe for analysis.
#' @param jthost host url for Amazon Redshift Database
#' @param jtport port for Amazon Redshift Database
#' @param jtdbname database name for Amazon Redshift Database
#' @param redshift_user password for Amazon Redshift Database
#' @param redshift_password password for Amazon Redshift Database

#' @keywords payments googlesheets data cheques
#' @export
#' @examples
#' load_orders_data(databaseconnection)
#' 
#' 

load_orders_data <- function(con1) {
  
  # query2 = "SELECT DATE (ord.src_created_time) AS order_date,
  #      CASE
  # WHEN DATE (ord.src_created_time) >= m.activation_date THEN 'Activated_Customer_Order'
  # WHEN m.activation_date IS NULL THEN 'Non_Activated_Customer_Order'
  # ELSE 'Non_Activated_Customer_Order'
  # END AS activated_order,
  # ord.order_id AS orderid,
  # ord.order_item_id AS order_item_id,
  # c.firstname AS customer_name,
  # c.customerid,
  # c.businessid AS bid,
  # c.pincode,
  # CASE
  # WHEN s.storename IS NOT NULL THEN s.storename
  # ELSE c.firstname
  # END AS store_name,
  # s.storeonboarddate,
  # s.tier AS customer_tier,
  # ord.seller_id AS seller_id,
  # org.org_name AS seller_name,
  # p.jpin AS JPIN,
  # p.title AS product_title,
  # b.displaytitle AS brand_name,
  # pv.pvname AS PV_name,
  # cat.category_name,
  # CASE
  # WHEN cat.distributed IS TRUE THEN 'FMCG'
  # ELSE 'NON_FMCG'
  # END AS FMCG,
  # ord.quantity AS ordered_units,
  # ord.created_units,
  # ord.confirmedunitquantity AS confirmed_quantity,
  # ord.underprocessunitquantity AS under_process_quantity,
  # ord.ready_to_ship_units,
  # ord.in_transit_units,
  # ord.delivered_units,
  # ord.cancelled_units,
  # ord.returned_units,
  # ord.shipped_unit_quantity AS shipped_units,
  # ord.return_requested_quantity AS return_requested_units,
  # ord.order_item_status AS orderitem_status,
  # sca.deadweight AS weight,
  # EXTRACT(month FROM s.storeonboarddate) AS onboarded_month_number,
  # TO_CHAR(s.storeonboarddate,'Month') AS onboarded_month,
  # ord.cartitemid AS cart_item_id,
  # CASE
  # WHEN cs.case_size IS NULL THEN 0
  # ELSE ord.quantity / cs.case_size
  # END AS shipmentunits_cases,
  # CASE
  # WHEN cs.case_size IS NULL THEN ord.quantity
  # ELSE ord.quantity % cs.case_size
  # END AS shipmentunits_pieces,
  # CASE
  # WHEN cs.case_size IS NULL THEN ord.quantity
  # ELSE ord.quantity / cs.case_size + ord.quantity % cs.case_size
  # END AS totalshipmentunits,
  # ord.shipping_charges AS shipping_charge,
  # ord.order_item_amount,
  # ord.order_item_amount / ord.quantity AS price_per_unit
  # FROM bolt_order_item_snapshot ord
  # JOIN customer_snapshot_ c ON c.customerid = ord.buyer_id
  # LEFT JOIN store_name s ON c.customerid = s.customerid
  # LEFT JOIN customer_milestone_ m ON m.bid = s.bid
  # LEFT JOIN listing_snapshot li ON li.listing_id = ord.listing_id
  # LEFT JOIN sellerproduct_snapshot sp ON sp.sp_id = li.sp_id
  # LEFT JOIN product_snapshot_ p ON p.jpin = sp.jpin
  # LEFT JOIN category cat ON p.pvid = cat.pvid
  # LEFT JOIN brand_snapshot_ b ON b.brandid = p.brandid
  # LEFT JOIN (SELECT DISTINCT (pvname), pvid FROM productvertical_snapshot_) pv ON pv.pvid = p.pvid
  # LEFT JOIN org_profile_snapshot org ON ord.seller_id = org.org_profile_id
  # LEFT JOIN case_details cs ON cs.jpin = p.jpin
  # LEFT JOIN supplychainattributes_snapshot_ sca ON sca.jpin = p.jpin
  # WHERE c.istestcustomer = 'false'"
  
  query1<-  "SELECT DATE (ord.src_created_time) AS order_date,
  CASE WHEN DATE (ord.src_created_time) >= m.activation_date THEN 'Activated_Customer_Order'
  WHEN m.activation_date IS NULL THEN 'Non_Activated_Customer_Order'
  ELSE 'Non_Activated_Customer_Order'
  END AS activated_order,
  ord.order_id AS orderid,
  ord.order_item_id AS order_item_id,
  c.firstname AS customer_name,
  c.customerid,
  c.businessid AS bid,
  c.pincode,
  CASE
  WHEN s.storename IS NOT NULL THEN s.storename
  ELSE c.firstname
  END AS store_name,
  s.storeonboarddate,
  s.tier AS customer_tier,
  ord.seller_id AS seller_id,
  org.org_name AS seller_name,
  p.jpin AS JPIN,
  p.title AS product_title,
  b.displaytitle AS brand_name,
  pv.pvname AS PV_name,
  cat.category_name,
  CASE
  WHEN cat.distributed IS TRUE THEN 'FMCG'
  ELSE 'NON_FMCG'
  END AS FMCG,
  ord.quantity AS ordered_units,
  ord.created_units,
  ord.confirmedunitquantity AS confirmed_quantity,
  ord.underprocessunitquantity AS under_process_quantity,
  ord.ready_to_ship_units,
  ord.in_transit_units,
  ord.delivered_units,
  ord.cancelled_units,
  ord.returned_units,
  ord.shipped_unit_quantity AS shipped_units,
  ord.return_requested_quantity AS return_requested_units,
  ord.order_item_status AS orderitem_status,
  sca.deadweight AS weight,
  EXTRACT(month FROM s.storeonboarddate) AS onboarded_month_number,
  TO_CHAR(s.storeonboarddate,'Month') AS onboarded_month,
  ord.cartitemid AS cart_item_id,
  CASE
  WHEN cs.case_size IS NULL THEN 0
  ELSE ord.quantity / cs.case_size
  END AS shipmentunits_cases,
  CASE
  WHEN cs.case_size IS NULL THEN ord.quantity
  ELSE ord.quantity % cs.case_size
  END AS shipmentunits_pieces,
  CASE
  WHEN cs.case_size IS NULL THEN ord.quantity
  ELSE ord.quantity / cs.case_size + ord.quantity % cs.case_size
  END AS totalshipmentunits,
  ord.shipping_charges AS shipping_charge,
  ord.order_item_amount,
  ord.order_item_amount / ord.quantity AS price_per_unit
  FROM bolt_order_item_snapshot ord
  JOIN customer_snapshot_ c ON c.customerid = ord.buyer_id
  LEFT JOIN store_name_old s ON c.customerid = s.customerid
  LEFT JOIN customer_milestone_ m ON m.bid = s.bid
  LEFT JOIN listing_snapshot li ON li.listing_id = ord.listing_id
  LEFT JOIN sellerproduct_snapshot sp ON sp.sp_id = li.sp_id
  LEFT JOIN product_snapshot_ p ON p.jpin = sp.jpin
  LEFT JOIN category cat ON p.pvid = cat.pvid
  LEFT JOIN brand_snapshot_ b ON b.brandid = p.brandid
  LEFT JOIN (SELECT DISTINCT (pvname), pvid FROM productvertical_snapshot_) pv ON pv.pvid = p.pvid
  LEFT JOIN org_profile_snapshot org ON ord.seller_id = org.org_profile_id
  LEFT JOIN case_details cs ON cs.jpin = p.jpin
  LEFT JOIN supplychainattributes_snapshot_ sca ON sca.jpin = p.jpin
  WHERE c.istestcustomer = 'false'"
  
  
  ordersdata <- dbGetQuery(con1, query1)
  
  maxdate = max(ordersdata$order_date, na.rm=T)
  print(maxdate)
  
  return (ordersdata)
}


#' Writes Function to Googlesheet
#'
#' This function collects cheque data from a googlesheet, cleans and returns a consistent dataframe for analysis.
#' @param df dataframe to write
#' @param df_name name of file to write in Googlesheets
#' @keywords googlesheets utility
#' @export
#' @examples
#' write_to_gs(df, df_name)
#' 
#' 
write_to_gs <- function(df, df_name) {
  library(googlesheets)
  
  #df.name <- deparse(substitute(df))
  #print(df.name)
  file_name = str_c(df_name, ".csv", "")
  
  write.csv(df, file_name)
  temp_gs <- gs_upload(file_name)
  file.remove(file_name)
  return(temp_gs)
}


#' Gets store names data
#'
#' This function collects cheque data from a googlesheet, cleans and returns a consistent dataframe for analysis.
#' @param redshift redshift object
#' @keywords sql utility
#' @export
#' @examples
#' get_jt_storenamedata(myRedshift)
#' 
#' 
get_jt_storenamedata <- function(myRedshift) {
  storenametable_rs <-tbl(myRedshift, "store_name")
  storenamedata = storenametable_rs %>% collect() 
  return(storenamedata)
}


#' Gets credit customer data
#'
#' This function collects cheque data from a googlesheet, cleans and returns a consistent dataframe for analysis.
#' @param redshift redshift object
#' @keywords sql utility
#' @export
#' @examples
#' get_credit_customers(myRedshift)
#' 
#' 
get_credit_customers <- function(myRedshift) {
  library(googlesheets)
  #creditcustomers_rs <-tbl(myRedshift, "customer_credit_details")
  #creditcustomers_rs = creditcustomers_rs %>% collect() 
  
  creditcustomers_gs <- gs_title("customer_credit_details")
  creditcustomers <- creditcustomers_gs %>% gs_read()
  
  creditcustomers = creditcustomers %>% 
    rename(
      oldbid = bid,
      bid = businessid,
      status = Status
    ) %>% 
    mutate(
      credit_onboard_date = parse_date_time(credit_onboard_date, c("dmY", "Ymd")),
      last_update_date = parse_date_time(last_update_date, c("dmY", "Ymd")), 
      last_update_date = if_else(is.na(last_update_date), credit_onboard_date, last_update_date)
    )
  
  
  return(creditcustomers)
}


#' Gets the bizid bid map
#'
#' This function collects cheque data from a googlesheet, cleans and returns a consistent dataframe for analysis.
#' @keywords redshift redshift object
#' @keywords sql utility
#' @export
#' @examples
#' get_bizbid_map(myRedshift)
#' 
#' 
get_bizbid_map <- function(myRedshift) {
  bizbid_table_rs = tbl(myRedshift, "business_snapshot")
  
  bizbid_table_rs = bizbid_table_rs %>% select(businessid, redshiftbusinessid) %>% collect()
  
  bizbid_table_rs = bizbid_table_rs %>%  
    rename(
      bid = redshiftbusinessid,
      bizid = businessid
    )
  
}


#' Adds a biz id column but keeps bid 
#'
#' This function collects cheque data from a googlesheet, cleans and returns a consistent dataframe for analysis.
#' @param redshift redshift object
#' @param bizbid_table dataframe with 2 cols bid and bizid, can use get_bizid_map() function
#' @keywords utility conversion
#' @export
#' @examples
#' add_biz_column(myRedshift, bizbid_table)
#' 
#' 

add_biz_column <-function(df, bizbid_table) {
  #library
  library(dplyr)
  library(stringr)
  
  #ungroup if dataframe is grouped
  df = df %>% ungroup()
  
  #do the join to map bizid to the existing bid
  df = df %>% left_join(bizbid_table)
  
  #create a bizid column that deals with the case where there is a bid column that also has bizids already
  df = df %>% mutate(
    bizid = ifelse(str_detect(df$bid, "BID-"), bizid, bid)
  )
  
  #clean up dataframe and return
  df = df %>% select(-bid) %>% rename(bid = bizid) %>% select(bid, everything())
  
  return(df)
}


#' Takes Payments Data and gives transactions by bizid and date 
#'
#' This function collects cheque data from a googlesheet, cleans and returns a consistent dataframe for analysis.
#' @param payments redshift object
#' @keywords daily transactions
#' @export
#' @examples
#' get_customer_transactions(paymentsdata)
#' 


get_customer_transactions <- function(paymentsdata) {
  #paymentsdata <- get_payments_data()
  
  paymentsdata_gmv_customer = paymentsdata %>% 
    group_by(deliverydate, bid) %>% 
    mutate(dailygmv_customer = sum(orderamountcollected, na.rm=T) + sum(shippingamountcollected,na.rm=T)) %>% ungroup()
  
  
  paymentsdata_gmv_customer = paymentsdata_gmv_customer %>% ungroup() %>% select(bid, deliverydate, paymentdate, paymentmode, storename, dailygmv_customer) %>% distinct() %>% arrange(deliverydate)
  
  paymentsdata_gmv_customer <- paymentsdata_gmv_customer %>% filter(!is.na(deliverydate))
  
  paymentsdata_gmv_customer$yr_month_dlv <- format(as.Date(paymentsdata_gmv_customer$deliverydate), "%Y-%m")
  paymentsdata_gmv_customer$yr_wkly_dlv <- format(as.Date(paymentsdata_gmv_customer$deliverydate), "%Y-%U")
  
  paymentsdata_gmv_customer <- paymentsdata_gmv_customer %>% mutate(pmode = ifelse(grepl("*Credit*", paymentmode), "Credit", paymentmode))
  
  paymentsdata_gmv_customer <- paymentsdata_gmv_customer %>% filter(deliverydate>"2017-01-01" & deliverydate < today())
  
  return(paymentsdata_gmv_customer)
  
}


#' Wrapper around paymetns data that only loads paymentsdata if it's not already up to date 
#' Load Payments Data since Jan to most recent available
#'
#' This function collects payments data from across several googlesheets, cleans and produces a consistent dataframe for analysis.
#' @param none no parameters
#' @keywords payments googlesheets data
#' @export
#' @examples
#' get_payments_data()
#' 

get_payments_data <-function() {
  if (exists("paymentsdata")) {
    max_date = max(paymentsdata$paymentdate, na.rm = T)
    max_date = ymd(max_date)
    print(max_date)
    if (max_date >= (today()-1)) { 
      return (paymentsdata)  
    } else {
      paymentsdata = load_payments_data()
      return(paymentsdata)
    }
  }
  paymentsdata = load_payments_data()
  return(paymentsdata)
  
}


