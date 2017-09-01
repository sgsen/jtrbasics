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

load_payments_data <- function(){
  library(tidyverse)
  library(RPostgreSQL)
  library(lubridate)
  library(stringr)
  library(scales)
  library(googlesheets)
  
  
  paymentsgooglesheet <- gs_title("PaymentReconciliationData_Aug17")
  paymentsdata_aug <- paymentsgooglesheet %>% gs_read(ws="Data Sheet")
  paymentsdata_aug = select(paymentsdata_aug, -starts_with("X")) #drop blanck columns
  paymentsdata_aug = select(paymentsdata_aug, -contains("Cheque Replacement")) #drop blanck columns
  paymentsdata_aug = select(paymentsdata_aug, -contains("Cheque Bounce")) #drop blanck columns
  paymentsdata_aug = paymentsdata_aug %>% select(noquote(order(colnames(paymentsdata_aug)))) #sortcolsalpha
  
  paymentsgooglesheet <- gs_title("PaymentReconciliationData_Jul17")
  paymentsdata_jul <- paymentsgooglesheet %>% gs_read(ws="Data Sheet")
  paymentsdata_jul = select(paymentsdata_jul, -starts_with("X")) #drop blanck columns
  paymentsdata_jul = select(paymentsdata_jul, -contains("Cheque Replacement")) #drop blanck columns
  paymentsdata_jul = select(paymentsdata_jul, -contains("Cheque Bounce")) #drop blanck columns
  paymentsdata_jul = paymentsdata_jul %>% select(noquote(order(colnames(paymentsdata_jul)))) #sortcolsalpha
  
  #Junedata
  paymentsgooglesheet <- gs_title("PaymentReconciliationData_Jun17")
  paymentsdata_jun = paymentsgooglesheet %>% gs_read(ws="Data Sheet")
  paymentsdata_jun = select(paymentsdata_jun, -starts_with("X")) #drop blanck columns
  paymentsdata_jun = select(paymentsdata_jun, -contains("Cheque Replacement")) #drop blanck columns
  paymentsdata_jun = select(paymentsdata_jun, -contains("Cheque Bounce")) #drop blanck columns
  paymentsdata_jun = paymentsdata_jun %>% select(noquote(order(colnames(paymentsdata_jun)))) #sortcolsalpha
  
  
  paymentsgooglesheet_feb <- gs_title("PaymentReconciliationData_Feb17")
  paymentsdata_feb <- paymentsgooglesheet_feb %>% gs_read(ws="Data Sheet")
  paymentsdata_feb = select(paymentsdata_feb, -starts_with("X")) #drop blanck columns
  paymentsdata_feb = paymentsdata_feb %>% select(noquote(order(colnames(paymentsdata_feb)))) #sortcolsalpha
  
  paymentsgooglesheet_mar <- gs_title("PaymentReconciliationData_Mar17")
  paymentsdata_mar <- paymentsgooglesheet_mar %>% gs_read(ws="Data Sheet")
  paymentsdata_mar = select(paymentsdata_mar, -starts_with("X")) #drop blanck columns
  paymentsdata_mar = paymentsdata_mar %>% select(noquote(order(colnames(paymentsdata_mar)))) #sortcolsalpha
  
  paymentsgooglesheet_apr <- gs_title("PaymentReconciliationData_Apr17")
  paymentsdata_apr <- paymentsgooglesheet_apr %>% gs_read(ws="Data Sheet")
  paymentsdata_apr = select(paymentsdata_apr, -contains("Cheque Replacement")) #drop blanck columns
  paymentsdata_apr = select(paymentsdata_apr, -starts_with("X")) #drop blanck columns
  paymentsdata_apr = paymentsdata_apr %>% select(noquote(order(colnames(paymentsdata_apr)))) #sortcolsalpha
  
  paymentsgooglesheet_may <- gs_title("PaymentReconciliationData_May17")
  paymentsdata_may <- paymentsgooglesheet_may %>% gs_read(ws="Data Sheet")
  paymentsdata_may = select(paymentsdata_may, -contains("Cheque Replacement")) #drop blanck columns
  paymentsdata_may = select(paymentsdata_may, -contains("Cheque Bounce")) #drop blanck columns
  paymentsdata_may = select(paymentsdata_may, -starts_with("X")) #drop blanck columns
  paymentsdata_may = paymentsdata_may %>% select(noquote(order(colnames(paymentsdata_may)))) #sortcolsalpha
  
  
  # #test to see if current data and feb data has the same structure, if so bind
  # x = colnames(paymentsdata)
  # y = colnames(paymentsdata_feb)
  # z = colnames(paymentsdata_mar)
  # a = colnames(paymentsdata_apr)
  # b = colnames(paymentsdata_may)
  # View(cbind(x,y,z,a,b))
  
  
  paymentsdata = rbind(paymentsdata_feb, paymentsdata_mar, paymentsdata_apr, paymentsdata_may, paymentsdata_jun, paymentsdata_jul, paymentsdata_aug)
  
  #remove duplicate rows
  paymentsdata = paymentsdata %>% distinct()
  
  rm(paymentsdata_feb, paymentsdata_mar, paymentsdata_apr, paymentsdata_may)
  
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
  
  # #typechanges
  # paymentsdata_Jan$ChequeNumber1 = as.integer(paymentsdata_Jan$ChequeNumber1) #typechange
  # paymentsdata_Jan$`Dispatched Quantity` = as.integer(paymentsdata_Jan$`Dispatched Quantity`)
  # paymentsdata_Jan$`Weight Cashback` = as.character(paymentsdata_Jan$`Weight Cashback`)
  #paymentsdata_Jan$`Payment Date`= mdy(paymentsdata_Jan$`Payment Date`)
  #paymentsdata_Jan$`Delivery Date`= mdy(paymentsdata_Jan$`Delivery Date`)
  
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
    paymentdate = dmy(paymentdate),
    deliverydate = dmy(deliverydate),
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
  
  
  #correct data types for post Jan data
  paymentsdata = paymentsdata %>% mutate(
    paymentdate = dmy(paymentdate),
    deliverydate = dmy(deliverydate),
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
  
  
  #bind data sets
  paymentsdata = rbind(paymentsdata_Jan, paymentsdata)
  
  #cleanup paymentmode
  
  paymentsdata = paymentsdata %>% 
    mutate(
      paymentmode = ifelse(paymentmode == "cash", "Cash", paymentmode),
      paymentmode = ifelse(paymentmode == "wallet", "Wallet", paymentmode))
  
  paymentsdata = paymentsdata %>% mutate(
    paymentmode = ifelse(paymentmode == "Credit - FundsCorner" | paymentmode == "FundsCorner" | paymentmode == "FundsCorner - PDC", "Credit - FundsCorner - PDC", paymentmode), 
    paymentmode = ifelse(paymentmode == "FundsCorner - Cash", "Credit - FundsCorner - Cash", paymentmode)
  ) 
  
  
  # make factors
  paymentsdata = paymentsdata %>% mutate(
    storestatus = factor(storestatus),
    finalstatus= factor(finalstatus), 
    paymentmode = factor(paymentmode)
  ) 
  
  rm(paymentsdata_Jan)
  
  paymentsdata = paymentsdata %>% filter(!is.na(bid))
  
  paymentsdata = paymentsdata %>% 
    select(bid, bank:shippingamountcollected, storestatus, weightcashback, storename)
  
  
  paymentsdata %>% count(paymentmode)
  
  
  paste("Payments Data Loaded. Earliest payment date in the data as follows:")
  
  min(paymentsdata$paymentdate, na.rm = T)
  
  paste("Latest date in the data as follows:")
  
  max(paymentsdata$paymentdate, na.rm = T)
  
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
#' load_orders_data(jt_host, jt_port, jt_dbname, redshift_user, redshift_pwd)
#' 
#' 

load_orders_data <- function(jt_host, jt_port, jt_dbname, redshift_user, redshift_pwd) {
  library(RPostgreSQL)
  
    #DATABASE CONNECTIONS
    drv <- dbDriver("PostgreSQL")
    con1 <- dbConnect(drv, host = jt_host , port = jt_port, dbname = jt_dbname, user = redshift_user, password = redshift_pwd)
  

    query2 = "SELECT DATE (ord.src_created_time) AS order_date,
         CASE
    WHEN DATE (ord.src_created_time) >= m.activation_date THEN 'Activated_Customer_Order'
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
    LEFT JOIN store_name s ON c.customerid = s.customerid
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


  ordersdata <- dbGetQuery(con1, query2)
  
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
  #return(temp_gs)
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


get_customer_transactions <- function() {
  paymentsdata <- get_payments_data()
  
  paymentsdata_gmv_customer = paymentsdata %>% 
    group_by(paymentdate, bid) %>% 
    mutate(dailygmv_customer = sum(orderamountcollected, na.rm=T) + sum(shippingamountcollected,na.rm=T)) %>% ungroup()
  
  
  paymentsdata_gmv_customer = paymentsdata_gmv_customer %>% ungroup() %>% select(bid, deliverydate, paymentdate, paymentmode, storename, dailygmv_customer) %>% distinct() %>% arrange(deliverydate)

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


