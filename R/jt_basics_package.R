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
  paymentsgooglesheet <- gs_title("PaymentReconciliationData_a")
  paymentsdata_a <- paymentsgooglesheet %>% gs_read(ws="Data Sheet")
  paymentsdata_a = select(paymentsdata_a, -starts_with("X")) #drop blanck columns
  paymentsdata_a = select(paymentsdata_a, -contains("Cheque Replacement")) #drop blanck columns
  paymentsdata_a = select(paymentsdata_a, -contains("Cheque Bounce")) #drop blanck columns
  paymentsdata_a = paymentsdata_a %>% select(noquote(order(colnames(paymentsdata)))) #sortcolsalpha
  
  
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
  
  
  paymentsdata = rbind(paymentsdata_feb, paymentsdata_mar, paymentsdata_apr, paymentsdata_may, paymentsdata_a, paymentsdata_jul, paymentsdata_aug)
  
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
  
  paymentsdata_gmv_customer = paymentsdata %>% 
    group_by(paymentdate, bid) %>% 
    mutate(dailygmv_customer = sum(orderamountcollected, na.rm=T) + sum(shippingamountcollected,na.rm=T)) %>% ungroup() %>% select(bid, deliverydate, paymentdate, paymentmode, storename, dailygmv_customer) %>% distinct()
  
  paste("paymentsdata_gmv_customer loaded")
}

