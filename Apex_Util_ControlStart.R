#This is the start of the APEX Formatting tool, created by ICF 2019. It uses a .bat file 
#to utilize user inputs to download, format, and prepare AQS data for use in the APEX tool.

#Created for ICF July, 2019 by Delaney Reilly

rm(list=ls())

wd <- read.table("user_cd.txt", stringsAsFactors = FALSE)
wd <- wd[1,1]
# wd <- "C:\\Users\\39482\\Documents\\APEX\\APEX-Data-prep\\APEX-Data-FormattR\\APEX-TOOL-Testrun"
setwd(wd)


#Install and load needed packages
if (!require("BBmisc")) {
  install.packages("BBmisc", repos="http://cran.rstudio.com/")
  library("BBmisc")
}
if (!require("rlang")) {
  install.packages("rlang", repos="http://cran.rstudio.com/")
  library("rlang")
}
if (!require("geosphere")) {
  install.packages("geosphere", repos="http://cran.rstudio.com/")
  library("geosphere")
}
if (!require("spatstat")) {
  install.packages("spatstat", repos="http://cran.rstudio.com/")
  library("spatstat")
}
if (!require("foreach")) {
  install.packages("foreach", repos="http://cran.rstudio.com/")
  library("foreach")
}
if (!require("doParallel")) {
  install.packages("doParallel", repos="http://cran.rstudio.com/")
  library("doParallel")
}
if (!require("dplyr")) {
  install.packages("dplyr", repos="http://cran.rstudio.com/")
  library("dplyr")
}
if (!require("sqldf")) {
  install.packages("sqldf", repos="http://cran.rstudio.com/")
  library("sqldf")
}
if (!require("data.table")) {
  install.packages("data.table", repos="http://cran.rstudio.com/")
  library("data.table")
}
if (!require("xlsx")) {
  install.packages("xlsx", repos="http://cran.rstudio.com/")
  library("xlsx")
}
if (!require("readr")) {
  install.packages("readr", repos="http://cran.rstudio.com/")
  library("readr")
}
if (!require("xlsx")) {
  install.packages("xlsx", repos="http://cran.rstudio.com/")
  library("xlsx")
}

#Import Site and Monitor data from AQS website
download.file(paste0("https://aqs.epa.gov/aqsweb/airdata/aqs_sites.zip"), paste0(getwd(),"/aqs_sites.zip"))
download.file(paste0("https://aqs.epa.gov/aqsweb/airdata/aqs_monitors.zip"), paste0(getwd(),"/aqs_monitors.zip"))
unzip('aqs_sites.zip')
unzip('aqs_monitors.zip')

#Define list of chemicals with AQS chemical code
criteria_gas_numbers <- data.frame(AQSchem = c("Ozone", "SO2", "CO", "NO2", 
                                               "PM2.5 FRM/FEM", "PM2.5 non FRM/FEM",
                                               "PM10 Mass", "PM2.5 Speciation", "PM10 Speciation",
                                               "HAPs", "VOCs", "NONOxNOy", "Lead"),
                                   chemcode = c(44201, 42401, 42101, 42602, 
                                                88101, 88502, 81102, "SPEC", "PM10SPEC",
                                                "HAPS", "VOCS", "NONOxNOy", "LEAD"))
#Read in user input data
user.inputs <- read.table(paste0(wd,"/User_inputs.txt"), header = TRUE, stringsAsFactors = FALSE)
#Create AQS file name from User inputs
file.name <- paste("hourly",
                       criteria_gas_numbers$chemcode[criteria_gas_numbers$AQSchem 
                                                    %in% user.inputs$Value[user.inputs$Variable=="AQS-chem"]], 
                       user.inputs$Value[user.inputs$Variable=="data-year"], sep = "_")

#Download AQS data
for (i in 1:length(file.name)) {
download.file(paste0("https://aqs.epa.gov/aqsweb/airdata/",file.name[i], ".zip"), paste0(getwd(),"/",file.name[i],".zip"))
unzip(paste0(file.name[i],".zip"))
}

#Run the body of the fixing and formatting tool
source(file.path(wd,"Apex_Util_main.R"))
