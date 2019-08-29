#This is the start of the APEX Formatting tool, created by ICF 2019. It uses a .bat file 
#to utilize user inputs to download, format, and prepare AQS data for use in APEX.

#Created for EPA; Completed August 20, 2019

rm(list=ls())

wd <- read.table("user_cd.txt", stringsAsFactors = FALSE)
wd <- wd[1,1]
#wd <- "MAC USERS INPUT WORKING DIRECTORY HERE AND REMOVE LEADING #."
setwd(wd)


#Install and load needed packages
if (!require("geosphere")) {
  install.packages("geosphere", repos="http://cran.rstudio.com/")
  library("geosphere")
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
if (!require("ggmap")) {
  install.packages("ggmap", repos="http://cran.rstudio.com/")
  library("ggmap")
}
if (!require("ggsn")) {
  install.packages("ggsn", repos="http://cran.rstudio.com/")
  library("ggsn")
}

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

#Import Site and Monitor data from AQS website if user indicates that the files are new or if they don't exist previously.
if (user.inputs$Value[user.inputs$Variable == "New-sites"] == "No") {
  if (!file.exists(paste0(wd,"/aqs_sites.csv"))) {
    if (!file.exists(paste0(wd,"/aqs_sites.zip"))) {
      download.file(paste0("https://aqs.epa.gov/aqsweb/airdata/aqs_sites.zip"), paste0(getwd(),"/aqs_sites.zip"))
   }
    unzip('aqs_sites.zip')
  }} else {
    download.file(paste0("https://aqs.epa.gov/aqsweb/airdata/aqs_sites.zip"), paste0(getwd(),"/aqs_sites.zip"))
    unzip('aqs_sites.zip')
}

#Create AQS file name from User inputs
file.name <- paste("hourly",
                       criteria_gas_numbers$chemcode[criteria_gas_numbers$AQSchem 
                                                    %in% user.inputs$Value[user.inputs$Variable=="AQS_Chem"]], 
                       user.inputs$Value[user.inputs$Variable=="data-year"], sep = "_")

##User input error handling ##
if (length(file.name)>1) {
  stop ("Please enter just one file name per run\n")
}
if((user.inputs$Value[user.inputs$Variable=="AQS_Chem"] %in% criteria_gas_numbers$AQSchem)==FALSE) {
  stop("Cannot find a corresponding AQS file. Please verify your AQS-chem input.")
}
#Provide warning if datasets are likely to be too large.
if(length(user.inputs$Value[user.inputs$Variable=="data-year"])>2) {
  warning("Please process only one year at a time.")
}

#Download AQS data if not already present.
if (!file.exists(paste0(wd,"/",file.name,".csv"))) {  
  if (!file.exists(paste0(wd,"/",file.name,".zip"))) {
    download.file(paste0("https://aqs.epa.gov/aqsweb/airdata/",file.name, ".zip"), paste0(getwd(),"/",file.name,".zip"))
  }
  cat("Unzipping hourly data file")  
  unzip(paste0(file.name,".zip"))
}

#Run the body of the fixing and formatting tool
source(file.path(wd,"Apex_Util_main.R"))
