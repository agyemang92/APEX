## Preprocess Data ##
## 07/26/2019 - ICF ##
## This script contains 1 function (preprocess_data) that processes the raw data - filtering for relevant time period, chemical, and location, filling in missing days, making sure all hours are available, etc ##
 
## load packages ##
require(dplyr)
require(sqldf)
require(data.table)
require(xlsx)
require(readr)
require(foreach)
require(doParallel)
require(geosphere)

preprocess_data = function(date_start,date_end,time_start,time_end,FIPS,frac,chem,hrly_data_loc,site_data_loc,monitor_data_loc,countyFIPS_data_loc,ad,aq,ad_ofpn,aq_ofpn,max_dist){
## Error handling start ##

#browser()
#try-catch blocks to:
#check that date_end > date_start
#check that date is YYYY-MM-DD format
#check that time_end > date_start
#check that time is in 24-hr format

## Error handling end ##

## read in data ##
hrly_data                      <- as.data.frame(fread(hrly_data_loc,check.names = FALSE, stringsAsFactors = FALSE))    #read in hourly data
hrly_data$`State Code`         <- as.integer(hrly_data$`State Code`)                                                   #convert state code column to integers
hrly_data$`County Code`        <- as.integer(hrly_data$`County Code`)                                                  #convert county code column to integers
hrly_data$`Sample Measurement` <- as.numeric(hrly_data$`Sample Measurement`)                                           #convert measurement to numeric

sites                          <- as.data.frame(fread(site_data_loc,check.names = FALSE, stringsAsFactors = FALSE))    #read in site description data

## Script for selecting sites based on distance to study area ##
## Graham Glen, ICF, July 2019 ##
##=================================================================

max_dist    <- max_dist      # maximum acceptable distance in meters

sites <- sites[sites$`State Code`>="01" & sites$`State Code`<="56",]                        # Restrict to 50 states + DC
sites <- sites[sites$Latitude!=0 & sites$Longitude!=0,]                                     # Sites must have lat and long
sites$FIPS <- substr(100000+1000*as.numeric(sites$`State Code`)+sites$`County Code`,2,6)    # Create FIPS for all sites
study_FIPS <- FIPS                                                                          #user provided codes

sites <- sites[!is.na(sites$`Site Number`),] #remove all NAs

study <- sites[sites$FIPS %in% study_FIPS,]  #filter for sites with study FIPS

d <- as.data.frame(distm(cbind(sites$Longitude,sites$Latitude),cbind(study$Longitude,study$Latitude)))    # create matrix of distances

study$`State Code`            <- sprintf("%02d",as.integer(study$`State Code`))            # fix state code to 2 characters 
study$`County Code`           <- sprintf("%03d",as.integer(study$`County Code`))           # fix county code to 3 characters 
study$`Site Num`              <- sprintf("%05d",as.integer(study$`Site Num`))              # fix site number to 5 characters 

study$`Station ID`            <- paste0(study$`State Code`,study$`County Code`,
                                        study$`Site Num`)                                  #concatenate State+County+Site IDs

sites$`State Code`            <- sprintf("%02d",as.integer(sites$`State Code`))            # fix state code to 2 characters 
sites$`County Code`           <- sprintf("%03d",as.integer(sites$`County Code`))           # fix county code to 3 characters 
sites$`Site Num`              <- sprintf("%05d",as.integer(sites$`Site Num`))              # fix site number to 5 characters 

sites$`Station ID`            <- paste0(sites$`State Code`,sites$`County Code`,
                                        sites$`Site Num`)                                  #concatenate State+County+Site IDs


colnames(d) <- as.character(study$`Station ID`) #name columns
rownames(d) <- as.character(sites$`Station ID`) #name rows


n <- nrow(d)        #number of rows
s <- ncol(d)        #number of columns
near <- rep(9999,n) #list with n entries containing dummy number
for (i in 1:n) {    #for each row in d, find the miniumum distance to a sites
  near[i] <- min(d[i,1:s])                    # distance to nearest study site  
}
sites_near <- sites[near<max_dist,]           # sites to keep
d_near     <- d[near<max_dist,1:s]            # distances to sites kept  

#### QA ####

write.csv(sites_near, file = file.path(getwd(),"Output/QA/sites_near.csv"))
write.csv(d, file = file.path(getwd(),"Output/QA/d.csv"))

#### QA ####
##=================================================================

monitor_data               <- as.data.frame(fread(monitor_data_loc,check.names = FALSE, stringsAsFactors = FALSE)) #read in monitor description data
monitor_data               <- monitor_data[monitor_data$`State Code`>="01" & monitor_data$`State Code`<="56",]     # Restrict to 50 states + DC
monitor_data$`State Code`  <- as.integer(monitor_data$`State Code`)                                                #convert state code column to integers
monitor_data$`County Code` <- as.integer(monitor_data$`County Code`)                                               #convert county code column to integers

countyFIPS_data                  <- as.data.frame(fread(countyFIPS_data_loc,check.names = FALSE, stringsAsFactors = FALSE))             #read in county FIPS code data
countyFIPS_data                  <- countyFIPS_data[countyFIPS_data$County_FIPS_Code>="01" & countyFIPS_data$County_FIPS_Code <="56",]  # Restrict to 50 states + DC
countyFIPS_data$County_FIPS_Code <- as.integer(countyFIPS_data$County_FIPS_Code)                              #convert county FIPS code to int

## read in user chemical data ##
## transform user chemical data vector list into the prepared datatable ##
chemical_data           <- data.frame(matrix(unlist(chem),nrow = length(chem),byrow = T),check.names = F, stringsAsFactors = F) #set up dataframe
colnames(chemical_data) <- c("Chemical")                                                                                        #rename column


## read in user county FIPS data ##
## transform the user FIPS data vector list into the prepared datatable ##
len         <- as.integer(length(FIPS))                                                                                #number of FIPS codes specified by User
county_data <- data.frame(matrix(unlist(FIPS),nrow = len, byrow = T),check.names = F, stringsAsFactors = F)            #set up dataframe
colnames(county_data)        <- c("County_FIPS_Code")                                                                  #rename column
county_data$`State Code`     <- as.integer(substr(county_data$County_FIPS_Code,1,2))                                   #Extract state code
county_data$`County Code`    <- as.integer(substr(county_data$County_FIPS_Code,3,5))                                   #Extract county code
county_data$County_FIPS_Code <- as.integer(county_data$County_FIPS_Code)                                               #convert FIPS code col to integers


## filter data by county(ies) with an inner join ##
county_code_data_info       <- inner_join(county_data,countyFIPS_data,by = "County_FIPS_Code")                          #fetch user county and state names
county_code_data            <- as.data.frame(county_code_data_info[,c("County Code","State Code"),drop=FALSE])          #A table with just county codes

#browser()
sites_near$`State Code`     <- as.integer(sites_near$`State Code`)  #this is to make the inner_join below possible (joining character fields vs numbers&character fields)
sites_near$`County Code`     <- as.integer(sites_near$`County Code`)  #this is to make the inner_join below possible (joining character fields vs numbers&character fields)

site_data_by_county         <- inner_join(county_code_data, sites_near, by = c("County Code","State Code"))              #inner join by county and state codes

monitor_data_by_county      <- inner_join(county_code_data, monitor_data, by = c("County Code","State Code"))           #inner join by county and state codes
monitor_data_by_county_chem <- inner_join(monitor_data_by_county, chemical_data, by = c("Parameter Name" = "Chemical")) #inner join by chemical

hrly_data_by_chem           <- inner_join(hrly_data, chemical_data, by = c("Parameter Name" = "Chemical"))              #inner join by chemical

## Query data by date and time ranges and select only relevant columns and average sample measurement at multiple monitors at the same site ##
## Build Query string ##

QRYstr <- paste0("SELECT `State Code`, `County Code`, `Site Num`, `Parameter Code`, `Latitude`, `Longitude`, `Parameter Name`, `Date Local`, `Time Local`, Avg(`Sample Measurement`) AS `Sample Measurement`, `State Name`, `County Name` ",
                 "FROM hrly_data_by_chem ",
                 "GROUP BY `State Code`, `County Code`, `Site Num`, `Parameter Code`, `Latitude`, `Longitude`, `Parameter Name`, `Date Local`, `Time Local`, `State Name`, `County Name` ",
                 "HAVING (`Date Local` between '",date_start,"' and '",date_end,
                   "' and `Time Local` between '",time_start,"' and '",time_end,"')")

## Run query ##
hrly_data_by_chem_date_time <- sqldf(QRYstr, stringsAsFactors = FALSE)

## Update all -ve sample measurement values to 0 ##
hrly_data_by_chem_date_time[hrly_data_by_chem_date_time$`Sample Measurement`<0,] <- 0

hrly_data_by_chem_date_time_county         <- inner_join(county_code_data, hrly_data_by_chem_date_time, by = c("County Code","State Code"))              #inner join by county and state codes

## Massage hrly data ##
hrly_data_by_chem_date_time_county$`State Code`            <- sprintf("%02d",hrly_data_by_chem_date_time_county$`State Code`)            # fix state code to 2 characters 
hrly_data_by_chem_date_time_county$`County Code`           <- sprintf("%03d",hrly_data_by_chem_date_time_county$`County Code`)           # fix county code to 3 characters 
hrly_data_by_chem_date_time_county$`Site Num`              <- sprintf("%05d",hrly_data_by_chem_date_time_county$`Site Num`)              # fix site number to 5 characters 
hrly_data_by_chem_date_time_county$`Sample Measurement`    <- sprintf("%.5f",hrly_data_by_chem_date_time_county$`Sample Measurement`)    # fix measurements to 5 numbers after decimal
hrly_data_by_chem_date_time_county$`Date Local`            <-  gsub("-","",hrly_data_by_chem_date_time_county$`Date Local`)              #remove "-" in dates    

hrly_data_by_chem_date_time_county$`Station ID`            <- paste0(hrly_data_by_chem_date_time_county$`State Code`,hrly_data_by_chem_date_time_county$`County Code`,
                                                                     hrly_data_by_chem_date_time_county$`Site Num`)                      #concatenate State+County+Site IDs

hrly_data_by_chem_date_time$`State Code`            <- sprintf("%02d",hrly_data_by_chem_date_time$`State Code`)            # fix state code to 2 characters 
hrly_data_by_chem_date_time$`County Code`           <- sprintf("%03d",hrly_data_by_chem_date_time$`County Code`)           # fix county code to 3 characters 
hrly_data_by_chem_date_time$`Site Num`              <- sprintf("%05d",hrly_data_by_chem_date_time$`Site Num`)              # fix site number to 5 characters 
hrly_data_by_chem_date_time$`Sample Measurement`    <- sprintf("%.5f",hrly_data_by_chem_date_time$`Sample Measurement`)    # fix measurements to 5 numbers after decimal
hrly_data_by_chem_date_time$`Date Local`            <-  gsub("-","",hrly_data_by_chem_date_time$`Date Local`)              #remove "-" in dates    

hrly_data_by_chem_date_time$`Station ID`            <- paste0(hrly_data_by_chem_date_time$`State Code`,hrly_data_by_chem_date_time$`County Code`,
                                                                     hrly_data_by_chem_date_time$`Site Num`)               #concatenate State+County+Site IDs

hrly_data_by_chem_date_time$UniqueLocID <- paste0(hrly_data_by_chem_date_time$`Parameter Name`,hrly_data_by_chem_date_time$`Station ID`,hrly_data_by_chem_date_time$`Date Local`, hrly_data_by_chem_date_time$`Time Local`) #unique identifier for each row

## Prepare air quality dataframe ##
air_quality_data2<- data.frame("Parameter Name" = character(),
                                   "Station ID" = character(),
                                   "Date Local" = character(),
                                   "Time Local" = character(),
                           "Sample Measurement" = character(),stringsAsFactors = F, check.names = F) #sample measurement stored as a vector

hrly_data_cols           <- c("00:00","01:00","02:00","03:00","04:00","05:00","06:00","07:00","08:00","09:00","10:00","11:00",
                              "12:00","13:00","14:00","15:00","16:00","17:00","18:00","19:00","20:00","21:00","22:00","23:00") #column names of air quality data

## Transform vector into data frame ##
hrly_data_cols           <- data.frame(matrix(unlist(hrly_data_cols),nrow = length(hrly_data_cols), byrow = T),check.names = F, stringsAsFactors = F)
colnames(hrly_data_cols) <- c("Colnames")

#================

Stations <- unique(hrly_data_by_chem_date_time_county$`Station ID`) #unique stations in the air quality data

begin <- as.Date(date_start) #as R date object
endd  <- as.Date(date_end)   #as R date object
numDays <- as.numeric(as.Date(endd,"%y-%m-%d") - as.Date(begin,"%y-%m-%d")) + 1    #number of days
numMeasurement <- numDays * 24                                                     #total number of measurements

MissingDaysData <- data.frame("Parameter Name" = character(),"Station ID" = character(), "Date Local" = character(),stringsAsFactors = F, check.names = F)

for (i in 1:length(Stations)){                                      #for each station ID
  currentStation                            <- Stations[i]          #current station
  
  hrly_data_by_county_chem_date_time_filterByStatn <- hrly_data_by_chem_date_time_county[hrly_data_by_chem_date_time_county$`Station ID`==currentStation,c("Parameter Name","Station ID","Date Local","Time Local","Sample Measurement")] #filter by current station ID and relevant columns
  statnDataFrac <- (nrow(hrly_data_by_county_chem_date_time_filterByStatn))/numMeasurement #compute fraction of available data 
  statnDataFrac <- sprintf("%.4f",statnDataFrac) #4 decimal places
  
  if(statnDataFrac >= frac){ #if station has enough data...
  
  currentDate       <- begin #current date
  
  while(currentDate <= endd){ #while last date is not reached, do the following for every date within the interval
    Date <- gsub("-","",currentDate) #remove "-" from date

    hrly_data_by_county_chem_date_time_filterByStatnDate   <- hrly_data_by_county_chem_date_time_filterByStatn[hrly_data_by_county_chem_date_time_filterByStatn$`Date Local`==Date,c("Parameter Name","Station ID","Date Local","Time Local","Sample Measurement")] #filter by current date and relevant columns
    
    if (nrow(hrly_data_by_county_chem_date_time_filterByStatnDate)==0){ #missing day
      MissingDaysData[nrow(MissingDaysData)+1, ] <- c(chem,currentStation,Date)
    }
    
    hrly_data_by_county_chem_date_time_filterByStatnDate   <- right_join(hrly_data_by_county_chem_date_time_filterByStatnDate,hrly_data_cols,by=c("Time Local"="Colnames"))                           #right join by expected columns to align values in their right column
    
    hrly_data_by_county_chem_date_time_filterByStatnDate$`Parameter Name`   <- chem 
    hrly_data_by_county_chem_date_time_filterByStatnDate$`Station ID`       <- currentStation
    hrly_data_by_county_chem_date_time_filterByStatnDate$`Date Local`       <- Date

    air_quality_data2 <- rbind(air_quality_data2, hrly_data_by_county_chem_date_time_filterByStatnDate) #add data to the end of the air quality data frame
    
    currentDate <- currentDate + 1 #evaluate next date
  }
}
}

air_quality_data2$UniqueLocID <- paste0(air_quality_data2$`Parameter Name`,air_quality_data2$`Station ID`,air_quality_data2$`Date Local`, air_quality_data2$`Time Local`)

clean_data <- list("Air Quality Data Vector" = air_quality_data2,"Air Quality Data by ChemDateTime" = hrly_data_by_chem_date_time,"Sites Near Data" = sites_near,"Data Sites" = site_data_by_county, "distance to nearby sites" = d_near ,"Monitor Data" = monitor_data_by_county_chem,"Missing Days" = MissingDaysData)

return (clean_data)
}

