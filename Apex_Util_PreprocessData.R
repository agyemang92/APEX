## Preprocess Data ##
## Completed August 6, 2019 - ICF ##
## This script contains 1 function (preprocess_data) that processes the raw data - filtering for relevant time period, chemical, and location, filling in missing days, making sure all hours are available, etc ##
 
## load packages ##
require(dplyr)
require(sqldf)
require(data.table)
#require(xlsx)
require(readr)
require(foreach)
require(doParallel)
require(geosphere)

preprocess_data = function(date_start,date_end,time_start,time_end,FIPS,frac,chem,hrly_data_loc,site_data_loc,monitor_data_loc,countyFIPS_data_loc,ad,aq,ad_ofpn,aq_ofpn,max_dist){
#browser()
  ## Error handling start ##
  
  #try-catch blocks to:
  #check that date_end > date_start
  if(date_end <= date_start) {
    stop("Please enter an end date that is after your start date.")
  }
  #Check that the MaxFrac is a fraction not percent.
  if(frac>1) {
    stop("Please enter a MaxFrac value less than or equal to 1.0 ")
  }
  #Check that the end date is not past the current date.
  if(date_end >= Sys.Date()) {
    stop("Please enter a valid end date.")
  }
  #Provide warning if MaxD is too small.
  if(max_dist<=10000) {
    warning("Please note that the MaxD should be provided in meters. The distance you provided is less than 10,000m (10km), which may result in missing values unable to be fixed.")
  }
  
  ## Error handling end ##

  #=========================================================================================================================================================================================================================================
## Script to prepare sites based on distance to study area ##
## Graham Glen, ICF, July 2019 ##
sites                <- as.data.table(fread(site_data_loc,check.names = FALSE, stringsAsFactors = FALSE))    #read in site description data

sites <- sites[sites$`State Code`>="01" & sites$`State Code`<="56",]                        # Restrict to 50 states + DC
sites <- sites[sites$Latitude!=0 & sites$Longitude!=0,]                                     # Sites must have lat and long
sites$FIPS <- substr(100000+1000*as.numeric(sites$`State Code`)+sites$`County Code`,2,6)    # Create FIPS for all sites
study_FIPS <- FIPS                                                                          # user provided codes

if((FIPS %in% sites$FIPS)==FALSE) {
  stop("No sites exist for the given FIPS code(s). Please try a different FIPS code.")
}

sites <- sites[!is.na(sites$`Site Number`),] #remove all NAs

sites$`State Code`            <- sprintf("%02d",as.integer(sites$`State Code`))            #fix state code to 2 characters 
sites$`County Code`           <- sprintf("%03d",as.integer(sites$`County Code`))           #fix county code to 3 characters 
sites$`Site Number`           <- sprintf("%05d",as.integer(sites$`Site Num`))              #fix site number to 5 characters 
sites$`Station ID`            <- paste0(sites$`State Code`,sites$`County Code`,sites$`Site Num`)                                  #concatenate State+County+Site IDs


study <- sites[sites$FIPS %in% study_FIPS,]  #filter for sites with study FIPS

study$`State Code`            <- sprintf("%02d",as.integer(study$`State Code`))            #fix state code to 2 characters 
study$`County Code`           <- sprintf("%03d",as.integer(study$`County Code`))           #fix county code to 3 characters 
study$`Site Number`           <- sprintf("%05d",as.integer(study$`Site Num`))              #fix site number to 5 characters 
study$`Station ID`            <- paste0(study$`State Code`,study$`County Code`,study$`Site Num`)                                  #concatenate State+County+Site IDs
#browser()
d <- as.data.table(distm(cbind(sites$Longitude,sites$Latitude),cbind(study$Longitude,study$Latitude)),stringsAsFactors=F,check.names=F)    # create matrix of distances

colnames(d) <- as.character(study$`Station ID`) #name columns
setDF(d,rownames = as.character(sites$`Station ID`))

n <- nrow(d)                          #number of rows
s <- ncol(d)                          #number of columns
near <- rep(9999,n)                   #list with n entries containing dummy number
for (i in 1:n) {                      #for each row in d, find the miniumum distance to a sites
  near[i] <- min(d[i,1:s])            #distance to nearest study site  
}

max_dist             <- max_dist                    #max distance in meters to consider set by user
max_dist_greatest    <- min(300000,3*max_dist)      #maximum distance of area that will be considered. No more than 300km 

sites_near <- sites[near<max_dist_greatest,]   #sites to keep
d_near     <- d[near<max_dist_greatest,1:s]    #distances to sites kept

rm(sites) #clean the full list of sites away to free up RAM
gc()      #remove the full list of sites from memory
#=========================================================================================================================================================================================================================================
## prepare user chemical data ##
## transform user chemical data vector list into the prepared datatable ##
chemical_data           <- data.table(matrix(unlist(chem),nrow = length(chem),byrow = T),check.names = F, stringsAsFactors = F) #set up data table
colnames(chemical_data) <- c("Chemical")                                                                                        #rename column
#=========================================================================================================================================================================================================================================
#prepare county data
countyFIPS_data                  <- as.data.table(fread(countyFIPS_data_loc,check.names = FALSE, stringsAsFactors = FALSE)) #read in county FIPS code data
countyFIPS_data$County_FIPS_Code <- as.integer(countyFIPS_data$County_FIPS_Code)                                            #convert county FIPS code to int
countyFIPS_data$County_FIPS_Code <- sprintf("%05d",countyFIPS_data$County_FIPS_Code)                                        #fix number to 5 characters 
#==========================================================================================================================================================================================================================================
## transform the user FIPS data vector list into the prepared datatable ##
len                          <- as.integer(length(FIPS))                                                                     #number of FIPS codes specified by User
county_data                  <- data.table(matrix(unlist(FIPS),nrow = len, byrow = T),check.names = F, stringsAsFactors = F) #set up dataframe
colnames(county_data)        <- c("County_FIPS_Code")                                                      #rename column
county_data$`State Code`     <- substr(county_data$County_FIPS_Code,1,2)                                   #Extract state code
county_data$`County Code`    <- substr(county_data$County_FIPS_Code,3,5)                                   #Extract county code
county_data$County_FIPS_Code <- county_data$County_FIPS_Code                                               #convert FIPS code col to integers

county_code_data_info       <- inner_join(county_data,countyFIPS_data,by = "County_FIPS_Code")                          #fetch user county and state names
county_code_data            <- as.data.table(county_code_data_info[,c("County Code","State Code"),drop=FALSE])          #A table with just county codes

#==========================================================================================================================================================================================================================================
#site data by county
#browser()
site_data_by_county         <- inner_join(county_code_data, sites_near, by = c("County Code","State Code"))              #inner join by county and state codes
#==========================================================================================================================================================================================================================================
## read in hourly data ##
if (length(hrly_data_loc)>1) {
  hrly_data                  <- bind_rows(lapply(hrly_data_loc,fread))
  hrly_data                  <- as.data.table(hrly_data, check.names = FALSE, stringsAsFactors = FALSE)
} else {
  hrly_data                  <- as.data.table(fread(hrly_data_loc,check.names = FALSE, stringsAsFactors = FALSE))
}

if(is.na(hrly_data[1,2])==TRUE) {
  stop("The data chosen has 0 rows. Please choose a file with data. This information is provided under each file on the AQS website")
}
#==========================================================================================================================================================================================================================================
## filter hourly data further ##
#browser()
hrly_data$`Station ID` <- paste0(sprintf("%02d",hrly_data$`State Code`),sprintf("%03d",hrly_data$`County Code`),sprintf("%05d",hrly_data$`Site Num`))  #concatenate State+County+Site IDs
data_unit <<- unique(hrly_data$`Units of Measure`)
#browser()
hrly_data_nearby  <- hrly_data[hrly_data$`Station ID` %in% sites_near$`Station ID`,] #hourly data of nearby stations. Later on the tool will search this data table for nearby data to fix large gaps.

rm(hrly_data) #clean the full list of hourly data away to free up RAM
gc()      #remove the full list of hourly data from memory

hrly_data_by_chem_nearby   <- inner_join(hrly_data_nearby, chemical_data, by = c("Parameter Name" = "Chemical"))              #inner join by chemical

rm(hrly_data_nearby)
gc()

## Query data by date and time ranges and select only relevant columns and average sample measurement at multiple monitors at the same site ##
## Build Query string ##

QRYstr <- paste0("SELECT `State Code`, `County Code`, `Site Num`, `Parameter Code`, `Latitude`, `Longitude`, `Parameter Name`, `Date Local`, `Time Local`, Avg(`Sample Measurement`) AS `Sample Measurement`, `State Name`, `County Name`, `Station ID` ",
                 "FROM hrly_data_by_chem_nearby ",
                 "GROUP BY `State Code`, `County Code`, `Site Num`, `Parameter Code`, `Latitude`, `Longitude`, `Parameter Name`, `Date Local`, `Time Local`, `State Name`, `County Name`, `Station ID` ",
                 "HAVING (`Date Local` between '",date_start,"' and '",date_end,
                   "' and `Time Local` between '",time_start,"' and '",time_end,"')")

## Run query ##
hrly_data_by_chem_date_time_nearby <- sqldf(QRYstr, stringsAsFactors = FALSE)

rm(hrly_data_by_chem_nearby)
gc()
hrly_data_by_chem_date_time_nearby$`State Code`            <- sprintf("%02d",hrly_data_by_chem_date_time_nearby$`State Code`)            # fix state code to 2 characters 
hrly_data_by_chem_date_time_nearby$`County Code`           <- sprintf("%03d",hrly_data_by_chem_date_time_nearby$`County Code`)           # fix county code to 3 characters 
hrly_data_by_chem_date_time_nearby$`Site Num`              <- sprintf("%05d",hrly_data_by_chem_date_time_nearby$`Site Num`)              # fix site number to 5 characters 

## Update all -ve sample measurement values to 0 ##
hrly_data_by_chem_date_time_nearby[(hrly_data_by_chem_date_time_nearby$`Sample Measurement` < 0), "Sample Measurement"] <- 0.0


hrly_data_by_chem_date_time_nearby$`Sample Measurement`    <- sprintf("%.5f",hrly_data_by_chem_date_time_nearby$`Sample Measurement`)   # fix measurements to 5 numbers after decimal
hrly_data_by_chem_date_time_nearby$`Date Local`            <- gsub("-","",hrly_data_by_chem_date_time_nearby$`Date Local`)              #remove "-" in dates    

#rm(hrly_data_by_chem_date_time)
#gc()

#==========================================================================================================================================================================================================================================
#fix nearby data to have records for each day in the user specified interval and to have data/NA for each hour in a 24-hour period#

begin <- as.Date(date_start) #as R date object
endd  <- as.Date(date_end)   #as R date object
numDays <- as.numeric(as.Date(endd,"%y-%m-%d") - as.Date(begin,"%y-%m-%d")) + 1    #number of days
numMeasurement <- numDays * 24                                                     #total number of measurements

## Prepare nearby air quality dataframe to hold data for nearby sites##
nearby_air_quality_data<- data.table("Parameter Name" = character(),
                                         "State Code" = character(),
                                        "County Code" = character(),
                                           "Site Num" = character(),
                                         "Station ID" = character(), 
                                         "Date Local" = character(),
                                         "Time Local" = character(),
                                 "Sample Measurement" = character(), stringsAsFactors = F, check.names = F) #air quality dataframes of nearby sites

hrly_data_cols           <- c("00:00","01:00","02:00","03:00","04:00","05:00","06:00","07:00","08:00","09:00","10:00","11:00",
                              "12:00","13:00","14:00","15:00","16:00","17:00","18:00","19:00","20:00","21:00","22:00","23:00") #column names of air quality data. Always 24 hours.

## Transform vector into data table ##
hrly_data_cols           <- data.table(matrix(unlist(hrly_data_cols),nrow = length(hrly_data_cols), byrow = T),check.names = F, stringsAsFactors = F)
colnames(hrly_data_cols) <- c("Colnames")

MissingDaysData <- data.table("Parameter Name" = character(),"Station ID" = character(), "Date Local" = character(),stringsAsFactors = F, check.names = F) #notes for missing days data

Stations <- as.list(unique(hrly_data_by_chem_date_time_nearby$`Station ID`)) #unique stations in the nearby air quality data

#For a station and then for each day of a station and for each hour of the day, this major_fill_function makes sure there is data/NA. Makes note of missing days.

major_fill_func = function(currentStation){

  StateCd  <- substr(currentStation,1,2)  #state code
  CountyCd <- substr(currentStation,3,5)  #county code
  SiteNm   <- substr(currentStation,6,10) #site number
  
  print(paste0(grep(currentStation,Stations),"/",length(Stations))) #show progress
  hrly_data_by_chem_date_time_filterByStatn <- hrly_data_by_chem_date_time_nearby[hrly_data_by_chem_date_time_nearby$`Station ID`==currentStation,c("Parameter Name","State Code", "County Code", "Site Num", "Station ID","Date Local","Time Local","Sample Measurement")] #filter by current station ID and relevant columns
  
  currentDate <- begin #current date
  
  dates <- as.list(seq(begin,endd,by=1)) #list of all dates within the specified interval
  
  #for each date in the specified interval, this minor_fill_function makes sure there is data for every hour
  minor_fill_func = function(Date){  
    Date <- gsub("-","",Date) #remove "-" from date
    
    hrly_data_by_chem_date_time_filterByStatnDate   <- hrly_data_by_chem_date_time_filterByStatn[hrly_data_by_chem_date_time_filterByStatn$`Date Local`==Date,] #filter by current date/day
    
    if (nrow(hrly_data_by_chem_date_time_filterByStatnDate)==0){ #missing day
     
      MissingDaysData <<- rbindlist(list(MissingDaysData,list(chem,currentStation,Date)))

    }
    
    hrly_data_by_chem_date_time_filterByStatnDate   <- right_join(hrly_data_by_chem_date_time_filterByStatnDate,hrly_data_cols,by=c("Time Local"="Colnames"))                           #right join by expected columns to align values in their right column
    
    hrly_data_by_chem_date_time_filterByStatnDate$`Parameter Name`   <- chem 
    hrly_data_by_chem_date_time_filterByStatnDate$`Station ID`       <- currentStation
    hrly_data_by_chem_date_time_filterByStatnDate$`Date Local`       <- Date
    hrly_data_by_chem_date_time_filterByStatnDate$`State Code`       <- StateCd
    hrly_data_by_chem_date_time_filterByStatnDate$`County Code`      <- CountyCd
    hrly_data_by_chem_date_time_filterByStatnDate$`Site Num`         <- SiteNm
    
    return(hrly_data_by_chem_date_time_filterByStatnDate)
    
  }
  
  stn_period_data <- lapply(dates, minor_fill_func) #apply the minor_fill_func function to each date
  
  nearby_air_quality_data <- rbindlist(list(nearby_air_quality_data,bind_rows(stn_period_data))) #combine individual returns and bind to the nearby_air_quality_data datatable
  
  return(nearby_air_quality_data)
}
#browser()
all_stn_period_data <- lapply(Stations, major_fill_func)  #apply the function major_fill_func function to each station
nearby_air_quality_data <- bind_rows(all_stn_period_data) #combine individual returns

rm(hrly_data_by_chem_date_time_nearby)
gc()

nearby_air_quality_data$UniqueLocID <- paste0(nearby_air_quality_data$`Parameter Name`,nearby_air_quality_data$`Station ID`,nearby_air_quality_data$`Date Local`, nearby_air_quality_data$`Time Local`) #unique identifier for each row

hrly_data_by_chem_date_time_county <- inner_join(county_code_data, nearby_air_quality_data, by = c("County Code","State Code"))              #inner join by county and state codes to get data for selected counties from the nearby data

## Prepare air quality dataframe to hold data for selected region(s)##
air_quality_data<- data.table("Parameter Name" = character(),
                              "Station ID" = character(),
                              "Date Local" = character(),
                              "Time Local" = character(),
                              "UniqueLocID" = character(),
                              "Sample Measurement" = character(),stringsAsFactors = F, check.names = F) #air quality dataframes of selected sites

stations2 <- unique(hrly_data_by_chem_date_time_county$`Station ID`)

#this function will remove stations with low data from consideration 
remove_bad_statn = function(i){
  currentStation                            <- stations2[i]          #current station
  
  hrly_data_by_county_chem_date_time_filterByStatn <- hrly_data_by_chem_date_time_county[hrly_data_by_chem_date_time_county$`Station ID`==currentStation,c("Parameter Name","Station ID","Date Local","Time Local","UniqueLocID","Sample Measurement")] #filter by current station ID and relevant columns
  #browser()
  statnDataFrac <- (sum(is.na(hrly_data_by_county_chem_date_time_filterByStatn$`Sample Measurement`)))/numMeasurement #compute fraction of missing data 
  statnDataFrac <- sprintf("%.4f",statnDataFrac) #4 decimal places
  statnDataFrac <<- as.numeric(statnDataFrac) #as numeric
  #browser()
  if(statnDataFrac < frac){ #if station has enough data...
    air_quality_data <<- rbindlist(list(air_quality_data, hrly_data_by_county_chem_date_time_filterByStatn)) #add station data to the end of the air quality data table
    }
}

bad_statns_removed <- lapply(as.list(seq(1,length(stations2),by=1)),remove_bad_statn) #apply function remove_bad_statn to each station

processed_data <- list("Air Quality Data for Selected Sites" = air_quality_data, "Air Quality Data for Nearby Sites" = nearby_air_quality_data, "Sites Near Data" = sites_near, "distances to nearby sites" = d_near, "Missing Days" = MissingDaysData)

return (processed_data)
statnDataFrac <<- as.numeric(statnDataFrac) #as numeric
}

