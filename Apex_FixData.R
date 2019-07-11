## APEX Data Management ##
## 07/11/2019 - ICF ##
## This script contains four functions (fix_missing_days, fix_small_window, find_dist_to_other_stations, and fix_large_widows) that help to fix missing air quality data ##

#rm(list=ls())

require(geosphere)  #for lat/long distance calculations
require(spatstat)   #for progress report
require(foreach)    #for parallel processing
require(doParallel) #for parallel processing

source(file.path(getwd(),"Apex_FormatData.R"))

#===============================================================================================================================================
## fix_missing_days - insert any missing day data with NAs ##
fix_missing_days = function(air_quality_data,date_start,date_end){
  
  begin <- as.Date(date_start) #as R date object
  endd  <- as.Date(date_end)   #as R date object
  
  MissingDays <- 0 #counter for number of missing days
  
  for (station in unique(air_quality_data$`Station ID`)) { #for each unique station in air quality data
    stationData <- air_quality_data[air_quality_data$`Station ID`==station,] #filter air quality data to this station
    currentDate <- begin #current date
    
    while(currentDate <= endd){ #while current date is within the start and end date boundaries for this air quality data...
      Date <- gsub("-","",currentDate) #remove "-" from date
      stationDateData <- stationData[stationData$Date==Date,] #filter by this date
      
      if(nrow(stationDateData)==0){ #if data for this date for this station does not exist
        air_quality_data[nrow(air_quality_data)+1, ] <- c(NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,Date,station) #insert NAs 
        MissingDays <- MissingDays + 1 #count this as a missing day
      }
      
      currentDate <- currentDate + 1 #evaluate next date
    }
  }
  
  air_quality_data <- air_quality_data[order(air_quality_data$`Station ID`),] #order by increasing Station ID
  air_quality_data <- air_quality_data[order(air_quality_data$Date),] #order by increasing date
  air_quality_data <- air_quality_data[order(air_quality_data$`Station ID`),] #finally order by increasing station ID
  
  return(list("aqd" = air_quality_data, "md" = MissingDays)) #return air quality data and number of missing days
}

#===============================================================================================================================================
## fix_small_window - small window is defined as 1-hour gaps ##
fix_small_window = function(HourlyData,firsthr,lasthr,n){
  
  #small_window_fixes <- data.frame("Row Number" = integer(), "Column Number" = integer(), "Time" = character(),"Date"= character(),"Station ID" = character(), stringsAsFactors = F, check.names = F) #keep track of where replacements are made
  #browser()
  
  #firsthrData <-  HourlyData
  
  firsthrData <-  HourlyData[,firsthr]
  lasthrData  <- HourlyData[,lasthr]
  
  firsthr <- as.numeric(substr(firsthr,1,unlist(gregexpr(":",firsthr))[1]-1))
  lasthr <- as.numeric(substr(lasthr,1,unlist(gregexpr(":",lasthr))[1]-1))
  
  x <- c(firsthr,lasthr)
  y <- c(firsthrData,lasthrData)
  
  linear <- approx(x,y,method = "linear",n=n)
  linear_y <- linear$y
  
  return(linear_y) #return air quality data and data frame of where fixes were made
  
}

#===============================================================================================================================================
## find_dist_to_other_stations - calculate distance from a station to all other stations within stated maximum distance listed in the sites csv  ##
find_dist_to_other_stations = function(StationID,max_dist,site_data){
  
  #browser()
  distance_to_all_sites <- site_data[,c("State Code","County Code","Site Number","Latitude","Longitude","Zip Code","State Name")] #only interested in the listed columns
  
  distance_to_all_sites$`State Code`            <- sprintf("%02d",distance_to_all_sites$`State Code`)            # fix state code to 2 characters 
  distance_to_all_sites$`County Code`           <- sprintf("%03d",distance_to_all_sites$`County Code`)           # fix county code to 3 characters 
  distance_to_all_sites$`Site Number`           <- sprintf("%05d",distance_to_all_sites$`Site Number`)           # fix site number to 5 characters
  
  distance_to_all_sites$StationID <- paste0(distance_to_all_sites$`State Code`,distance_to_all_sites$`County Code`,distance_to_all_sites$`Site Number`) #build station ID
  
  StationLat  <- site_data[distance_to_all_sites$StationID==StationID,4] #Latitude of station of interest 
  StationLong <- site_data[distance_to_all_sites$StationID==StationID,5] #longitude of station of interest
  
  distance_to_all_sites$Distance_m <- as.vector(t(distm(c(StationLong,StationLat),distance_to_all_sites[,5:4],fun = distHaversine))) #compute and insert the Haversine distance from all stations to this station of interest
  
  distance_to_all_sites <- as.data.frame(distance_to_all_sites) #as data frame
  
  distance_to_all_sites <- distance_to_all_sites[distance_to_all_sites$Distance_m>0,] #Distance_m = 0 is the same as station of interest, so ignore
  distance_to_all_sites <- distance_to_all_sites[distance_to_all_sites$Distance_m<=max_dist,] #filter by those stations within stated maximum distance
  
  distance_to_all_sites <- distance_to_all_sites[order(distance_to_all_sites$Distance_m),] #sort dataframe by ascending distance_m
  distance_to_all_sites <- distance_to_all_sites[!is.na(distance_to_all_sites$Distance_m),] #remove rows with missing distances
  
  return(distance_to_all_sites[,c("StationID","Distance_m")]) #return dataframe of just the station ID and their distances to the station of interest
}

#===============================================================================================================================================
## fix_large_windows - fix windows missing more than an hour of data ##
fix_large_window = function(missingdf,dist_to_other_stations,stationID,Date,large_window_fixes){
  
  #browser()
  
  i <- 1    #indicator for station ID in the while loop below. 1 = first station (with shortest distance to current station ID) in dist_to_other_stations data frame
  found_some_data <- "No" #indicator to know if some replacement data (not necessarily for all missing hours) has been found 
  reached_end_of_stations <- "No" #indicator to know when the last closest station has been reached
  
  while(TRUE %in% is.na(missingdf) && reached_end_of_stations=="No"){ #while some data is yet to be replaced and the last of the nearby stations is not reached
    #browser()
    missingdf_rem <- missingdf[,is.na(missingdf)] #remaining NAs
    #build query criteria for the missing hours to search for
    hr_crit <- ""
    for (k in colnames(missingdf_rem)){
      hr_crit <- paste0(hr_crit,
                        "(nearby_station_data.`Time Local` = '",k,"'",") or ")
    }
    
    hr_crit <- substr(hr_crit,1,nchar(hr_crit)-4) #remove last "or"
    
    
    closest_station <- dist_to_other_stations$StationID[i] #closest station to station of interest with missing data
    nearby_station_data <- air_quality_data_ChemDateTime[air_quality_data_ChemDateTime$`Station ID`==closest_station & air_quality_data_ChemDateTime$`Date Local`==Date,] #filter the original air quality data that is 
    #filtered only by chemical, data, and time to this station closest to the station of interest missing data
    
    #build query to filter the nearby_station_data by the current date and time.
    QRYstr <- paste0("SELECT nearby_station_data.`Time Local`, nearby_station_data.`Sample Measurement`
                                                    FROM nearby_station_data ",
                     "WHERE (", hr_crit,")")
    
    
    nearby_Station_data_by_date_time <- sqldf(QRYstr, stringsAsFactors = FALSE) #execute query
    
    #if data was found, then replace any data that was found
    if(nrow(nearby_Station_data_by_date_time)>0){                                     
      
      nearby_Station_data_by_date_time$`Sample Measurement` <- as.numeric(nearby_Station_data_by_date_time$`Sample Measurement`) #set sample measurement as numeric
      
      n <- nearby_Station_data_by_date_time[,1] #names of columns
      
      nearby_Station_data_by_date_time <- t(nearby_Station_data_by_date_time[,-1]) #transpose only the last column (sample measurement)
      
      colnames(nearby_Station_data_by_date_time) <- n #assign colnames
      nearby_Station_data_by_date_time <- as.data.frame(nearby_Station_data_by_date_time) #as data frame
      nearby_Station_data_by_date_time[1,]<- sprintf("%.5f",nearby_Station_data_by_date_time[1,]) #fix number of digits to 5 
      
      #browser()
      missingdf[,colnames(missingdf_rem)] <- nearby_Station_data_by_date_time
      found_some_data <- "Yes"
    }
    
    #if after searching no data could be found (because it doesn't exist in any other stations within max distance), then try next closest station
    else{
      #browser()
      found_some_data <- "No"   #no data was found
      i <- i + 1#evaluate next station id to next station id
      
      if(i>nrow(dist_to_other_stations)){ #end of nearest station datatable reached
        #browser()
        reached_end_of_stations <- "Yes"
      }
    }
  }
  
  #browser()
  
  if(TRUE %in% is.na(missingdf)){
    #browser()
    large_window_fixes[nrow(large_window_fixes)+1,] <- c(Date,stationID,paste0("No: ",unlist(toString(colnames(is.na(missingdf)))))) #make note
  }
  else{
    #browser()  
    large_window_fixes[nrow(large_window_fixes)+1,] <- c(Date,stationID,paste0("Yes: ",unlist(toString(colnames(missingdf))))) #make note
  }
  
  return(list("mdf" = missingdf,"lwf" = large_window_fixes))
  
}

#===============================================================================================================================================
#===============================================================================================================================================
