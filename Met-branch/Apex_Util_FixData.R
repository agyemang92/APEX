## Fix Data ##
## 08/07/2019 - ICF ##
## This script contains four main functions: "fix_small_window", "fix_large_widows", and "fill_missing_data" are some helper functions to help fix missing air quality data, and "fix_data" uses these functions to eventually fix missing data. There are smaller functions used to perform lapply ##

require(geosphere)  #for lat/long distance calculations
require(spatstat)   #for progress report
require(foreach)    #for parallel processing
require(doParallel) #for parallel processing
require(rlang)

#===============================================================================================================================================
## fix_small_window gaps ##

fix_small_window = function(firsthr,lasthr,n,stationID,positionnn,small_window_fixes,run_type){
  
  ####print(paste0(grep(stationID, unique(sites_air_quality_data$`Station ID`)), "/",length(unique(sites_air_quality_data$`Station ID`))," ",run_type," ",unlist(toString(positionnn))," being evaluated as a small fix."))
  
  #firsthr           : measurement for the hour before first NA
  #lasthr            : measurement for the hour after last NA
  #n                 : total number of data (number missing data + two ends of non missing data)
  #stationID         : station ID
  #positionnn        : list of unique position locators of missing data
  #small_window_fixes: datatable for notes
  
  
  n <- n+2 #total number of data points (including firsthr and lasthr)
    
  x <- c(1, n)           #x axis of two data points at the start and end
  y <- c(firsthr,lasthr) #y axis of two data points at the start and end
  
  missingSmallWin <- data.table(matrix(unlist(positionnn),nrow=length(positionnn),byrow=T),check.names = F, stringsAsFactors = F) #datatable of unique position/location of missing data
  colnames(missingSmallWin) <- c("UniquePosLoc") #rename column
  
  len <- nchar(missingSmallWin$UniquePosLoc[1]) #length of the unique position/location identifier
  
  missingSmallWin$`Parameter name` <- substr(missingSmallWin$UniquePosLoc,1,len-23)      #slice chemical name
  missingSmallWin$Station          <- substr(missingSmallWin$UniquePosLoc,len-22,len-13) #slice station ID
  missingSmallWin$Date             <- substr(missingSmallWin$UniquePosLoc,len-12,len-5)  #slice missing date
  missingSmallWin$Time             <- substr(missingSmallWin$UniquePosLoc,len-4,len)     #slice missing time
  missingSmallWin$Measurement      <- NA                                                 #measurement is missing so NA
  
  #if first hr or last hr is na then fix is not possible, else continue with fix
  if (is.na(firsthr)||is.na(lasthr)){
    
    small_window_fixes <- rbindlist(list(small_window_fixes,list(stationID,unlist(toString(missingSmallWin$Date)),unlist(toString(missingSmallWin$Time)),"No. One of the two needed data points is NA."))) #note details of replacement
    
    return(list("y" = rep(NA,n-2),"swf" = small_window_fixes))
  }
  
  else{
    linear <- approx(x,y,method = "linear",n=n)  #linear interpolate
    linear_y <- linear$y[2:(length(linear$y)-1)] #grap y values for replacement (excluding the first and last data that was used for linear interpolation)
    linear_y <- sprintf("%.5f",linear_y)         #proper number of decimals
    
    small_window_fixes <- rbindlist(list(small_window_fixes,list(stationID,unlist(toString(missingSmallWin$Date)),unlist(toString(missingSmallWin$Time)),"Yes."))) #note details of replacement
    
    return(list("y" = linear_y,"swf" = small_window_fixes)) #return air quality data and data table of where fixes were made
    }
  }

#===============================================================================================================================================
## fix_large_windows ##
fix_large_window = function(nearby_air_quality_data,positionnn,Sites_near_dist,stationID,large_window_fixes,max_dist,run_type){
  
  #nearby_air_quality_data: nearby air quality data
  #positionnn             : positions of missing data          
  #Sites_near_dist        : distances of nearby stations
  #stationID              : current station ID
  #large_window_fixes     : notes for large fix
  #max_dist               : max radius to consider when searching for nearby data
  
  d_nearbyStationsAll        <- as.data.table(Sites_near_dist[,as.character(stationID)])                                    #filter for distances relevant to this station 
  
  setDF(d_nearbyStationsAll, rownames = rownames(Sites_near_dist)) #assign rownames
  colnames(d_nearbyStationsAll) <- c("dist")                       #rename column name
  
  setorder(d_nearbyStationsAll) #order in increasing distance from station
  #browser()
  d_nearbyStations <- d_nearbyStationsAll[d_nearbyStationsAll$dist<=max_dist,,drop=F]   #filter for distances less than or equal to the max_dist specified (extra/redundant)
  d_nearbyStations <- subset(d_nearbyStations,rownames(d_nearbyStations) %in% unique(nearby_air_quality_data$`Station ID`)) #keep only those nearby stations that exist in nearby_air_quality_data
  
  write.csv(d_nearbyStations, file = file.path(getwd(),paste0("Output/QA/",run_type,"_",stationID,"_d_nearbyStations.csv"))) #for QA purposes
  
  missingLargeWin <- data.table(matrix(unlist(positionnn),nrow=length(positionnn),byrow=T),check.names = F, stringsAsFactors = F) #datatable to hold missing data to be filled
  colnames(missingLargeWin) <- c("UniquePosLoc")  #rename column
  
  len <- nchar(missingLargeWin$UniquePosLoc[1])   #length of unique position/location
  
  missingLargeWin$`Parameter name` <- substr(missingLargeWin$UniquePosLoc,1,len-23)      #slice chemical name
  missingLargeWin$Station          <- substr(missingLargeWin$UniquePosLoc,len-22,len-13) #slice station ID
  missingLargeWin$Date             <- substr(missingLargeWin$UniquePosLoc,len-12,len-5)  #slice Date
  missingLargeWin$Time             <- substr(missingLargeWin$UniquePosLoc,len-4,len)     #slice time
  missingLargeWin$Measurement      <- NA                                                 #measurement for missing data is NA
  missingLargeWin$Measurement      <- as.character(missingLargeWin$Measurement)
  
  reached_end_of_stations <- "No"    #indicator to know when the last closest station has been reached

  j <- 2                             #counter for nearest station. This starts at location 2 because the station at location 1 is the same station with missing data with dist 0 meters

  while (reached_end_of_stations=="No" && (TRUE %in% is.na(missingLargeWin$Measurement))){  #while end of nearby stations is not reached and there is some missing data
    
    next_nearest_statn <- rownames(d_nearbyStations)[j]
    next_nearest_statn_dist <- d_nearbyStations$dist[j]
    
    ####print(paste0(grep(stationID, unique(sites_air_quality_data$`Station ID`)), "/",length(unique(sites_air_quality_data$`Station ID`))," ",run_type," ",unlist(toString(positionnn))," being evaluated. Searching next nearest station for data: ",next_nearest_statn," - ", next_nearest_statn_dist," m away"))
    
    L <- nchar(missingLargeWin$`Parameter name`[1]) #length of chemical name
  
    nearStationPosi    <- as.vector(paste0(substr(unlist((missingLargeWin[is.na(missingLargeWin$Measurement) , "UniquePosLoc"])),1,L),next_nearest_statn,substr(unlist((missingLargeWin[is.na(missingLargeWin$Measurement) , "UniquePosLoc"])),L+11,L+23)))   #compute the unique position/location of remaining missing data for this nearby station
    nearStationData    <- nearby_air_quality_data[nearby_air_quality_data$UniqueLocID %in% nearStationPosi, "Sample Measurement"] #filter original air quality data for data for this station at the specified date, chemical, and time using the unique loction identifier
    
    #==========================================================================================================================================
    if((nrow(nearStationData)==0) || ((sum(is.na(nearStationData))))==nrow(nearStationData)){ #if no data was found or all NA was found, then try next station
      j <- j + 1
    }
    else{ #if some numerical data was found
      
      missingLargeWin_PosLoc    <- as.vector(paste0(substr((nearStationPosi),1,L),stationID,substr((nearStationPosi),L+11,L+23)))   #compute the unique position/location of this station's missing data (whose replacement has been found at some nearby station)
      
      temp_replc_df      <- data.table(PosLoc = missingLargeWin_PosLoc, Measurement = unlist(nearStationData),stringsAsFactors = F, check.names = F) #a datatable of just this current replacement iteration.
      temp_replc_df$Date <- missingLargeWin[which(missingLargeWin$UniquePosLoc %in% temp_replc_df$PosLoc),"Date"] #dates of replacement data
      temp_replc_df$Time <- missingLargeWin[which(missingLargeWin$UniquePosLoc %in% temp_replc_df$PosLoc),"Time"] #times of replacement data
      
      set(missingLargeWin,i=which(missingLargeWin$UniquePosLoc %in% missingLargeWin_PosLoc),j="Measurement",value = unlist(nearStationData)) #update missingLargeWin
      
      MisDate <- temp_replc_df[!is.na(temp_replc_df$Measurement),toString(Date)] #which dates were filled with non-NA data
      MisTime <- temp_replc_df[!is.na(temp_replc_df$Measurement),toString(Time)] #which corresponding times were filled with non-NA data
      
      large_window_fixes <- rbindlist(list(large_window_fixes,list(stationID,MisDate,MisTime,paste0("Yes. Fixed with data from ", next_nearest_statn, " at distance ", next_nearest_statn_dist," meters away.")))) #make note
    }
    #==========================================================================================================================================
    
    if(j > nrow(d_nearbyStations)){      #if the end of the nearby stations is reached 
     
      reached_end_of_stations <- "Yes"   #reached end of nearby stations
      
      MisDate <- missingLargeWin[is.na(missingLargeWin$Measurement),toString(Date)] #remaining missing dates
      MisTime <- missingLargeWin[is.na(missingLargeWin$Measurement),toString(Time)] #remaining missing times
      
      large_window_fixes <- rbindlist(list(large_window_fixes,list(stationID,MisDate,MisTime,paste0("No. Did not find data in ", j-1," nearby stations with data within the specified max_dist of ", max_dist, ". Stations looked at: ", unlist(toString(rownames(d_nearbyStations))))))) #make note
    }
  }
  
  missingLargeWin$Measurement <- as.character(missingLargeWin$Measurement)
  
  return(list("mdf" = missingLargeWin,"lwf" = large_window_fixes)) #return the data table of missing data and the notes made of fixes/lack thereof.
}

#===============================================================================================================================================
#if missing data was found
fill_missing_data = function(positionnn,thres,sites_air_quality_data,StationData,first,last,stationID,small_window_fixes,nearby_air_quality_data,Sites_near_dist,large_window_fixes,max_dist,run_type){

  if(length(positionnn) > 0){
  
  missinghrsLen <- length(positionnn) #length of missing data
  
  #is the missinghrsLen above or below the set threshold for small gaps (fixed with linear interpolation) or large gaps (fixed with next nearest monitor)
  #small fix - fix with linear interpolation
  if (missinghrsLen <= thres) { 
    
    small_window_fix   <- fix_small_window(first,last,missinghrsLen,stationID,positionnn,small_window_fixes,run_type) #small fix
    small_window_fixes <- small_window_fix$swf #notes
    missingdf          <- as.character(small_window_fix$y)   #small window fix replacement data
    
    set(StationData,i=which(StationData$`UniqueLocID` %in% positionnn),j="Sample Measurement",value = unlist(missingdf)) #update StationData
    set(sites_air_quality_data,i=which(sites_air_quality_data$`UniqueLocID` %in% positionnn),j="Sample Measurement",value = unlist(missingdf)) #update sites_air_quality_data
  }
  #large fix - fix with data from next nearest monitor
  else{
    
    large_window_fix    <- fix_large_window(nearby_air_quality_data,positionnn,Sites_near_dist,stationID,large_window_fixes,max_dist,run_type) #large fix
    large_window_fixes  <- large_window_fix$lwf  #notes
    missingdf           <- large_window_fix$mdf  #large fix datatable
    missingdf           <- missingdf$Measurement #large fix replacement data
    
    set(StationData,i=which(StationData$`UniqueLocID` %in% positionnn),j="Sample Measurement",value = unlist(missingdf))                         #update StationData
    set(sites_air_quality_data,i=which(sites_air_quality_data$`UniqueLocID` %in% positionnn),j="Sample Measurement",value = unlist(missingdf))   #update StationData
    
  }
}
  
  filled_data <- list("Station Data" = StationData, "sites air quality data" = sites_air_quality_data, "small window fixes" = small_window_fixes, "large window fixes" = large_window_fixes)
  return(filled_data)
}
#===============================================================================================================================================
# fix data #

fix_data = function(nearby_air_quality_data,sites_air_quality_data,Sites_near_dist,small_window_fixes,large_window_fixes,thres,max_dist,run_type){
  
  uniqueStatnID <- unique(sites_air_quality_data$`Station ID`)
  
  #function to fill NAs in a station (used in lapply with uniqueStatnID)
  fillStatnNA = function(stationID){
    
    StationData <- sites_air_quality_data[sites_air_quality_data$`Station ID`==stationID, ]
    print(paste0(grep(stationID, unique(sites_air_quality_data$`Station ID`)), "/",length(unique(sites_air_quality_data$`Station ID`))," ===================== ",run_type," ",stationID," being evaluated.")) #print progress
    
    pos <- 2                     #where to begin evalation. should be second position (the very first position would have been looked at already)
    lastloc <- nrow(StationData)-1 #where to end evaluation. should be last but one position (the very last position would have been looked at already)
    
    #=============================================================================
    #if the first data is NA, then fix it first
    if(is.na(StationData[1,6])){
      #browser()
      pos1 <- 1
      positionnn <- c()
      beg   <- StationData[1:24,]                                       #first 24 hours of the data
      
      if(sum(is.na(beg))!=24){ #if all 24 hrs of the start of the station is missing, then do not proceed
      
        while(is.na(StationData[pos1,6])){
        positionnn <- append(positionnn,StationData[pos1,5],length(positionnn))
        pos1 <- pos1 + 1 #evaluate next row
        }
      
      #print(paste0(grep(stationID, unique(sites_air_quality_data$`Station ID`)), "/",length(unique(sites_air_quality_data$`Station ID`))," ===================== ",run_type," ",stationID," being evaluated."," First end of data is NA ending at hour ",pos1))
        
      missinghrsLenF <- length(positionnn) #number of missing data at the beginning of the station data
      pos <- missinghrsLenF + 1 #update starting location (for middle section after ends of data have been fixed)
        
      last  <- as.numeric(StationData[pos1,6])
      firstpos1 <- 24 #start from end of first 24 hour period
      
      #finding first non NA data
      while(is.na(beg[firstpos1,6])){
        firstpos1 <- firstpos1 - 1 #evaluate previous location
      }
      
      first <- as.numeric(beg[firstpos1,6])
      
      filled_missing_data    <- fill_missing_data(positionnn,thres,sites_air_quality_data,StationData,first,last,stationID,small_window_fixes,nearby_air_quality_data,Sites_near_dist,large_window_fixes,max_dist,run_type)

      StationData            <<- filled_missing_data$`Station Data`             #updated stationData
      sites_air_quality_data <<- filled_missing_data$`sites air quality data`   #updated sites_air_quality_data
      small_window_fixes     <<- filled_missing_data$`small window fixes`       #updated notes
      large_window_fixes     <<- filled_missing_data$`large window fixes`       #updated notes
      
      }
      }
    
    #=============================================================================
    #if the last data is NA, then fix it too
   if(is.na(StationData[nrow(StationData),6])){
    #browser()
    pos1 <- nrow(StationData)
    positionnn <- c()
    enddn <- StationData[(nrow(StationData)-23):(nrow(StationData)),] #last 24 hours of the data. nrow(StationData) MUST equal 24 * number of days in the date range
    
    if(sum(is.na(enddn))!=24){
     
      while(is.na(StationData[pos1,6])){#find consecutive NAs
        positionnn <- append(positionnn,StationData[pos1,5],length(positionnn)) #append date
        pos1 <- pos1 - 1
      }
      
    positionnn <- rev(positionnn) #reverse order
    
    #print(paste0(grep(stationID, unique(sites_air_quality_data$`Station ID`)), "/",length(unique(sites_air_quality_data$`Station ID`))," ===================== ",run_type," ",stationID," being evaluated."," Last end of data is NA starting at hour ",pos1))
      
    missinghrsLenL <- length(positionnn) #missing hours at the end of the station
    lastloc <- nrow(StationData) - missinghrsLenL #update the last record to look at for the middle section after ends of data have been fixed 
    
    first <- as.numeric(StationData[pos1,6])
    lastpos1 <- 1 #start from the beginning of last 24 hours
    #finding last non NA data
    while(is.na(enddn[lastpos1,6])){
      lastpos1 <- lastpos1 + 1
    }
    last <- as.numeric(enddn[lastpos1,6])
    
    filled_missing_data    <- fill_missing_data(positionnn,thres,sites_air_quality_data,StationData,first,last,stationID,small_window_fixes,nearby_air_quality_data,Sites_near_dist,large_window_fixes,max_dist,run_type)
    
    StationData            <<- filled_missing_data$`Station Data`           #updated stationData
    sites_air_quality_data <<- filled_missing_data$`sites air quality data` #updated sites_air_quality_data
    small_window_fixes     <<- filled_missing_data$`small window fixes`     #updated notes
    large_window_fixes     <<- filled_missing_data$`large window fixes`     #updated notes
    
       
    }
  }
    #=============================================================================
    #now that the ends have been fixed, any gaps can be filled with either linear interpolation or data from nearest station
    
    if(TRUE %in% is.na(StationData[pos:lastloc, "Sample Measurement"])){ #if there are any missing data
      
      NAlocation <- which(is.na(StationData[pos:lastloc, "Sample Measurement"])) #where are NAs in the data (except for the ends, which have already been looked at)
      
      NAlocation <- as.list(NAlocation)     #list of indices of where there is one or more consecutive NAs
      #browser()
      #this function fills in NAs at a specified location (used in lapply with NAlocation)
      fillNA = function(pos0){

      pos0 <- as.numeric(pos0)
      pos0 <- pos0 +(pos-1)
      #print(pos0)
      positionnn <- c() #vector to hold unique position locator of missing data
      
      first <- as.numeric(StationData[pos0-1,6]) #data for hour before an NA

      while(is.na(StationData[pos0,6]) && (pos0 <= lastloc)){ #find consecutive NAs within boundaries
        
        positionnn <- append(positionnn,as.character(StationData[pos0,5]),length(positionnn)) #append unique position locator
        
        pos0 <- pos0 + 1 #evaluate the next row
      }
      
      last <- as.numeric(StationData[pos0,6]) #data for hour after an NA

      filled_missing_data    <- fill_missing_data(positionnn,thres,sites_air_quality_data,StationData,first,last,stationID,small_window_fixes,nearby_air_quality_data,Sites_near_dist,large_window_fixes,max_dist,run_type)
      
      StationData            <<- filled_missing_data$`Station Data`            #updated stationData
      sites_air_quality_data <<- filled_missing_data$`sites air quality data`  #updated sites_air_quality_data
      small_window_fixes     <<- filled_missing_data$`small window fixes`      #updated notes
      large_window_fixes     <<- filled_missing_data$`large window fixes`      #updated notes  
      #browser()
      }
      
      filledNA <- lapply(NAlocation,fillNA) #fill consecutive NAs starting at the indicated positions in NAlocation
    }
    
  }
  filledStatnNA <- lapply(uniqueStatnID,fillStatnNA) #fill NAs in each of the listed stations
  
  return(list("air quality" = sites_air_quality_data, "lwdf" = large_window_fixes, "smdf" = small_window_fixes))
}

