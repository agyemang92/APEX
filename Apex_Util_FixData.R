## Fix Data ##
## 07/26/2019 - ICF ##
## This script contains three functions: "fix_small_window", and "fix_large_widows" are helper functions to help fix missing air quality data and "fix_data" does the fixing ##

require(geosphere)  #for lat/long distance calculations
require(spatstat)   #for progress report
require(foreach)    #for parallel processing
require(doParallel) #for parallel processing
require(rlang)

#===============================================================================================================================================
## fix_small_window gaps ##

fix_small_window = function(firsthr,lasthr,n,stationID,positionnn,small_window_fixes){
  
  #firsthr:
  #lasthr:
  #n:
  #stationID:
  #positionnn:
  #small_window_fixes:
  
  n <- n+2 #total number of data points (including firsthr and lasthr)
    
  x <- c(1, n) #x axis of two data points at the start and end
  y <- c(firsthr,lasthr) #y axis of two data points at the start and end
  
  missingSmallWin <- data.frame(matrix(unlist(positionnn),nrow=length(positionnn),byrow=T),check.names = F, stringsAsFactors = F) #dataframe of unique position/location of missing data
  colnames(missingSmallWin) <- c("UniquePosLoc") #rename column
  
  len <- nchar(missingSmallWin$UniquePosLoc[1]) #length of the unique position/location identifier
  
  missingSmallWin$`Parameter name` <- substr(missingSmallWin$UniquePosLoc,1,len-23)      #slice chemical name
  missingSmallWin$Station          <- substr(missingSmallWin$UniquePosLoc,len-22,len-13) #slice station ID
  missingSmallWin$Date             <- substr(missingSmallWin$UniquePosLoc,len-12,len-5)  #slice missing date
  missingSmallWin$Time             <- substr(missingSmallWin$UniquePosLoc,len-4,len)     #slice missing time
  missingSmallWin$Measurement      <- NA                                                 #measurement is missing so NA
  
  #if first hr or last hr is na then fix is not possible, else continue with fix
  if (is.na(firsthr)||is.na(lasthr)){
    small_window_fixes[nrow(small_window_fixes)+1,] <- c(stationID,unlist(toString(missingSmallWin$Date)),unlist(toString(missingSmallWin$Time)),"No. One of the two needed data points is NA.") #note details of replacement
    return(list("y" = rep(NA,n-2),"swf" = small_window_fixes))
  }
  
  else{
    linear <- approx(x,y,method = "linear",n=n)  #linear interpolate
    linear_y <- linear$y[2:(length(linear$y)-1)] #grap y values for replacement (excluding the first and last data that was used for linear interpolation)
    linear_y <- sprintf("%.5f",linear_y)         #proper number of decimals
    
    small_window_fixes[nrow(small_window_fixes)+1,] <- c(stationID,unlist(toString(missingSmallWin$Date)),unlist(toString(missingSmallWin$Time)),"Yes.") #note details of replacement
    
    return(list("y" = linear_y,"swf" = small_window_fixes)) #return air quality data and data frame of where fixes were made
    }
  }

#===============================================================================================================================================
## fix_large_windows ##
fix_large_window = function(air_quality_data_ChemDateTime,positionnn,Sites_d_near,stationID,large_window_fixes,max_dist,run_type){
  
  #air_quality_data_ChemDateTime:
  #positionnn:
  #Sites_d_near:
  #stationID:
  #large_window_fixes:
  #max_dist:
  
  #print(stationID)
  
  d_nearbyStationsAll <- as.data.frame(Sites_d_near[,as.character(stationID),drop = F])                                 #filter for distances relevant to this station 
  d_nearbyStations <- as.data.frame(d_nearbyStationsAll[order(d_nearbyStationsAll[as.character(stationID)]),,drop=F])   #order in ascending order
  colnames(d_nearbyStations) <-  c("dist")                                                                              #rename column
  d_nearbyStations <- d_nearbyStations[d_nearbyStations$dist<=max_dist,,drop=F]                                         #filter for distances less than or equal to the max_dist specified (extra/redundant)
  
  write.csv(d_nearbyStations, file = file.path(getwd(),paste0("Output/QA/",run_type,"_",stationID,"_d_nearbyStations.csv"))) #for QA purposes
  
  missingLargeWin <- data.frame(matrix(unlist(positionnn),nrow=length(positionnn),byrow=T),check.names = F, stringsAsFactors = F) #dataframe for missing data
  colnames(missingLargeWin) <- c("UniquePosLoc")  #rename column
  
  len <- nchar(missingLargeWin$UniquePosLoc[1])   #length of unique position/location
  
  missingLargeWin$`Parameter name` <- substr(missingLargeWin$UniquePosLoc,1,len-23)      #slice chemical name
  missingLargeWin$Station          <- substr(missingLargeWin$UniquePosLoc,len-22,len-13) #slice station ID
  missingLargeWin$Date             <- substr(missingLargeWin$UniquePosLoc,len-12,len-5)  #slice Date
  missingLargeWin$Time             <- substr(missingLargeWin$UniquePosLoc,len-4,len)     #slice time
  missingLargeWin$Measurement      <- NA                                                 #measurement for missing data is NA
  
  #for each of the missing data, try to fill with data from the nearest station
  for(i in 1:nrow(missingLargeWin)){

    posi    <- missingLargeWin$UniquePosLoc[i]                #unique position/location for this missing data
    parameterName <- missingLargeWin$`Parameter name`[i]      #chemical name
    MisDate <- missingLargeWin$Date[i]                        #date of missing data
    MisTime <- missingLargeWin$Time[i]                        #time of missing data
    
    reached_end_of_stations <- "No"    #indicator to know when the last closest station has been reached
    found_some_data <- "No"            #indicator to know if a replacement data is found 
    
    j <- 2                             #counter for nearest station. This starts at location 2 because the station at location 1 is the same station with missing data  
    print(posi)
    while (reached_end_of_stations=="No" && found_some_data=="No"){  #while end of nearby stations is not reached and no data has been found
      
      nearStationPosi <- paste0(parameterName,rownames(d_nearbyStations)[j],MisDate,MisTime)                                               #compute the unique position/location of this nearby station
      nearStationData <- air_quality_data_ChemDateTime[air_quality_data_ChemDateTime$UniqueLocID == nearStationPosi, "Sample Measurement"] #filter original air quality data for data for this station at the specified date, chemical, and time using the unique loction identifier
      
      if((length(nearStationData)==0) || (is.na(nearStationData))){ #no data was found or NA was found then try next station
        j <- j + 1
      }
      
      else{ #if numerical data was found
        missingLargeWin[which(missingLargeWin$UniquePosLoc == posi ),"Measurement"] <- nearStationData #insert replacement data at the right location in the missing data dataframe
        found_some_data <- "Yes"  #we found data
        large_window_fixes[nrow(large_window_fixes)+1,] <- c(stationID,MisDate,MisTime,paste0("Yes. Fixed with data from ", rownames(d_nearbyStations)[j], " at distance ",d_nearbyStations$dist[j] ," meters away.")) #make note
      }
      
      if(j > nrow(d_nearbyStations)){      #if the end of the nearby stations is reached 
        reached_end_of_stations <- "Yes"   #reached end of nearby stations
        missingLargeWin[which(missingLargeWin$UniquePosLoc == posi ),"Measurement"] <- NA  #replacement for this missing data could not be found
        large_window_fixes[nrow(large_window_fixes)+1,] <- c(stationID,MisDate,MisTime,paste0("No. Did not find data in ", j-1," nearby stations within specified max_dist of ", max_dist, ". Stations looked at: ", unlist(toString(rownames(d_nearbyStations))))) #make note
      }
    }
  }

  return(list("mdf" = missingLargeWin,"lwf" = large_window_fixes)) #return the data frame of missing data and the notes made of fixes/lack thereof.
}

#===============================================================================================================================================
# fix data #

fix_data = function(air_quality_data_ChemDateTime,air_quality_data_Vector,Sites_d_near,small_window_fixes,large_window_fixes,thres,run_type){
  for (stationID in unique(air_quality_data_Vector$`Station ID`)){                            #for each unique station
    #browser()
    #print(stationID)
    #stationID <- "0603709033"
    StationData <- air_quality_data_Vector[air_quality_data_Vector$`Station ID`==stationID, ]
    print(paste0(StationData[1,6],"====================================================="))
    
    pos <- 2 #where to begin normal evalation (i.e. data not at the beginning or ends of the station data)
    lastloc <- nrow(StationData) #where to end normal evaluation.
    
    #if the first data is NA, then we cannot fix with linear interpolation. 
    if(is.na(StationData[1,5])){
      #while loop and append
      pos1 <- 1
      positionnn <- c() #vector to hold unique position ID of missing data
      
      while(is.na(StationData[pos1,5])){
        positionnn <- append(positionnn,StationData[pos1,6],length(positionnn)) #append unique position ID
        
        pos1 <- pos1 + 1 #evaluate the next row
      }
      
      missinghrsLenF <- length(positionnn) #missing data at the beginning of the station data
      pos <- missinghrsLenF + 1 #update starting location
      
      if(missinghrsLenF>thres){#if this is a large gap
        
        #browser()
        missingg <- fix_large_window(air_quality_data_ChemDateTime,positionnn,Sites_d_near,stationID,large_window_fixes,max_dist,run_type) #find data from nearby stations
        
        missingdf <- missingg$mdf           #missing dataframe
        missingdf <- missingdf$Measurement  #missinsg data replacement values
        large_window_fixes  <- missingg$lwf #large window fix notes
        
        StationData[which(StationData$`UniqueLocID` %in% positionnn),"Sample Measurement"] <- missingdf #fix stationdata with replacement values - crucial for while loop
        air_quality_data_Vector[which(air_quality_data_Vector$`UniqueLocID` %in% positionnn),"Sample Measurement"] <- missingdf #fix original air quality data with replacement values         
      }
      
      else{#this is a small fix gap, but cannot be fixed with a small fix function because the gap occurs at the beginning of the time span.
        
        missingSmallWin <- data.frame(matrix(unlist(positionnn),nrow=length(positionnn),byrow=T),check.names = F, stringsAsFactors = F) #dataframe for missing data
        colnames(missingLargeWin) <- c("UniquePosLoc")  #rename column
        len <- nchar(missingLargeWin$UniquePosLoc[1])   #length of unique position/location
        
        missingSmallWin$`Parameter name` <- substr(missingSmallWin$UniquePosLoc,1,len-23)      #slice chemical name
        missingSmallWin$Station          <- substr(missingSmallWin$UniquePosLoc,len-22,len-13) #slice station ID
        missingSmallWin$Date             <- substr(missingSmallWin$UniquePosLoc,len-12,len-5)  #slice Date
        missingSmallWin$Time             <- substr(missingSmallWin$UniquePosLoc,len-4,len)     #slice time
        missingSmallWin$Measurement      <- NA                                                 #measurement for missing data is NA
        
        small_window_fixes[nrow(small_window_fixes)+1,] <- c(stationID,unlist(toString(missingSmallWin$Date)),unlist(toString(missingSmallWin$Time)),"No. There is no preceeding data point to interpolate with.") #note details of replacement
        
      }
    }
    
    #if last data is NA, then we cannot fix with linear interpolation
    if(is.na(StationData[nrow(StationData),5])){
      
      #browser()
      
      #while loop and prepend
      pos1 <- nrow(StationData)
      positionnn <- c() #vector to hold dates of missing data
      
      while(is.na(StationData[pos1,5])){#find consecutive NAs
        positionnn <- append(positionnn,StationData[pos1,6],length(positionnn)) #append date
        
        pos1 <- pos1 - 1
        
      }
      
      positionnn <- rev(positionnn) #reverse order
      
      missinghrsLenL <- length(positionnn) #missing hours at the end of the station
      lastloc <- nrow(StationData) - missinghrsLenL #update the last record to look at for the normal evaluation 
        
      if(missinghrsLenL>thres){ #large fix
        
        #browser()
        missingg <- fix_large_window(air_quality_data_ChemDateTime,positionnn,Sites_d_near,stationID,large_window_fixes,max_dist,run_type) #find replacement data
        
        missingdf <- missingg$mdf #missing dataframe
        missingdf <- missingdf$Measurement #replacement data
        large_window_fixes  <- missingg$lwf #notes
        
        StationData[which(StationData$`UniqueLocID` %in% positionnn),"Sample Measurement"] <- missingdf #fix station data - crucial for while loop
        air_quality_data_Vector[which(air_quality_data_Vector$`UniqueLocID` %in% positionnn),"Sample Measurement"] <- missingdf #fix air quality data
      }
      
      else{#this is a small fix gap, but cannot be fixed with a small fix function because the gap occurs at the end of the time span.
        
        missingSmallWin <- data.frame(matrix(unlist(positionnn),nrow=length(positionnn),byrow=T),check.names = F, stringsAsFactors = F) #dataframe for missing data
        colnames(missingSmallWin) <- c("UniquePosLoc")  #rename column
        len <- nchar(missingSmallWin$UniquePosLoc[1])   #length of unique position/location
        
        missingSmallWin$`Parameter name` <- substr(missingSmallWin$UniquePosLoc,1,len-23)      #slice chemical name
        missingSmallWin$Station          <- substr(missingSmallWin$UniquePosLoc,len-22,len-13) #slice station ID
        missingSmallWin$Date             <- substr(missingSmallWin$UniquePosLoc,len-12,len-5)  #slice Date
        missingSmallWin$Time             <- substr(missingSmallWin$UniquePosLoc,len-4,len)     #slice time
        missingSmallWin$Measurement      <- NA                                                 #measurement for missing data is NA
        
        small_window_fixes[nrow(small_window_fixes)+1,] <- c(stationID,unlist(toString(missingSmallWin$Date)),unlist(toString(missingSmallWin$Time)),"No. There is no succeeding data point to interpolate with.") #note details of replacement
        
      }
      
    }
    
    #now that the ends have been fixed, any gaps can be filled with either linear interpolation or data from nearest station
    
    while(pos < lastloc){ #while effective end of stationData is not reached
      
      positionnn <- c() #vector to hold dates of missing data
      
      first <- StationData[pos-1,5] #data for hour before an NA
      
      
      while(is.na(StationData[pos,5])&&(pos < lastloc)){ #find consecutive NAs within boundaries
        
        positionnn <- append(positionnn,StationData[pos,6],length(positionnn)) #append date
        
        pos <- pos + 1 #evaluate the next row
      }
      
      last <- StationData[pos,5] #data for hour after an NA
      
      #if missing data was found
      if(length(positionnn) > 0){
        
        missinghrsLen <- length(positionnn) #length of missing data
        
        #is the missinghrsLen above or below the set threshold for small gaps (fixed with linear interpolation) or large gaps (fixed with next nearest monitor)
        
        #small fix - fix with linear interpolation
        if (missinghrsLen <= thres) { 
          
          small_window_fix <- fix_small_window(first,last,missinghrsLen,stationID,positionnn,small_window_fixes) #small fix
          small_window_fixes <- small_window_fix$swf #notes
          #browser()
          StationData[which(StationData$`UniqueLocID` %in% positionnn),"Sample Measurement"] <- small_window_fix$y                         #fix missing data in station data - crucial for while loop
          air_quality_data_Vector[which(air_quality_data_Vector$`UniqueLocID` %in% positionnn),"Sample Measurement"] <- small_window_fix$y #fix missing data in air quality data
          
        }
        
        #large fix - fix with data from next nearest monitor
        else{
          
          #browser()
          missingg <- fix_large_window(air_quality_data_ChemDateTime,positionnn,Sites_d_near,stationID,large_window_fixes,max_dist,run_type) #large fix
          missingdf <- missingg$mdf #large fix dataframe
          missingdf <- missingdf$Measurement #large fix replacement data
          large_window_fixes  <- missingg$lwf #notes
          
          StationData[which(StationData$`UniqueLocID` %in% positionnn),"Sample Measurement"] <- missingdf                         #fix missing data in station data - crucial for while loop
          air_quality_data_Vector[which(air_quality_data_Vector$`UniqueLocID` %in% positionnn),"Sample Measurement"] <- missingdf #fix missing data in air quality data
          
        }
        #browser()
      }
      
      pos <- pos + 1#evaluate the "next" row (if there were NAs, then this will be the next row after the first non-NA row)
      
    }
    
    
  }
  return(list("air quality" = air_quality_data_Vector, "lwdf" = large_window_fixes, "smdf" = small_window_fixes))
}

