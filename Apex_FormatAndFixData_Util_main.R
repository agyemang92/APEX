## APEX Format and Fix Data - Main ##
## 07/12/2019 - ICF ##
rm(list=ls())

source(file.path(getwd(),"Apex_FixData.R"))
require(BBmisc)
require(rlang)
#replace grep with gregexpr
before<-Sys.time()

#################### User inputs start ####################
## Set time period of interest ##
date_start           <- "2010-01-01"   #YYYY-MM-DD. Local.
date_end             <- "2010-01-15"   #YYYY-MM-DD. Local.

time_start           <- "00:00"        #HH:00. Local 24-hr. 
time_end             <- "24:00"        #HH:00. Local 24-hr.

## Set FIPS code ##

FIPS                 <- c("06037","06059", '06065', '06071', '06073', '06111') #XXXXX - five digits

## chemical(s) ##
chem                 <- c("Ozone")     #Chemical names

## Set file directories ##
hrly_data_loc        <- file.path(getwd(),"hourly_44201_2010.csv") #location of hourly data
site_data_loc        <- file.path(getwd(),"aqs_sites.csv")         #location of site description data on disk
monitor_data_loc     <- file.path(getwd(),"aqs_monitors.csv")      #location of monitor description data on disk
countyFIPS_data_loc  <- file.path(getwd(),"FIPS_County.csv")       #location of county FIPS code data

## output file location ##
dir.create(file.path(getwd(),"Output"))                        #create output directory
ad                  <- "air_district_2010.txt"                 #air district file name
aq                  <- "air_quality_2010.txt"                  #air quality file name
ad_ofpn             <- file.path(getwd(),"Output",ad)          #air district output file path name
aq_ofpn             <- file.path(getwd(),"Output",aq)          #air quality output file path name

frac                <- 0.2   #acceptable fraction of missing data/Output (a fraction from 0 - 1)
max_dist            <- 30739.303 #maximum distance (meters) of nearby station(s) to be used.
thres               <- 3 #range from 0 - 22 (because at least 2 points are needed for linear interpolation) inclusive #number of consecutive missing hourly data at or below which linear interpolation will be used, and beyond which data from the next nearest station will be used

#################### User inputs end ####################

#===============================================================================================================================================================================================================================================
## Prepare data for fixing ##
#format data
y <- Format_data(date_start,date_end,time_start,time_end,FIPS,chem,hrly_data_loc,site_data_loc,monitor_data_loc,
                 countyFIPS_data_loc,ad,aq,ad_ofpn,aq_ofpn)               #returns formatted and filtered air quality data and air districts data
#assign data
#air_quality_data <- read.csv(file.path(getwd(),"aq_test.csv"),stringsAsFactors = F, check.names = F)
#air_quality_data$`Station ID` <- "0603700002"
#colnames(air_quality_data) <- c("00:00","01:00","02:00","03:00","04:00","05:00","06:00","07:00","08:00","09:00","10:00","11:00",
#                                "12:00","13:00","14:00","15:00","16:00","17:00","18:00","19:00","20:00","21:00","22:00","23:00","Date","Station ID")
air_quality_data   <- y$`Air Quality Data`                                      #get formatted air quality data
air_quality_data_ChemDateTime <- y$`Air Quality Data by ChemDateTime`

#air_quality_data_ChemDateTime <- read.csv("C:/Users/39492/Desktop/APEX Utility/missing data/aqd_chemDateTime.csv",stringsAsFactors = F, check.names = F)

##sprintf("%02d",hrly_data_by_chem_date_time_county$`State Code`)            # fix state code to 2 characters
##air_quality_data_ChemDateTime$`Station ID` <- as.character(air_quality_data_ChemDateTime$`Station ID`)
#air_quality_data_ChemDateTime$`Station ID` <- sprintf("%010d",air_quality_data_ChemDateTime$`Station ID`)
#air_quality_data_ChemDateTime$`Time Local` <- sprintf("%05s",air_quality_data_ChemDateTime$`Time Local`)
#air_quality_data_ChemDateTime$`Time Local` <- gsub(" ",0,air_quality_data_ChemDateTime$`Time Local`)

air_districts_data <- y$`Air Districts Data`                                    #get formatted air district data
air_districts_data <- air_districts_data[air_districts_data$`Start Date`!="",]  #only return rows with full data
monitor_data       <- y$`Monitor Data`                                          #get formatted monitor data
site_data          <- y$`Site Data`                                             #get formatted site data

#fill in missing days
missing_days_filled <- fix_missing_days(air_quality_data,date_start,date_end) #fix missing days
air_quality_data <- missing_days_filled$aqd #air quality data with missing days fixed
MissingDays <- missing_days_filled$md #log

#write data before fixes
write.csv(air_quality_data,"C:/Users/39492/Desktop/APEX Utility/missing data/Output/before.csv") #before edits

## Calculate fraction of missing data ##
Missing_data   <- sum(is.na(air_quality_data))                                  #count all NAs in the air quality data frame
Available_data <- sum(!is.na(air_quality_data))                                 #count all non NA's in the air quality data frame
All_data_I     <- Missing_data + Available_data                                 #number of cells
All_data_II    <- nrow(air_quality_data) * ncol(air_quality_data)               #number of cells
#we can include a check here to make sure All_data_I == All_data_II
Frac_missing   <- signif(Missing_data/All_data_I,4)                             #compute fraction of missing data

#check if fraction of missind data is less than set threshold 
if (Frac_missing > frac){ #if there are more missing data than the threshold
  #sink() #use to write message to file
  print(paste0("Fraction of missing data ","(",Frac_missing,")"," is greater than the acceptable fraction of missing data ","(",frac,"). ","\n",
               "Hourly data cannot be used with current specifications."))
  break   #no need to proceed
}
#===============================================================================================================================================================================================================================================

#function to perform fixes
fix_data = function(air_quality_data,small_window_fixes,large_window_fixes){
  for (stationID in unique(air_quality_data$`Station ID`)){                            #for each unique station
    #print(stationID)
    for (Date in unique(air_quality_data$Date)){                                       #for each unique date
      
      #print(Date)
      
      #stationID <- "0607101234" #0603700002, 0603700002, 0603700002
      #Date      <- "20100107"  #20100101, 20100102, 20100103
      #browser()
      #browser()
      StationDateData <- air_quality_data[air_quality_data$`Station ID`==stationID & air_quality_data$Date==Date,] #filter air quality data by the current station and date (must be 1 row of data)
      HourlyData      <- StationDateData[,1:24] #filter for just hourly data
      
      columns         <- colnames(HourlyData) #column names (hours)
      
      MissingData     <- HourlyData[,is.na(HourlyData)] #filter for NAs
      Missinglen      <- length(MissingData) #number of hours with missing data
      #browser()  
      ##### Main Block - if there are some missing data in this row of data for this station ID and date, then proceed. Otherwise skip and move to next date for current station ID ########
      if (Missinglen > 0){ 
        #browser()#for debugging
        
        ####### Block 1 - If all hours are missing data ##########
        if (Missinglen == 24){ #if all hours are missing data
          #perform a large fix
          #browser()  
          dist_to_other_stations <- find_dist_to_other_stations(stationID,max_dist,site_data) #calculate and sort in ascending order the distances from the current station ID to all 
          #other stations listed in site data that are within stated maximum distance
          
          missin <- HourlyData
          
          #browser()
          #large fix
          missingg <- fix_large_window(missin,dist_to_other_stations,stationID,Date,large_window_fixes)
          
          missingdf <- missingg$mdf
          large_window_fixes  <- missingg$lwf
          
          #at this point any data that can be replaced has been replaced in missingdf (any NAs mean that data was not found in any of the nearby statioins within the specified max_dist).
          #so perform real replacement in the air_quality_data table
          
          #browser()
          air_quality_data[which(air_quality_data$Date==Date & air_quality_data$`Station ID`==stationID),colnames(missingdf)] <- as.vector(missingdf) #perform data replacement for relevant columns
          HourlyData[,colnames(missingdf)] <- as.vector(missingdf) #perform data replacement for relevant columns in hourlyData
          
          
        }##Block 1 End###
        
        ####### Block 2 - Else if there is just one hour not missing data (can only do a large fix) ##########
        else if (Missinglen == 23) { 
          
          ###Block 2a - if this available hour is between 00:00 - 23:00 exclusive####
          if (is.na(HourlyData[1]) && is.na(HourlyData[24])) { #the only available data is somewhere in the middle on the clock
            
            singlehr <- columns[grep(HourlyData[,!is.na(HourlyData)],HourlyData)[1]]
            lasthr  <- columns[grep(singlehr,columns)[1]-1]
            firsthr   <- columns[grep(singlehr,columns)[1]+1]
            
            missinghrs <- columns[!(columns %in% singlehr)]
            
            #perform large fix
            
          }
          
          ###Block 2b - else the only available data is at one of the two ends of the clock
          else{ 
            
            pos <- 1 #position tracker for hours 0 - 23
            pos2 <- 1 #second position tracker for finding consecutive NAs     
            
            while(pos <= 24 && pos >= 1){ #while within the 0-23 hr boundary
              #browser()
              missinghrs <- c() #vector to hold column names of hours with missing data
              
              while(is.na(HourlyData[pos2])&&!is.error(HourlyData[pos2])){ #while the current hour's data is missing...
                #browser()  
                missinghrs <- append(missinghrs,columns[pos2],length(missinghrs)) #append the name of the column of this missing hour
                
                pos2 <- pos2 + 1 #evaluate the next position
                pos <- pos2      #update pos to current position
                
                if(pos2 > 24){ #pos2 reaches > 24
                  
                  break #to avoid error in entering while loop when pos2 > 24 eg when data for all hours is NA #try adding this condition to the while loop statement
                }
              }
              
              #if (length(missinghrs)>0) {
              
              #onlyhr <-  columns[grep(missinghrs[length(missinghrs)],columns)[1]+1] #the only hr with data
              
              missinghrsLen <- length(missinghrs) #length of missing consecutive hourly data
              
              #if(missinghrsLen > 0){ #if there are any missing consecutive hourly data then these need to be fixed...
              
              #is the missinghrsLen above or below the set threshold for small gaps (fixed with linear interpolation) or large gaps (fixed with next nearest monitor)
              #small fix - fix with linear interpolation
              #if (missinghrsLen <= thres) { 
              
              #  n <- missinghrsLen + 2 #number of data points (missing data + 2 available data and the endpoints)
              #  small_window_fix <- fix_small_window(HourlyData,firsthr,lasthr,n) #find linear interpolation data points
              #browser()
              #  air_quality_data[which(air_quality_data$Date==Date & air_quality_data$`Station ID`==stationID),missinghrs] <- small_window_fix[2:(length(small_window_fix)-1)] #perform data replacement for relevant columns
              #  small_window_fixes[nrow(small_window_fixes)+1,] <- c(Date,stationID,unlist(toString(missinghrs))) #note details of replacement
              
              
              #}
              #large fix - fix with data from next nearest monitor
              #else{ 
              
              dist_to_other_stations <- find_dist_to_other_stations(stationID,max_dist,site_data) #calculate and sort in ascending order the distances from the current station ID to all 
              #other stations listed in site data that are within stated maximum distance
              
              missin <- HourlyData[,missinghrs]
              
              #browser()
              #large fix
              missingg <- fix_large_window(missin,dist_to_other_stations,stationID,Date,large_window_fixes)
              
              missingdf <- missingg$mdf
              large_window_fixes  <- missingg$lwf
              
              #at this point any data that can be replaced has been replaced in missingdf (any NAs mean that data was not found in any of the nearby statioins within the specified max_dist).
              #so perform real replacement in the air_quality_data table
              
              #browser()
              air_quality_data[which(air_quality_data$Date==Date & air_quality_data$`Station ID`==stationID),colnames(missingdf)] <- as.vector(missingdf) #perform data replacement for relevant columns
              HourlyData[,colnames(missingdf)] <- as.vector(missingdf) #perform data replacement for relevant columns in hourlyData
              
              
              #}
              #}
              #}
              pos <- pos + 1#length(missinghrs) #evaluate the position after these consecutive NAs
              pos2 <- pos #update pos2 to current position
            }
            
            
          }#End of Block 2b
          
        }#End of Block 2
        
        ####### Block 3 - Else there is at least 2 hours of available data, in which case a large fix or a small fix (requiring two points for linear interpolation) can be performed ##########
        else { 
          
          #### Block 3a - first handle the replacement of data at the ends of clock at hours 0 and 23 (since the clock restarts at hour 0 after hour 23)
          if (is.na(HourlyData[1]) && is.na(HourlyData[24])){                 #if the ends of the clock are both NA
            #browser()
            missinghrs <- c("23:00")
            
            poss2 <- 23 #start from 22:00 the hour before 23:00
            
            while (is.na(HourlyData[poss2])) {#is the hour missing data
              #browser()
              missinghrs <- prepend(missinghrs,columns[poss2],1) #append name of hour missing data
              poss2 <- poss2 - 1 #evaluate the previous hour
            }
            
            poss  <- 1 #start from hour 01:00
            
            while (is.na(HourlyData[poss])) {#(poss <= 24 && poss2 >= 1){ #is the hour missing data
              #browser()
              missinghrs <- append(missinghrs,columns[poss],length(missinghrs)) #if missing data, append name of hour to missinghrs
              poss <- poss + 1 #evaluate the next hour
              #no need for an if statement to break if poss is beyond 24, because is.na() will break the loop when data is found, which must be found because there is at least one hour with data
              
            }
            
            missinghrsLen <- length(missinghrs)
            
            firsthr <- columns[poss2]                    #first hr before start of consecutive NAs
            lasthr  <- columns[poss]   #last hr after end of consecutive NAs
            
            #small fix - fix with linear interpolation
            if (missinghrsLen <= thres) { 
              
              n <- missinghrsLen + 2 #number of data points (missing data + 2 available data and the endpoints)
              small_window <- fix_small_window(HourlyData,firsthr,lasthr,n,stationID,Date,small_window_fixes,missinghrs,missinghrs) #find linear interpolation data points
              small_window_fix <- small_window$y
              small_window_fixes <- small_window$swf
              
              small_window_fix <- small_window_fix[2:(length(small_window_fix)-1)] #ignore data at the ends
              small_window_fix <- rev(small_window_fix) #reverse order to match order of columns whose data is to be replaced
              
              #browser()
              air_quality_data[which(air_quality_data$Date==Date & air_quality_data$`Station ID`==stationID),missinghrs] <- small_window_fix #perform data replacement for relevant columns
              HourlyData[,missinghrs] <- small_window_fix #perform data replacement for relevant columns in hourlyData
              
              #small_window_fixes[nrow(small_window_fixes)+1,] <- c(Date,stationID,unlist(toString(missinghrs))) #note details of replacement
              
            }
            
            #large fix - fix with data from next nearest monitor
            else{ 
              
              #browser()
              
              dist_to_other_stations <- find_dist_to_other_stations(stationID,max_dist,site_data) #calculate and sort in ascending order the distances from the current station ID to all 
              #other stations listed in site data that are within stated maximum distance
              
              missin <- HourlyData[,missinghrs]
              
              #browser()
              #large fix
              missingg <- fix_large_window(missin,dist_to_other_stations,stationID,Date,large_window_fixes)
              
              missingdf <- missingg$mdf
              large_window_fixes  <- missingg$lwf
              
              #at this point any data that can be replaced has been replaced in missingdf (any NAs mean that data was not found in any of the nearby statioins within the specified max_dist).
              #so perform real replacement in the air_quality_data table
              
              #browser()
              air_quality_data[which(air_quality_data$Date==Date & air_quality_data$`Station ID`==stationID),colnames(missingdf)] <- as.vector(missingdf) #perform data replacement for relevant columns
              HourlyData[,colnames(missingdf)] <- as.vector(missingdf) #perform data replacement for relevant columns in hourlyData
              
            }
            
            
            
          }
          
          #### Block 3b - if hour 1 is NA but hour 24 is not NA ####
          else if (is.na(HourlyData[1]) && !is.na(HourlyData[24])) {        
            
            missinghrs <- c("00:00") #we know hour 0:00 is missing data
            #availablehrs <- c() #vector to hold the first and last hours with available data
            
            poss <- 2 #start from hour 01:00 
            
            while (is.na(HourlyData[poss])) {
              #browser()
              missinghrs <- append(missinghrs,columns[poss],length(missinghrs)) #if missing data, append name of hour to missinghrs
              poss <- poss + 1
              
            }
            
            
            #availablehrs <- append(availablehrs,columns[poss],length(availablehrs)) #the first hour with data 
            
            #availablehrs <- append(availablehrs,"23:00",length(availablehrs)) #the last hour with data because we know hour 23:00 has data
            #browser()
            missinghrsLen <- length(missinghrs)
            
            firsthr <- columns[24]                    #first hr before start of consecutive NAs
            lasthr  <- columns[poss]   #last hr after end of consecutive NAs
            
            #small fix - fix with linear interpolation
            if (missinghrsLen <= thres) { 
              
              n <- missinghrsLen + 2 #number of data points (missing data + 2 available data and the endpoints)
              small_window <- fix_small_window(HourlyData,firsthr,lasthr,n,stationID,Date,small_window_fixes,missinghrs) #find linear interpolation data points
              small_window_fix <- small_window$y
              small_window_fixes <- small_window$swf
              
              
              small_window_fix <- small_window_fix[2:(length(small_window_fix)-1)] #ignore data at the ends
              small_window_fix <- rev(small_window_fix)
              
              #browser()
              air_quality_data[which(air_quality_data$Date==Date & air_quality_data$`Station ID`==stationID),missinghrs] <- small_window_fix #perform data replacement for relevant columns
              HourlyData[,missinghrs] <- small_window_fix #perform data replacement for relevant columns in hourlyData
              
              #small_window_fixes[nrow(small_window_fixes)+1,] <- c(Date,stationID,unlist(toString(missinghrs))) #note details of replacement
              
            }
            
            #large fix - fix with data from next nearest monitor
            else{ 
              
              #browser()
              
              dist_to_other_stations <- find_dist_to_other_stations(stationID,max_dist,site_data) #calculate and sort in ascending order the distances from the current station ID to all 
              #other stations listed in site data that are within stated maximum distance
              
              missin <- HourlyData[,missinghrs]
              
              #browser()
              #large fix
              missingg <- fix_large_window(missin,dist_to_other_stations,stationID,Date,large_window_fixes)
              
              missingdf <- missingg$mdf
              large_window_fixes  <- missingg$lwf
              
              #at this point any data that can be replaced has been replaced in missingdf (any NAs mean that data was not found in any of the nearby statioins within the specified max_dist).
              #so perform real replacement in the air_quality_data table
              
              #browser()
              air_quality_data[which(air_quality_data$Date==Date & air_quality_data$`Station ID`==stationID),colnames(missingdf)] <- as.vector(missingdf) #perform data replacement for relevant columns
              HourlyData[,colnames(missingdf)] <- as.vector(missingdf) #perform data replacement for relevant columns in hourlyData
              
            }
            
            
          } 
          
          #### Block 3c - if hour 1 is not NA but hour 24 is NA ####
          else if (!is.na(HourlyData[1]) && is.na(HourlyData[24])) {        
            
            #browser()
            
            missinghrs <- c("23:00") #we know hour 23:00 is missing data
            #availablehrs <- c() #vector to hold the first and last hours with available data
            
            #availablehrs <- append(availablehrs,"01:00",length(availablehrs)) #the first hour with data because we know hour 01:00 has data
            
            poss <- 23 #start from hour 22:00 
            
            while (is.na(HourlyData[poss])) {
              #browser()
              missinghrs <- prepend(missinghrs,columns[poss],1) #if missing data, append name of hour to missinghrs
              poss <- poss - 1
              
            }        
            #availablehrs <- append(availablehrs,columns[poss],length(availablehrs)) #the last hour with data 
            missinghrsLen <- length(missinghrs)
            
            firsthr <- columns[poss]                    #first hr before start of consecutive NAs
            lasthr  <- columns[1]   #last hr after end of consecutive NAs
            
            #small fix - fix with linear interpolation
            if (missinghrsLen <= thres) { 
              
              n <- missinghrsLen + 2 #number of data points (missing data + 2 available data and the endpoints)
              small_window <- fix_small_window(HourlyData,firsthr,lasthr,n,stationID,Date,small_window_fixes,missinghrs) #find linear interpolation data points
              small_window_fix <- small_window$y
              small_window_fixes <- small_window$swf
              
              
              small_window_fix <- small_window_fix[2:(length(small_window_fix)-1)] #ignore data at the ends
              small_window_fix <- rev(small_window_fix)
              
              #browser()
              air_quality_data[which(air_quality_data$Date==Date & air_quality_data$`Station ID`==stationID),missinghrs] <- small_window_fix #perform data replacement for relevant columns
              HourlyData[,missinghrs] <- small_window_fix #perform data replacement for relevant columns in hourlyData
              
              #small_window_fixes[nrow(small_window_fixes)+1,] <- c(Date,stationID,unlist(toString(missinghrs))) #note details of replacement
              
            }
            
            #large fix - fix with data from next nearest monitor
            else{ 
              
              #browser()
              
              dist_to_other_stations <- find_dist_to_other_stations(stationID,max_dist,site_data) #calculate and sort in ascending order the distances from the current station ID to all 
              #other stations listed in site data that are within stated maximum distance
              
              missin <- HourlyData[,missinghrs]
              
              #browser()
              #large fix
              missingg <- fix_large_window(missin,dist_to_other_stations,stationID,Date,large_window_fixes)
              
              missingdf <- missingg$mdf
              large_window_fixes  <- missingg$lwf
              
              #at this point any data that can be replaced has been replaced in missingdf (any NAs mean that data was not found in any of the nearby statioins within the specified max_dist).
              #so perform real replacement in the air_quality_data table
              
              #browser()
              air_quality_data[which(air_quality_data$Date==Date & air_quality_data$`Station ID`==stationID),colnames(missingdf)] <- as.vector(missingdf) #perform data replacement for relevant columns
              HourlyData[,colnames(missingdf)] <- as.vector(missingdf) #perform data replacement for relevant columns in hourlyData
              
            }
            
            
            
          }
          
          #### Block 3d - now that ends of the clock have been handled, handle hours 1:00 - 22:00 only ####
          pos  <- 2 #position tracker for hours 0 - 23
          pos2 <- 2 #second position tracker for finding consecutive NAs     
          
          #### Block 3di - while within the 0-23 hr boundary
          while(pos <= 23 && pos >= 2){ 
            
            #browser()
            missinghrs <- c() #vector to hold column names of hours with missing data
            
            #### Block 3di1 - while the current hour's data is missing, append consecutive missing hours
            #maybe take is.error() away
            while(is.na(HourlyData[pos2])&&!is.error(HourlyData[pos2])){ 
              #browser()  
              missinghrs <- append(missinghrs,columns[pos2],length(missinghrs)) #append the name of the column of this missing hour
              
              #browser()  
              pos2 <- pos2 + 1 #evaluate the next position
              pos <- pos2      #update pos to current position
              
              #if pos2 reaches > 24
              if(pos2 > 24){break} #to avoid error in entering while loop when pos2 > 24 eg when data for all hours is NA #try adding this condition to the while loop statement 
              
            }
            
            #### Block 3di2 - if missing data is found
            if(length(missinghrs)>0){
              
              firsthr <- columns[grep(missinghrs[1],columns)[1]-1]                    #first hr before start of consecutive NAs
              lasthr  <- columns[grep(missinghrs[length(missinghrs)],columns)[1]+1]   #last hr after end of consecutive NAs
              missinghrsLen <- length(missinghrs) #length of missing consecutive hourly data
              
              #### Block 3di2a - if there are any missing consecutive hourly data then these need to be fixed...
              if(missinghrsLen > 0){
                
                #is the missinghrsLen above or below the set threshold for small gaps (fixed with linear interpolation) or large gaps (fixed with next nearest monitor)
                #small fix - fix with linear interpolation
                if (missinghrsLen <= thres) { 
                  #browser()
                  n <- missinghrsLen + 2 #number of data points (missing data + 2 available data and the endpoints)
                  small_window <- fix_small_window(HourlyData,firsthr,lasthr,n,stationID,Date,small_window_fixes,missinghrs) #find linear interpolation data points
                  small_window_fix <- small_window$y
                  small_window_fixes <- small_window$swf
                  
                  
                  #browser()
                  air_quality_data[which(air_quality_data$Date==Date & air_quality_data$`Station ID`==stationID),missinghrs] <- small_window_fix[2:(length(small_window_fix)-1)] #perform data replacement for relevant columns
                  HourlyData[,missinghrs] <- small_window_fix[2:(length(small_window_fix)-1)] #perform data replacement for relevant columns in hourlyData
                  
                  #small_window_fixes[nrow(small_window_fixes)+1,] <- c(Date,stationID,unlist(toString(missinghrs))) #note details of replacement
                  
                }
                
                #large fix - fix with data from next nearest monitor
                else{ 
                  
                  
                  dist_to_other_stations <- find_dist_to_other_stations(stationID,max_dist,site_data) #calculate and sort in ascending order the distances from the current station ID to all 
                  #other stations listed in site data that are within stated maximum distance
                  
                  #browser()
                  
                  missin <- as.data.frame(HourlyData[,missinghrs])
                  
                  #browser()
                  #large fix
                  missingg <- fix_large_window(missin,dist_to_other_stations,stationID,Date,large_window_fixes)
                  
                  missingdf <- missingg$mdf
                  large_window_fixes  <- missingg$lwf
                  
                  #at this point any data that can be replaced has been replaced in missingdf (any NAs mean that data was not found in any of the nearby statioins within the specified max_dist).
                  #so perform real replacement in the air_quality_data table
                  
                  #browser()
                  air_quality_data[which(air_quality_data$Date==Date & air_quality_data$`Station ID`==stationID),colnames(missingdf)] <- as.vector(missingdf) #perform data replacement for relevant columns
                  HourlyData[,colnames(missingdf)] <- as.vector(missingdf) #perform data replacement for relevant columns in hourlyData
                  
                }
              }
              
            }
            #browser()
            pos <- pos + 1#length(missinghrs) #evaluate the position after these consecutive NAs
            pos2 <- pos #update pos2 to current position
            
          }
        }###Block 3 End## 
      }
      ##### Main Block End ########
    }
    ##### Evaluate next date ########
  }
  ##### Evaluate next stationID ########
  return(list("aqd" = air_quality_data, "lrgfixdf" = large_window_fixes, "smlfixdf" = small_window_fixes))
}

#===============================================================================================================================================

small_window_fixes <- data.frame("Date" = character(),"Station ID" = character(),"Hours Missing Data" = character(),"Fixed?" = character(), stringsAsFactors = F, check.names = F) #keep track of where replacements are made
large_window_fixes <- data.frame("Date" = character(),"Station ID" = character(),"Hours Missing Data" = character(),"Fixed?" = character(), stringsAsFactors = F, check.names = F) #dataframe to keep track of where data is replaced

## Perform fixes ##
yg <- fix_data(air_quality_data,small_window_fixes,large_window_fixes)

air_quality_data <- yg$aqd
large_window_fixes <- yg$lrgfixdf
small_window_fixes <- yg$smlfixdf

###maybe include a check that if (sum(is.na(air_quality_data))>0) then run script a second time to fix any small gaps?? Any remaining NAs after that are NAs that cannot be fixed because ###
### they are not small enough to be fixed with linear interpolation (or do not have the two endpoints needed for linear interpolation) or with nearby stations within stated max_dist because those ###
### stations themselves do not have the data ###


#===============================================================================================================================================
#write files#

# write air district data to file #
write.table(air_districts_data, ad_ofpn, append = TRUE, sep = "   ", dec = ".", row.names = FALSE, col.names = FALSE,quote = F)#,na="0.00000")
##Note: we can also create air_district_data in the nested loop above using the hrly data table. I did it this way as a form of a QA.

## write air quality data to text file ##
for (k in unique(air_quality_data$`Station ID`)){                                    #for each station ID
  air_quality_data_sub <- air_quality_data[air_quality_data$`Station ID`== k,]       #filter by current station ID
  air_quality_data_sub <- air_quality_data_sub[,c(1:25)]                             #need only the first 25 columns
  write(paste0("Name =",k),aq_ofpn,append = T)                                       #print out name
  write.table(air_quality_data_sub, aq_ofpn, append = TRUE, sep = " ", dec = ".", row.names = FALSE, col.names = FALSE,quote = F)
}

write.csv(air_quality_data,"C:/Users/39492/Desktop/APEX Utility/missing data/Output/after.csv")

write.csv(large_window_fixes,"C:/Users/39492/Desktop/APEX Utility/missing data/Output/large_window_fixes.csv")

write.csv(small_window_fixes,"C:/Users/39492/Desktop/APEX Utility/missing data/Output/small_window_fixes.csv")


after<-Sys.time()
after-before