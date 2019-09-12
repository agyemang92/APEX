## Preprocess Data ##
## Completed 08/20/2019 by ICF ##
## This script contains 1 function (preprocess_data) that processes the raw data - filtering for relevant time period, chemical, and location, filling in missing days, making sure all hours are available, etc ##
 
preprocess_data = function(date_start,date_end,time_start,time_end,FIPS,frac,chem,hrly_data_loc,site_data_loc,max_dist,decimals){

  ## Error handling start ##
  
  #try-catch blocks to:
  #check that date_end > date_start
  if(date_end <= date_start) {
    stop("Please enter an end date that is after your start date.")
  }
  if (substr(date_end,1,4)!=substr(date_start,1,4)) {
    stop("Please ensure start and end dates are in same year.")
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
  if(max_dist<10000) {
    warning("Please note that the MaxD should be provided in meters. The distance you provided is less than 10,000m (10km), which may result in missing values unable to be fixed.")
  }
  
  ## Error handling end ##

  #=========================================================================================================================================================================================================================================
  ## Script to prepare sites based on distance to study area ##
  ## Graham Glen, ICF, July 2019 ##
  sites <- as.data.table(fread(site_data_loc,check.names = FALSE, stringsAsFactors = FALSE))  # read in site description data
  sites <- sites[sites$`State Code`>="01" & sites$`State Code`<="56",]                        # Restrict to 50 states + DC
  sites <- sites[sites$Latitude!=0 & sites$Longitude!=0,]                                     # Sites must have lat and long
  sites$FIPS <- substr(100000+1000*as.numeric(sites$`State Code`)+sites$`County Code`,2,6)    # Create FIPS for all sites
  study_FIPS <- FIPS                                                                          # user provided codes

  found <- FALSE
  for (i in 1:length(FIPS)) {
     if((FIPS[i] %in% sites$FIPS)==TRUE) found<- TRUE
  }
  if (!found) stop("No sites exist for the given FIPS code(s). Please try a different FIPS code.")

  sites <- sites[!is.na(sites$`Site Number`),]                                                    # remove all NAs
  sites$`Station ID` <- substr(10000000000+100000*as.numeric(sites$FIPS)+sites$`Site Num`,2,11)   # concatenate FIPS+Site IDs

  study <- sites[sites$FIPS %in% study_FIPS,]  #filter for sites with study FIPS
  study$`Station ID` <- substr(10000000000+100000*as.numeric(study$FIPS)+study$`Site Num`,2,11)   # concatenate FIPS+Site IDs

  # create matrix of distances
  d <- as.data.table(distm(cbind(sites$Longitude,sites$Latitude),cbind(study$Longitude,study$Latitude)),stringsAsFactors=F,check.names=F)   
  colnames(d) <- as.character(study$`Station ID`) 
  cutoff      <- min(200000,2*max_dist)             #  maximum distance of area that will be considered. No more than 300km 
  d_min       <- as.list(lapply(transpose(d),min))
  sites_near  <<- sites[d_min<=cutoff]
  site_names  <- sites_near$`Station ID`
  d_near      <- d[d_min<=cutoff]

  rm(sites, d, d_min)  # clean the full list of sites away to free up RAM
  gc()                 # remove the full list of sites from memory
  #=========================================================================================================================================================================================================================================
  ## prepare user chemical data ##
  ## transform user chemical data vector list into the prepared datatable ##
  chemical_data           <- data.table(matrix(unlist(chem),nrow = length(chem),byrow = T),check.names = F, stringsAsFactors = F) #set up data table
  colnames(chemical_data) <- c("Chemical")                                                                                        #rename column
  #=========================================================================================================================================================================================================================================
  
  # Read all the hourly data from the specified unzipped file
  cat( "Reading hourly data for whole country\n")
  hrly_data   <- fread(hrly_data_loc,check.names = FALSE, stringsAsFactors = FALSE)
  
  if(is.na(hrly_data[1,2])==TRUE) {
    stop("The data chosen has 0 rows. Please choose a file with data. This information is provided under each file on the AQS website")
  }
  #==========================================================================================================================================================================================================================================
  ## filter hourly data further ##
  cat("Reducing hourly data to sites near",study_area,"\n")
  hrly_data$`Station ID` <- substr(10000000000+100000000*hrly_data$`State Code`+100000*hrly_data$`County Code`+hrly_data$`Site Num`,2,11)     
  hrly_data_nearby  <- hrly_data[hrly_data$`Station ID` %in% sites_near$`Station ID`,]    # hourly data at core sites and nearby stations.
  
  rm(hrly_data)   # clean the full list of hourly data away to free up RAM
  gc()            # remove the full list of hourly data from memory

  hrly_data_by_chem_nearby   <- inner_join(hrly_data_nearby, chemical_data, by = c("Parameter Name" = "Chemical"))      #inner join by chemical 
  data_unit <<- unique(hrly_data_by_chem_nearby$`Units of Measure`)
  rm(hrly_data_nearby)
  gc()

  ## Query data by date and time ranges and select only relevant columns and average sample measurement at multiple monitors at the same site ##
  ## Build Query string ##

  QRYstr <- paste0("SELECT `State Code`, `County Code`, `Site Num`, `Parameter Code`, `Latitude`, `Longitude`, `Parameter Name`, 
                   `Date Local`, `Time Local`, Avg(`Sample Measurement`) AS `Sample Measurement`, `State Name`, `County Name`, `Station ID` ",
                   "FROM hrly_data_by_chem_nearby ",
                   "GROUP BY `State Code`, `County Code`, `Site Num`, `Parameter Code`, `Latitude`, `Longitude`, `Parameter Name`, 
                   `Date Local`, `Time Local`, `State Name`, `County Name`, `Station ID` ",
                   "HAVING (`Date Local` between '",date_start,"' and '",date_end,
                   "' and `Time Local` between '",time_start,"' and '",time_end,"')")

  ## Run query ##
  hrly_data_by_chem_date_time_nearby <- sqldf(QRYstr, stringsAsFactors = FALSE)

  rm(hrly_data_by_chem_nearby)
  gc()
 
 
  ## Update all negative sample measurement values to 0 ##
  hrly_data_by_chem_date_time_nearby[(hrly_data_by_chem_date_time_nearby$`Sample Measurement` < 0), "Sample Measurement"] <- 0.0

  # fix measurements to specified number of decimals
  hrly_data_by_chem_date_time_nearby$`Sample Measurement`    <- round(hrly_data_by_chem_date_time_nearby$`Sample Measurement`,decimals)  
  hrly_data_by_chem_date_time_nearby$`Date Local`            <- gsub("-","",hrly_data_by_chem_date_time_nearby$`Date Local`)              # remove "-" in dates    

  #==========================================================================================================================================================================================================================================
  
  begin   <<- as.Date(date_start) #as R date object
  endd    <<- as.Date(date_end)   #as R date object
  numDays <<- as.numeric(as.Date(endd,"%y-%m-%d") - as.Date(begin,"%y-%m-%d")) + 1    #number of days
  numMeasurement <<- numDays * 24                                                     #total number of measurements
  hrly_data_cols <<- c("00:00","01:00","02:00","03:00","04:00","05:00","06:00","07:00","08:00","09:00","10:00","11:00",
                      "12:00","13:00","14:00","15:00","16:00","17:00","18:00","19:00","20:00","21:00","22:00","23:00") 
                      #column names of air quality data. Always 24 hours.

  ## Prepare matrix of all relevant air quality ##
  
  stations_all  <<- unique(hrly_data_by_chem_date_time_nearby$`Station ID`)
  hr_base <- as.data.table(select(hrly_data_by_chem_date_time_nearby,`Station ID`,`Date Local`,`Time Local`,`Sample Measurement`))
  mode(hr_base$`Sample Measurement`) <- "numeric"
  hr_mat <- as.data.table(matrix(as.numeric(NA),nrow=numMeasurement,ncol=length(stations_all)))     # has room for all hours 
  setnames(hr_mat,stations_all)
  dates <- as.character(seq(begin,endd,by=1)) 
  hr_missing <- rep(0,length(stations_all))
  hr_mat$`Date Local` <- rep(gsub("-","",dates),each=24)
  hr_mat$`Time Local` <- rep(hrly_data_cols,numDays)
  for (i in 1:length(stations_all)) {
    temp <- hr_base[hr_base$`Station ID`==stations_all[i]]
    temp$`Station ID` <- NULL
    y <- hr_mat %>% left_join(temp, by = c("Date Local","Time Local"))
    hr_mat[,i] <- y$`Sample Measurement`
    hr_missing[i] <- sum(is.na(y$`Sample Measurement`))
  }
  hr_cutoff <- frac * numMeasurement                                        # maximum number of missing hours allowed
  stations1 <- stations_all[hr_missing<= hr_cutoff]                         # remove stations with too much missing data from list
  stations_good <<- stations1[stations1 %in% study$`Station ID`]            # remove stations outside core counties from list
  if (length(stations_good)==0) {
    stop ("No stations meet selection criteria. Please select a different county or allow for more more missing data to be acceptable.")
  }
  hrs_missing   <<- hr_missing[stations_all %in% stations_good]             # number of missing hours at each good station

  return (hr_mat)
}

