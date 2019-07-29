## Format data for output and produce output files ##
## 07/26/2019 - ICF ##
##formats data for output ##
 
format_and_output_data = function(fixed_air_quality_data,data_sites,date_start,date_end,time_start,time_end,chem){

  ## Info for text outputs ##
  chemical <- chem
  yr       <- substr(date_start,1,4)
  location <- "" #To be done...
  
  ## air districts info text ##
  air_districts_info <- paste0("! ", yr, " ", chemical," air quality data for an example metropolitan area", "\n",
                               "! ", "For ", 0, " air quality districts, for the period ", date_start, " ", time_start, " to ", date_end, " ", time_end, "\n",
                               "! This file created ",date())
  
  ## write air district info to file ##
  write(air_districts_info, ad_ofpn)
  
  ## air quality info text ##
  air_quality_info   <- paste0("! ", yr, " base ", chemical, ' air quality data for CSA ', "0 : ",location, "\n",
                               "! ", "0 ","districts ", date_start, " ", time_start, " ", date_end, " ", time_end, "\n",
                               "! ", "Rows are days", "\n",
                               "! ", "Columns ", "\n",
                               "! ", "Can input different monitors by specifying 'Name = #####'")
  
  ## write air quality info to file ##
  write(air_quality_info, aq_ofpn)
  
  # Prepare datatable to carry air quality data #
  air_quality_data_formatted <- data.frame("00:00" = character(),
                                 "01:00" = character(),
                                 "02:00" = character(),
                                 "03:00" = character(),
                                 "04:00" = character(),
                                 "05:00" = character(),
                                 "06:00" = character(),
                                 "07:00" = character(),
                                 "08:00" = character(),
                                 "09:00" = character(),
                                 "10:00" = character(),
                                 "11:00" = character(),
                                 "12:00" = character(),
                                 "13:00" = character(),
                                 "14:00" = character(),
                                 "15:00" = character(),
                                 "16:00" = character(),
                                 "17:00" = character(),
                                 "18:00" = character(),
                                 "19:00" = character(),
                                 "20:00" = character(),
                                 "21:00" = character(),
                                 "22:00" = character(),
                                 "23:00" = character(),
                                 "Date"  = character(),
                            "Station ID" = character(),stringsAsFactors = F,check.names = F)
  
  
  ## Prepare air district output data ##
  air_districts_data               <- data_sites[,c("County Code","State Code","Site Number","Latitude","Longitude")] #select relevant columns
   
  air_districts_data$`State Code`  <- sprintf("%02d",air_districts_data$`State Code`)  # fix state code to 2 characters 
  air_districts_data$`County Code` <- sprintf("%03d",air_districts_data$`County Code`) # fix county code to 3 characters 
  air_districts_data$`Site Number` <- sprintf("%05d",air_districts_data$`Site Number`) # fix site number to 5 characters 
  air_districts_data$Latitude      <- sprintf("%.4f",air_districts_data$Latitude)      # fix lat number to 4 characters
  air_districts_data$Longitude     <- sprintf("%.4f",air_districts_data$Longitude)     # fix long number to 4 characters
   
   
  air_districts_data$`Station ID`  <- paste0(air_districts_data$`State Code`,air_districts_data$`County Code`,air_districts_data$`Site Number`) #State+County+Site IDs
  air_districts_data$`Start Date`  <- "" #First year
  air_districts_data$`End Date`    <- "" #Last year
  air_districts_data               <- air_districts_data[,c("Station ID","Latitude","Longitude","Start Date","End Date")]                       #final columns


  ## Loop accross each station and then accross each date to prepare data for export ##
  for (i in 1:length(unique(fixed_air_quality_data$`Station ID`))){                                      #for each station ID
    #browser()
    currentStation                            <- unique(fixed_air_quality_data$`Station ID`)[i]                  #current station
    
    hrly_data_by_county_chem_date_time_filterByStatn <- fixed_air_quality_data[fixed_air_quality_data$`Station ID`==currentStation,] #filter by current station ID
    
    Dates       <- unique(hrly_data_by_county_chem_date_time_filterByStatn$`Date Local`)            #unique dates in data
    firstdate   <- Dates[1]                                                                         #first date                                                                      
    lastdate    <- Dates[length(Dates)]                                                             #last date
    
    air_districts_data[which(air_districts_data$`Station ID`==currentStation),"Start Date"] <- firstdate   #insert first date into air district output
    air_districts_data[which(air_districts_data$`Station ID`==currentStation),"End Date"]   <- lastdate    #insert last date into air district output
    
    foreach(j = 1:length(Dates),.packages = c("foreach","dplyr")) %do% { #couldn't get dopar to work as I wanted
      #for (j in 1:length(Dates)){ #alternative
      currentdate                                            <- Dates[j]                                                                                                                                #current date
      
      hrly_data_by_county_chem_date_time_filterByStatnDate   <- hrly_data_by_county_chem_date_time_filterByStatn[ hrly_data_by_county_chem_date_time_filterByStatn$`Date Local`==currentdate,]          #filter by current date
      hrly_data_by_county_chem_date_time_filterByStatnDate   <- hrly_data_by_county_chem_date_time_filterByStatnDate[,c("Station ID","Date Local","Time Local","Sample Measurement","Parameter Name")]  #relevant cols
      
      #hrly_data_by_county_chem_date_time_filterByStatnDate   <- right_join(hrly_data_by_county_chem_date_time_filterByStatnDate,hrly_data_cols,by=c("Time Local"="Colnames"))                           #right join by expected columns to align values in their right columns
      
      hrly_data_by_county_chem_date_time_filterByStatnDate_t <- t(hrly_data_by_county_chem_date_time_filterByStatnDate$`Sample Measurement`)                                                            #transpose data
      
      air_quality_data_formatted[nrow(air_quality_data_formatted)+1, ]           <- c(as.character(hrly_data_by_county_chem_date_time_filterByStatnDate_t),currentdate,currentStation)                                      #append transposed data, current date and current station to air quality data table
    }
  }
  
  air_districts_data <- air_districts_data[air_districts_data$`Start Date`!="",]  #only return rows with full data
  
  formatted_data <- list("air districts" = air_districts_data, "air quality" = air_quality_data_formatted)
  return(formatted_data)
  
}