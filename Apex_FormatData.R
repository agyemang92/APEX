## APEX Input File Formatting ##
## 07/11/2019 - ICF ##
## This script contains 1 function (Format_data) that formats the data for input to Apex ##

#rm(list=ls())

## load packages ##
require(dplyr)
require(sqldf)
require(data.table)
require(xlsx)
require(readr)
require(foreach)
require(doParallel)

Format_data = function(date_start,date_end,time_start,time_end,FIPS,chem,hrly_data_loc,site_data_loc,monitor_data_loc,countyFIPS_data_loc,ad,aq,ad_ofpn,aq_ofpn){
  ## Error handling start ##
  
  #try-catch blocks to:
  #check that date_end > date_start
  #check that date is YYYY-MM-DD format
  #check that time_end > date_start
  #check that time is in 24-hr format
  
  
  ## Error handling end ##
  
  # reading directly from zip file test
  # testdf = as.data.frame(read_csv('hourly_44201_2018.zip'))
  
  ## read in data ##
  hrly_data                      <- as.data.frame(fread(hrly_data_loc,check.names = FALSE, stringsAsFactors = FALSE))    #read in hourly data
  hrly_data$`State Code`         <- as.integer(hrly_data$`State Code`)                                                   #convert state code column to integers
  hrly_data$`County Code`        <- as.integer(hrly_data$`County Code`)                                                  #convert county code column to integers
  hrly_data$`Sample Measurement` <- as.numeric(hrly_data$`Sample Measurement`)                                           #convert measurement to numeric
  
  site_data                  <- as.data.frame(fread(site_data_loc,check.names = FALSE, stringsAsFactors = FALSE))    #read in site description data
  site_data$`State Code`     <- as.integer(site_data$`State Code`)                                                   #convert state code column to integers
  site_data$`County Code`    <- as.integer(site_data$`County Code`)#NAS intoduced by coercion                        #convert county code column to integers
  site_data$`Site Number`    <- as.integer(site_data$`Site Number`)
  
  monitor_data               <- as.data.frame(fread(monitor_data_loc,check.names = FALSE, stringsAsFactors = FALSE)) #read in monitor description data
  monitor_data$`State Code`  <- as.integer(monitor_data$`State Code`)                                                #convert state code column to integers
  monitor_data$`County Code` <- as.integer(monitor_data$`County Code`)#NAS intoduced by coercion                     #convert county code column to integers
  
  countyFIPS_data                  <- as.data.frame(fread(countyFIPS_data_loc,check.names = FALSE, stringsAsFactors = FALSE)) #read in county FIPS code data
  countyFIPS_data$County_FIPS_Code <- as.integer(countyFIPS_data$County_FIPS_Code)#NAS intoduced by coercion                  #convert county FIPS code to int
  
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
  
  site_data_by_county         <- inner_join(county_code_data, site_data, by = c("County Code","State Code"))              #inner join by county and state codes
  
  monitor_data_by_county      <- inner_join(county_code_data, monitor_data, by = c("County Code","State Code"))           #inner join by county and state codes
  monitor_data_by_county_chem <- inner_join(monitor_data_by_county, chemical_data, by = c("Parameter Name" = "Chemical")) #inner join by chemical
  
  hrly_data_by_chem    <- inner_join(hrly_data, chemical_data, by = c("Parameter Name" = "Chemical"))    #inner join by chemical
  
  ## Query data by date and time ranges ##
  ## Build Query string ##
  QRYstr <- paste0("select * from hrly_data_by_chem ",
                   "where (hrly_data_by_chem.`Date Local` between '",date_start,"' and '",date_end,
                   "' and hrly_data_by_chem.`Time Local` between '",time_start,"' and '",time_end,"')")
  
  ## Run query ##
  hrly_data_by_chem_date_time <- sqldf(QRYstr, stringsAsFactors = FALSE)
  
  hrly_data_by_chem_date_time_county         <- inner_join(county_code_data, hrly_data_by_chem_date_time, by = c("County Code","State Code"))              #inner join by county and state codes
  
  ## Massage hrly data ##
  hrly_data_by_chem_date_time_county$`State Code`            <- sprintf("%02d",hrly_data_by_chem_date_time_county$`State Code`)            # fix state code to 2 characters 
  hrly_data_by_chem_date_time_county$`County Code`           <- sprintf("%03d",hrly_data_by_chem_date_time_county$`County Code`)           # fix county code to 3 characters 
  hrly_data_by_chem_date_time_county$`Site Num`              <- sprintf("%05d",hrly_data_by_chem_date_time_county$`Site Num`)              # fix site number to 5 characters 
  hrly_data_by_chem_date_time_county$`Sample Measurement`    <- sprintf("%.5f",hrly_data_by_chem_date_time_county$`Sample Measurement`)    # fix measurements to 5 numbers after decimal
  hrly_data_by_chem_date_time_county$`Date Local`            <-  gsub("-","",hrly_data_by_chem_date_time_county$`Date Local`)              #remove "-" in dates    
  
  hrly_data_by_chem_date_time_county$`Station ID`            <- paste0(hrly_data_by_chem_date_time_county$`State Code`,hrly_data_by_chem_date_time_county$`County Code`,
                                                                       hrly_data_by_chem_date_time_county$`Site Num`)                      #concatenate State+County+Site IDs
  #write to excel - for QA purposes only#
  #write.xlsx(hrly_data_by_county_chem_date_time, file=ofpn, sheetName="Hrly_data", row.names=FALSE, showNA = FALSE)
  #write.xlsx(monitor_data_by_county_chem, file=ofpn, sheetName="monitor_data", append=TRUE, row.names=FALSE, showNA = FALSE)
  #write.xlsx(site_data_by_county, file=ofpn, sheetName="Site_data", append=TRUE, row.names=FALSE, showNA = FALSE)
  
  hrly_data_by_chem_date_time$`State Code`            <- sprintf("%02d",hrly_data_by_chem_date_time$`State Code`)            # fix state code to 2 characters 
  hrly_data_by_chem_date_time$`County Code`           <- sprintf("%03d",hrly_data_by_chem_date_time$`County Code`)           # fix county code to 3 characters 
  hrly_data_by_chem_date_time$`Site Num`              <- sprintf("%05d",hrly_data_by_chem_date_time$`Site Num`)              # fix site number to 5 characters 
  hrly_data_by_chem_date_time$`Sample Measurement`    <- sprintf("%.5f",hrly_data_by_chem_date_time$`Sample Measurement`)    # fix measurements to 5 numbers after decimal
  hrly_data_by_chem_date_time$`Date Local`            <-  gsub("-","",hrly_data_by_chem_date_time$`Date Local`)              #remove "-" in dates    
  
  hrly_data_by_chem_date_time$`Station ID`            <- paste0(hrly_data_by_chem_date_time$`State Code`,hrly_data_by_chem_date_time$`County Code`,
                                                                hrly_data_by_chem_date_time$`Site Num`)               #concatenate State+County+Site IDs
  
  
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
  
  ## Prepare air district output data ##
  air_districts_data               <- site_data_by_county[,c("County Code","State Code","Site Number","Latitude","Longitude")] #select relevant columns
  
  air_districts_data$`State Code`  <- sprintf("%02d",air_districts_data$`State Code`)  # fix state code to 2 characters 
  air_districts_data$`County Code` <- sprintf("%03d",air_districts_data$`County Code`) # fix county code to 3 characters 
  air_districts_data$`Site Number` <- sprintf("%05d",air_districts_data$`Site Number`) # fix site number to 5 characters 
  air_districts_data$Latitude      <- sprintf("%.4f",air_districts_data$Latitude)      # fix lat number to 4 characters
  air_districts_data$Longitude     <- sprintf("%.4f",air_districts_data$Longitude)     # fix long number to 4 characters
  
  
  air_districts_data$`Station ID`  <- paste0(air_districts_data$`State Code`,air_districts_data$`County Code`,air_districts_data$`Site Number`) #State+County+Site IDs
  air_districts_data$`Start Date`  <- "" #First year
  air_districts_data$`End Date`    <- "" #Last year
  air_districts_data               <- air_districts_data[,c("Station ID","Latitude","Longitude","Start Date","End Date")]                       #final columns
  
  ## Prepare air quality output data ##
  # Prepare datatable to carry air quality data #
  air_quality_data <- data.frame("00:00" = character(),
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
  
  hrly_data_cols           <- c("00:00","01:00","02:00","03:00","04:00","05:00","06:00","07:00","08:00","09:00","10:00","11:00",
                                "12:00","13:00","14:00","15:00","16:00","17:00","18:00","19:00","20:00","21:00","22:00","23:00") #column names of air quality data
  
  ## Transform vector into data frame ##
  hrly_data_cols           <- data.frame(matrix(unlist(hrly_data_cols),nrow = length(hrly_data_cols), byrow = T),check.names = F, stringsAsFactors = F)
  colnames(hrly_data_cols) <- c("Colnames")
  
  
  Stations <- unique(hrly_data_by_chem_date_time_county$`Station ID`) #unique stations in the air quality data
  
  
  
  Clusterz<-makeCluster(3)      #set up 3 processors/cores
  registerDoParallel(Clusterz)  #set up 3 processors/cores
  
  #foreach(i = 1:length(Stations),.packages = c("foreach","dplyr")) %:% #couldn't get nested foreach loop using dopar to work
  ## Loop accross each station and then accross each date to prepare data for export ##
  for (i in 1:length(Stations)){                                      #for each station ID
    currentStation                            <- Stations[i]          #current station
    
    hrly_data_by_county_chem_date_time_filterByStatn <- hrly_data_by_chem_date_time_county[hrly_data_by_chem_date_time_county$`Station ID`==currentStation,] #filter by current station ID
    
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
      
      hrly_data_by_county_chem_date_time_filterByStatnDate   <- right_join(hrly_data_by_county_chem_date_time_filterByStatnDate,hrly_data_cols,by=c("Time Local"="Colnames"))                           #right join by expected columns to align values in their right columns
      
      hrly_data_by_county_chem_date_time_filterByStatnDate_t <- t(hrly_data_by_county_chem_date_time_filterByStatnDate$`Sample Measurement`)                                                            #transpose data
      
      air_quality_data[nrow(air_quality_data)+1, ]           <- c(as.character(hrly_data_by_county_chem_date_time_filterByStatnDate_t),currentdate,currentStation)                                      #append transposed data, current date and current station to air quality data table
    }
  }
  
  stopCluster(Clusterz) #need to stop clusters
  
  clean_data <- list("Air Quality Data" = air_quality_data,"Air Quality Data by ChemDateTime" = hrly_data_by_chem_date_time, "Air Districts Data" = air_districts_data,"Site Data" = site_data, "Site Data by County" = site_data_by_county,"Monitor Data" = monitor_data_by_county_chem)
  #clean_data <- list("Air Quality Data" = air_quality_data,"Air Districts Data" = air_districts_data,"Site Data" = site_data_by_county,"Monitor Data" = monitor_data_by_county_chem)
  
  
  return (clean_data)
}

