## Main ##
## 07/26/2019 - ICF ##
## contains the overall logic ##
 
rm(list=ls())
 
source(file.path(getwd(),"Apex_Util_PreprocessData.R"))      #this script processes the raw data and sorts for relevant data etc.
source(file.path(getwd(),"Apex_Util_FixData.R"))             #this script fixes NAs in the data according to specifications.
source(file.path(getwd(),"Apex_Util_FormatDataForOutput.R")) #this script formats the data for output and produces output files.

before<-Sys.time()

wd <- getwd()

user.inputs <- read.table(paste0(wd,"/User_inputs.txt"), header = TRUE, stringsAsFactors = FALSE)
file.name <- "hourly_44201_2010"

#################### User inputs start ####################
## Set time period of interest ##
date_start           <- user.inputs$Value[user.inputs$Variable=="Start-date"]  #YYYY-MM-DD. Local.
date_end             <- user.inputs$Value[user.inputs$Variable=="End-date"]  #YYYY-MM-DD. Local.

time_start           <- "00:00"        #HH:00. Local 24-hr. 
time_end             <- "23:00"        #HH:00. Local 24-hr.

## Set FIPS code ##

FIPS                 <- as.vector(user.inputs$Value[user.inputs$Variable=="FIPS-code"]) #XXXXX - five digits

## chemical(s) ##
chem                 <- user.inputs$Value[user.inputs$Variable=="Chemical"]     #Chemical names

## Set file directories ##
hrly_data_loc        <- file.path(getwd(),paste0(file.name,".csv")) #location of hourly data
site_data_loc        <- file.path(getwd(),"aqs_sites.csv")         #location of site description data on disk
monitor_data_loc     <- file.path(getwd(),"aqs_monitors.csv")      #location of monitor description data on disk
countyFIPS_data_loc  <- file.path(getwd(),"FIPS_County.csv")       #location of county FIPS code data

## output file location ##
if(!file.exists(file.path(getwd(),"Output"))){dir.create(file.path(getwd(),"Output"))}  #create output directory if it doesn't exist

if(!file.exists(file.path(getwd(),"Output/QA"))){dir.create(file.path(getwd(),"Output/QA"))}  #QA only

                       
ad                  <- "air_district.txt"                 #air district file name
aq                  <- "air_quality.txt"                  #air quality file name
ad_ofpn             <- file.path(getwd(),"Output",ad)          #air district output file path name
aq_ofpn             <- file.path(getwd(),"Output",aq)          #air quality output file path name

frac                <- as.numeric(user.inputs$Value[user.inputs$Variable=="MaxFrac"])   #acceptable fraction of missing data/Output (a fraction from 0 - 1)
max_dist            <- as.numeric(user.inputs$Value[user.inputs$Variable=="MaxD"]) #maximum distance (meters) of nearby station(s) to be used.
thres               <- as.numeric(user.inputs$Value[user.inputs$Variable=="MaxCumMiss"])  #number of consecutive missing hourly data at or below which linear interpolation will be used, and beyond which data from the next nearest station will be used

#################### User inputs end ####################

#===============================================================================================================================================================================================================================================
#1. Process Raw Data#
preprocessed_data <- preprocess_data(date_start,date_end,time_start,time_end,FIPS,frac,chem,hrly_data_loc,site_data_loc,monitor_data_loc,countyFIPS_data_loc,ad,aq,ad_ofpn,aq_ofpn,max_dist)   #returns formatted and filtered air quality data and air districts data

air_quality_data   <- preprocessed_data$`Air Quality Data Vector`  #air quality data
missingDaysFilled  <- preprocessed_data$`Missing Days`             #missing days filled

air_quality_data_ChemDateTime <- preprocessed_data$`Air Quality Data by ChemDateTime` #air quality data filtered to the chemical, date, and time period

monitor_data          <- preprocessed_data$`Monitor Data`                                                         #get formatted monitor data
Sites_d_near          <- preprocessed_data$`distance to nearby sites`                                             #get distances to nearby sites
site_near             <- preprocessed_data$`Sites Near Data`                                                      #sites near data
data_sites            <- preprocessed_data$`Data Sites`                                                           #sites in question

#===============================================================================================================================================================================================================================================

#### QA ####
write.csv(air_quality_data_ChemDateTime, file = file.path(getwd(),"Output/QA/chemDateTime.csv"))
write.csv(air_quality_data,file.path(getwd(),"Output/QA/air_quality_data_Vector.csv"),row.names = F)
#air_quality_data_Vector <- read.csv("C:/Users/39492/Desktop/temp/testing.csv",stringsAsFactors = F, check.names = F)
#air_quality_data_Vector$`Time Local` <- sprintf("%05s",air_quality_data_Vector$`Time Local`)
#air_quality_data_Vector$`Time Local` <- gsub(" ",0,air_quality_data_Vector$`Time Local`)
#air_quality_data_Vector$`UniqueLocID` <- paste0(air_quality_data_Vector$`Parameter Name`,air_quality_data_Vector$`Station ID`,air_quality_data_Vector$`Date Local`, air_quality_data_Vector$`Time Local`)
#### QA ####

#===========================================================================================================================================================================================================================================
#2. Fix missing data#
small_window_fixes <- data.frame("Station ID" = character(),"Date" = character(),"Hour" = character(),"Fixed?" = character(), stringsAsFactors = F, check.names = F) #keep track of where replacements are made
large_window_fixes <- data.frame("Station ID" = character(),"Date" = character(),"Hour" = character(),"Fixed?" = character(), stringsAsFactors = F, check.names = F) #dataframe to keep track of where data is replaced

#2.a first fix small and large gaps given the user specifications
fixed_data <- fix_data(air_quality_data_ChemDateTime,air_quality_data,Sites_d_near,small_window_fixes,large_window_fixes,thres,"2a")
fixed_air_quality_data <- fixed_data$`air quality`

small_window_fixes <- fixed_data$smdf
large_window_fixes <- fixed_data$lwdf

#2.b. Second, if there are remaining NAs, then fix small gaps with linear interpolation
if(TRUE %in% is.na(fixed_air_quality_data)){
  small_window_fixes[nrow(small_window_fixes)+1,] <- c("2b","2b","2b","2b") #separation
  large_window_fixes[nrow(large_window_fixes)+1,] <- c("2b","2b","2b","2b") #separation
  
  fixed_data <- fix_data(air_quality_data_ChemDateTime,fixed_air_quality_data,Sites_d_near,small_window_fixes,large_window_fixes,999,"2b") #thres == 999 so any missing data that can be filled is filled (all NAs will be filled unless they occur at the ends of data)
  fixed_air_quality_data <- fixed_data$`air quality`
  
  small_window_fixes <- fixed_data$smdf
  large_window_fixes <- fixed_data$lwdf
  
}

#2.c. Third, if there are still NAs, then fix all NAs with neaby station data (this will fix NAs at the ends of the data)
if(TRUE %in% is.na(fixed_air_quality_data)){
  small_window_fixes[nrow(small_window_fixes)+1,] <- c("2c","2c","2c","2c") #separation
  large_window_fixes[nrow(large_window_fixes)+1,] <- c("2c","2c","2c","2c") #separation
  
  fixed_data <- fix_data(air_quality_data_ChemDateTime,fixed_air_quality_data,Sites_d_near,small_window_fixes,large_window_fixes,0,"2c") #thres == 0 so any missing data will be filled with data from nearby station.
  fixed_air_quality_data <- fixed_data$`air quality`
  
  small_window_fixes <- fixed_data$smdf
  large_window_fixes <- fixed_data$lwdf
  
}

#2.d. If there are some remaining NAs, then these could be fixed with linear interpolation (this will fix NAs at the ends of the data that had some but not all NAs filled with nearby data)
if(TRUE %in% is.na(fixed_air_quality_data)){
  small_window_fixes[nrow(small_window_fixes)+1,] <- c("2d","2d","2d","2d") #separation
  large_window_fixes[nrow(large_window_fixes)+1,] <- c("2d","2d","2d","2d") #separation
  
  fixed_data <- fix_data(air_quality_data_ChemDateTime,fixed_air_quality_data,Sites_d_near,small_window_fixes,large_window_fixes,999,"2d") #thres == 999 so any missing data that can be filled is filled (all NAs will be filled unless they occur at the ends of data)
  fixed_air_quality_data <- fixed_data$`air quality`
  
  small_window_fixes <- fixed_data$smdf
  large_window_fixes <- fixed_data$lwdf
  
}

#2.e. Last, if there still are NAs, then these cannot be fixed with the given max_dist (NAs at the ends of the data that have no data at nearby stations within max_dist) otherwise data is good.

if(TRUE %in% is.na(fixed_air_quality_data)){
print("There are NAs in data. Increasing max_dist could fix this problem. Increasing fraction of available data needed to consider a station could also fix this problem.")
}else{print("there are no NAs in data.")}

#===========================================================================================================================================================================================================================================

#### QA ####
write.csv(fixed_air_quality_data, file = file.path(getwd(),"Output/QA/fixed_air_quality_data.csv"))
write.csv(large_window_fixes,file.path(getwd(),"Output/large_window_fixes.csv"))
write.csv(small_window_fixes,file.path(getwd(),"Output/small_window_fixes.csv"))
#### QA ####

#===========================================================================================================================================================================================================================================
#3. Format data for output#
output_for_apex <- format_and_output_data(fixed_air_quality_data,data_sites,date_start,date_end,time_start,time_end,chem)

air_districts_data         <- output_for_apex$`air districts`
air_quality_data_formatted <- output_for_apex$`air quality`

#===========================================================================================================================================================================================================================================
#4. Write output files#

# write air district data to file #
write.table(air_districts_data, ad_ofpn, append = TRUE, sep = "   ", dec = ".", row.names = FALSE, col.names = FALSE,quote = F)#,na="0.00000")

##Note: we could also create air_district_data in the nested loop in the "format and output data" function using the hrly data table. I did it this way as a form of a QA.

## write air quality data to text file ##
for (k in unique(air_quality_data_formatted$`Station ID`)){                                    #for each station ID
  air_quality_data_sub <- air_quality_data_formatted[air_quality_data_formatted$`Station ID`== k,]       #filter by current station ID
  air_quality_data_sub <- air_quality_data_sub[,c(1:25)]                             #need only the first 25 columns
  write(paste0("Name =",k),aq_ofpn,append = T)                                       #print out name
  write.table(air_quality_data_sub, aq_ofpn, append = TRUE, sep = " ", dec = ".", row.names = FALSE, col.names = FALSE,quote = F)
}
#===========================================================================================================================================================================================================================================

after<-Sys.time()
after-before

#===========================================================================================================================================================================================================================================
