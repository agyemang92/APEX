## Main ##
## 08/07/2019 - ICF ##
## Contains the overall logic ##
 
#rm(list=ls())

beginningg <- Sys.time()

source(file.path(getwd(),"Apex_Util_PreprocessData.R"))      #this script processes the raw data and sorts for relevant data etc.
source(file.path(getwd(),"Apex_Util_FixData.R"))             #this script fixes NAs in the data according to specifications.
source(file.path(getwd(),"Apex_Util_FormatDataForOutput.R")) #this script formats the data for output and produces output files.


#################### User inputs start ####################
## Set time period of interest ##
date_start           <- user.inputs$Value[user.inputs$Variable=="Start-date"]  #YYYY-MM-DD. Local.
date_end             <- user.inputs$Value[user.inputs$Variable=="End-date"]    #YYYY-MM-DD. Local.
time_start           <- "00:00"        #HH:00. Local 24-hr. 
time_end             <- "23:00"        #HH:00. Local 24-hr.

## Set FIPS code ##
FIPS                 <- as.vector(user.inputs$Value[user.inputs$Variable=="FIPS-code"]) #XXXXX - five digits

## chemical(s) ##
chem                 <- user.inputs$Value[user.inputs$Variable=="Chemical"]     #Chemical names

## Set file directories ##
hrly_data_loc        <- file.path(getwd(),paste0(file.name,".csv"))#location of hourly data
site_data_loc        <- file.path(getwd(),"aqs_sites.csv")         #location of site description data on disk
monitor_data_loc     <- file.path(getwd(),"aqs_monitors.csv")      #location of monitor description data on disk #this data file is not used in the utility.
countyFIPS_data_loc  <- file.path(getwd(),"FIPS_County.csv")       #location of county FIPS code data

## output file location ##
if(!file.exists(file.path(getwd(),"Output"))){dir.create(file.path(getwd(),"Output"))}        #create output directory if it doesn't exist
if(!file.exists(file.path(getwd(),"Output/QA"))){dir.create(file.path(getwd(),"Output/QA"))}  #QA only

ad                  <- "air_district.txt"                      #air district file name
aq                  <- "air_quality.txt"                       #air quality file name
ad_ofpn             <- file.path(getwd(),"Output",ad)          #air district output file path name
aq_ofpn             <- file.path(getwd(),"Output",aq)          #air quality output file path name

frac                <- as.numeric(user.inputs$Value[user.inputs$Variable=="MaxNAFrac"])    #acceptable fraction of missing data/Output (a fraction from 0 - 1)
max_dist            <- as.numeric(user.inputs$Value[user.inputs$Variable=="MaxD"])       #maximum distance (meters) of nearby station(s) to be used.
thres               <- as.numeric(user.inputs$Value[user.inputs$Variable=="MaxConsMiss"]) #number of consecutive missing hourly data at or below which linear interpolation will be used, and beyond which data from the next nearest station will be used

#################### User inputs end ####################

#===============================================================================================================================================================================================================================================
#1. Process Raw Data#

preprocessed_data       <- preprocess_data(date_start,date_end,time_start,time_end,FIPS,frac,chem,hrly_data_loc,site_data_loc,monitor_data_loc,countyFIPS_data_loc,ad,aq,ad_ofpn,aq_ofpn,max_dist)   #returns formatted and filtered air quality data

sites_air_quality_data  <- preprocessed_data$`Air Quality Data for Selected Sites` #air quality data for selected sites
nearby_air_quality_data <- preprocessed_data$`Air Quality Data for Nearby Sites`   #air quality data for nearby sites
site_near               <- preprocessed_data$`Sites Near Data`                     #sites near data
Sites_near_dist         <- preprocessed_data$`distances to nearby sites`           #distances to nearby sites
missingDaysFilled       <- preprocessed_data$`Missing Days`                        #missing days filled

write.csv(sites_air_quality_data, file = file.path(getwd(),paste0("Output/QA/","unfixed_air_quality_data.csv"))) #for QA purposes

#===========================================================================================================================================================================================================================================
#2. Fix missing data#
small_window_fixes <- data.table("Station ID" = character(),"Date" = character(),"Hour" = character(),"Fixed?" = character(), stringsAsFactors = F, check.names = F) #to keep track of small replacements
large_window_fixes <- data.table("Station ID" = character(),"Date" = character(),"Hour" = character(),"Fixed?" = character(), stringsAsFactors = F, check.names = F) #to keep track of large replacements

#2.a first fix small and large gaps given the user specifications
fixed_data             <- fix_data(nearby_air_quality_data,sites_air_quality_data,Sites_near_dist,small_window_fixes,large_window_fixes,thres,max_dist,"User_defined_max_dist run-") #fix NAs in the air quality data

fixed_air_quality_data <- fixed_data$`air quality` #fixed air quality data
small_window_fixes     <- fixed_data$smdf          #small fix notes
large_window_fixes     <- fixed_data$lwdf          #large fix notes

#2.b. Second, if there are remaining NAs, then fix small gaps with try to fix gaps with user specification but consider the widest radius possible, which is 3 * max_dist but cannot exceed 300km == (min(300000,3*max_dist))
if(TRUE %in% is.na(fixed_air_quality_data)){
  small_window_fixes <- rbindlist(list(small_window_fixes, as.list(c("======","======","======","======")))) #separation
  large_window_fixes <- rbindlist(list(large_window_fixes, as.list(c("======","======","======","======")))) #separation
  
  fixed_data             <- fix_data(nearby_air_quality_data,fixed_air_quality_data,Sites_near_dist,small_window_fixes,large_window_fixes,thres,(min(300000,3*max_dist)),"Max_allowed_max_dist run-") 

  fixed_air_quality_data <- fixed_data$`air quality`
  small_window_fixes     <- fixed_data$smdf
  large_window_fixes     <- fixed_data$lwdf
}

Sys.time() - beginningg

write.csv(small_window_fixes, file = file.path(getwd(),paste0("Output/QA/","small_window_fixes.csv"))) #for QA purposes
write.csv(large_window_fixes, file = file.path(getwd(),paste0("Output/QA/","large_window_fixes.csv"))) #for QA purposes
write.csv(fixed_air_quality_data, file = file.path(getwd(),paste0("Output/QA/","fixed_air_quality_data.csv"))) #for QA purposes

#2.c. Last, if there still are NAs, stations containing any NAs will not be considered. 
stations_with_NAs                           <- fixed_air_quality_data$`Station ID`[is.na(fixed_air_quality_data$`Sample Measurement`)] 
stations_with_NAs                           <- unique(stations_with_NAs)
fixed_air_quality_data$`Sample Measurement` <- as.numeric(fixed_air_quality_data$`Sample Measurement`)
final_fixed_air_quality_data                <- fixed_air_quality_data[!(fixed_air_quality_data$`Station ID` %in% stations_with_NAs),]

#===========================================================================================================================================================================================================================================
#3. Format data for output#
output_for_apex            <- format_and_output_data(final_fixed_air_quality_data,site_near,date_start,date_end,time_start,time_end,chem)

air_districts_data         <- output_for_apex$`air districts`
air_quality_data_formatted <- output_for_apex$`air quality`

#===========================================================================================================================================================================================================================================
#4. Write output files#
# write air district data to file #
write.table(air_districts_data, ad_ofpn, append = TRUE, sep = "   ", dec = ".", row.names = FALSE, col.names = FALSE,quote = F)#,na="0.00000")

## write air quality data to text file ##
for (k in unique(air_quality_data_formatted$`Station ID`)){                                              #for each station ID
  air_quality_data_sub <- air_quality_data_formatted[air_quality_data_formatted$`Station ID`== k,]       #filter by current station ID
  air_quality_data_sub <- air_quality_data_sub[,c(1:25)]                                                 #need only the first 25 columns
  write(paste0("Name =",k),aq_ofpn,append = T)                                                           #print out name
  write.table(air_quality_data_sub, aq_ofpn, append = TRUE, sep = " ", dec = ".", row.names = FALSE, col.names = FALSE,quote = F)
}
#===========================================================================================================================================================================================================================================

source(file.path(getwd(),"Apex_Util_PrintMetaData.R")) 
source(file.path(getwd(),"Apex_Util_Map.R"))

#===========================================================================================================================================================================================================================================
