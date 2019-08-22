## Main ##
## Completed 08/20/2019 by ICF ##
## Contains the overall logic ##
 
beginningg <- Sys.time()

source(file.path(getwd(),"Apex_Util_Process_Raw_Data.R"))       # this script processes the raw data and sorts for relevant data etc.
source(file.path(getwd(),"APEX_Util_Fill_Data.R"))              # this script fixes NAs in the data according to specifications.
source(file.path(getwd(),"Apex_Util_Output.R"))                 # this script formats the data for output and produces output files.

cat("\nReading monitor site info \n")
#################### User inputs start ####################
## Set time period of interest ##
date_start           <- user.inputs$Value[user.inputs$Variable=="Start-date"]  # YYYY-MM-DD. Local.
date_end             <- user.inputs$Value[user.inputs$Variable=="End-date"]    # YYYY-MM-DD. Local.
time_start           <- "00:00"        #HH:00. Local 24-hr. 
time_end             <- "23:00"        #HH:00. Local 24-hr.

## Set FIPS code ##
FIPS                 <- as.vector(user.inputs$Value[user.inputs$Variable=="FIPS-code"]) # XXXXX - five digits

## chemical(s) ##
chem                 <- user.inputs$Value[user.inputs$Variable=="Chemical"]     # Chemical names

## Set file directories ##
hrly_data_loc        <- file.path(getwd(),paste0(file.name,".csv"))  # location of hourly data
site_data_loc        <- file.path(getwd(),"aqs_sites.csv")           # location of site description data on disk

## output file location ##
if(!file.exists(file.path(getwd(),"Output"))){dir.create(file.path(getwd(),"Output"))}   # create output directory if it doesn't exist

frac                <- as.numeric(user.inputs$Value[user.inputs$Variable=="MaxNAFrac"])     # acceptable fraction of missing data/Output (range 0 - 1)
max_dist            <- as.numeric(user.inputs$Value[user.inputs$Variable=="MaxD"])        # maximum distance (meters) of nearby station(s) to be used.
thres               <- as.numeric(user.inputs$Value[user.inputs$Variable=="MaxConsMiss"]) # max number of consecutive missing hours for linear interpolation
decimals            <- as.numeric(user.inputs$Value[user.inputs$Variable=="Decimals"])    # number of decimal places on AQ output file  
if ((decimals %% 1)!=0 | decimals < 0) decimals <- 4                                          # default value if decimals not given
study_area         <<- as.character(user.inputs$Value[user.inputs$Variable=="Location"])

#################### User inputs end ####################

# =====================================================================================================================================================
#returns formatted and filtered air quality data
preprocessed_data  <- preprocess_data(date_start,date_end,time_start,time_end,FIPS,frac,chem,hrly_data_loc,site_data_loc,max_dist,decimals)   

#======================================================================================================================================================
# 2. Fill gaps in data
cat("Filling gaps in the data \n")
final_data <- fill_data(preprocessed_data, sites_near, stations_good, stations_all)

#======================================================================================================================================================
#3. Format data for output#
cat("Writing the outputs \n")
output_air_data(final_data,date_start,date_end, chem, metadata)
#======================================================================================================================================================
#4. Map AQS data sites#
cat("Mapping AQS sites \n")
source(file.path(getwd(),"Apex_Util_Map.R"))