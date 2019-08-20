## Format data for output and produce output files ##
## Completed 08/20/2019 by ICF ##
##formats data for output ##
 
#format_and_output_data = function(fixed_air_quality_data,data_sites,date_start,date_end,time_start,time_end,chem){
output_air_data = function(final_data,date_start,date_end,chem, metadata) {
  
  # summarize results
  yr             <- substr(date_start,1,4)
  num_all        <- ncol(final_data)                                    # Number of stations including nearby ones
  stations_final <- metadata$`Station ID`[metadata$status=="Keep"]      # List of stations for use in APEX
  num_final      <- length(stations_final)                              # Number of usable stations
  
  ## get quantiles of final_data
  quants <- matrix(0,nrow=num_all,ncol=5)
  for (i in 1:num_all) {
    quants[i,1:5] <- quantile(final_data[,i],na.rm=TRUE)
  }
  ## air districts info text ##
  air_districts_meta <- paste0("! ", yr, " ", chem," air quality data for ",study_area, " in units of ",tolower(data_unit),"\n",
                               "! ", "For ", num_all, " air quality districts in the period from ", date_start, " ", time_start, " to ", date_end, " ", time_end, "\n",
                               "! This file was created ",date(),"\n",
                               "!StationID, Latitude, Longitude, Status, HrMiss, Capped, Inter1, Near1, Inter2, Near2, HrMissEnd, Minimum, 25%ile, Median, 75%ile, Maximum")
  
  ## write metadata on air districts to file ##
  am_filename <- paste0("output/AQ_metadata_",chem,"_",yr,"_",gsub(" ","",study_area),".csv")
  fwrite(as.list(air_districts_meta), am_filename,quote=FALSE)
  fwrite(as.data.table(cbind(metadata,quants)), am_filename,append=TRUE, sep=",",quote=FALSE)
  
  
  ## Info for text outputs ##
  aq_filename <- paste0("output/AQ_concs_",chem,"_",yr,"_",gsub(" ","",study_area),".txt")
  ## air quality info text ##
  air_quality_info   <- paste0("! ", yr, " base ", chem, " air quality data for ",study_area, " in units of ",tolower(data_unit),"\n",
                               "! ", num_final," districts; From ", date_start, " ", time_start, " to ", date_end, " ", time_end, "\n",
                               "! ", "Data for each site follows the line starting with 'Name = ' and the station ID\n",
                               "! ", "Rows are days and columns are hours within each day - with the date as the 25th (last) column\n",
                               "! ", "This file was prepared on ",date(),"\n!")
  
  ## write air quality info to file ##
  fwrite(as.list(air_quality_info), aq_filename,quote=FALSE)
  
  dates <- as.list(seq(begin,endd,by=1))
  APEXdates <- rep("0",numDays)
  for (j in 1:numDays){
    APEXdates[j] <- gsub("-","",as.character(dates[[j]]))
  }
  
  for (i in 1:num_final) {
    fwrite(as.list(paste("Name = ",stations_final[i])),aq_filename,append=TRUE,quote=FALSE)
    aq_matrix <- matrix(0,nrow=numDays,ncol=24)
    for (j in 1:numDays) {
      first <- 24*j-23
      last  <- 24*j
      aq_matrix[j,1:24] <- sprintf(paste0("%.",decimals,"f"),final_data[first:last,stations_final[i]])
    }
    aq_final <- data.table(cbind(aq_matrix,APEXdates))
    setnames(aq_final,c(hrly_data_cols,"APEXdate"))
    fwrite(aq_final,aq_filename,append=TRUE,sep=" ",quote=FALSE)
  }
 
  ## Prepare air district output data ##
  ad_filename <- paste0("output/AQ_districts_",chem,"_",yr,"_",gsub(" ","",study_area),".txt")
  APEX_start  <- gsub("-","",date_start)
  APEX_end    <- gsub("-","",date_end)
  air_districts_info  <- paste0("! ", yr," ",chem," air district locations for ",study_area,"\n",
                                "! From ",date_start," to ",date_end,"\n",
                                "! File was created on ",date(),"\n! \n",
                                "! StationID Latitude Longitude StartDate EndDate")  
  fwrite(as.list(air_districts_info), ad_filename,quote=FALSE)                              
  sites_final <- sites_near[sites_near$`Station ID` %in% stations_final]
  sites_info  <- as.data.table(cbind(sites_final$`Station ID`,sprintf("%.5f",sites_final$Latitude)
                                     ,sprintf("%.5f",sites_final$Longitude),APEX_start,APEX_end))                             
  fwrite(sites_info,ad_filename,append=TRUE, sep=" ",quote=FALSE)

 
  return(aq_final)
  
}