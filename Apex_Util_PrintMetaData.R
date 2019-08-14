### This file writes the meta data file to describe the data that is being fed into APEX ###
#Completed August 6, 2019 by ICF

sheetlist <- unique(final_fixed_air_quality_data$`Station ID`)
for (i in 1:length(sheetlist)) {
  appendyn <- ifelse(i==1, FALSE, TRUE)
  loop.station <- sheetlist[i]
  station.data.summary <- summary(final_fixed_air_quality_data$`Sample Measurement`
                                        [final_fixed_air_quality_data$`Station ID`== loop.station])
  
  #Data to print
  meta.data <- data.frame(Parameter = c("Data pulled from AQS",
                                         "Chemical",
                                         "Dates included",
                                        "Fraction missing data",
                                        "Contaminant Min",
                                        "Contaminant Q25",
                                        "Contaminant Median",
                                        "Contaminant Mean",
                                        "Contaminant Q75",
                                        "Contaminant Max"),
                          Value = c(file.name, #Return value to user for QA
                                     user.inputs$Value[user.inputs$Variable=="Chemical"], #Return value to user for QA
                                     paste0(user.inputs$Value[user.inputs$Variable=="Start-date"], #Return value to user for QA
                                            " to ", user.inputs$Value[user.inputs$Variable=="End-date"]), #Return value to user for QA
                                    allstatnDataFracs$MissingFrac[allstatnDataFracs$Station==sheetlist[i]], #pull missing data fraction
                                    paste(round(station.data.summary[1],4), data_unit, sep = " "), #pull data min
                                    paste(round(station.data.summary[2],4), data_unit, sep = " "), #pull data Q25
                                    paste(round(station.data.summary[3],4), data_unit, sep = " "), #pull data med
                                    paste(round(station.data.summary[4],4), data_unit, sep = " "), #pull data mean
                                    paste(round(station.data.summary[5],4), data_unit, sep = " "), #pull data Q75
                                    paste(round(station.data.summary[6],4), data_unit, sep = " ")  #pull data max
                                    ))
  write.xlsx(meta.data, file = paste0(wd,"/meta_data_",user.inputs$Value[user.inputs$Variable=="Chemical"], "_",
                                      user.inputs$Value[user.inputs$Variable=="data-year"], ".xlsx"),
             sheetName = paste0("Station-",loop.station), append = TRUE)
}