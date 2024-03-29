# This script makes a flexible map to map the area defined by the user defined FIPS codes.
# Completed 8/20/2019 by ICF

#Create the dataframe of coordinates to plot
graphingdata <- metadata[, c("Latitude", "Longitude")]

graphingdata$Latitude <- as.numeric(graphingdata$Latitude)
graphingdata$Longitude <- as.numeric(graphingdata$Longitude)

#Compute bounding box of the map to download
height <- max(graphingdata$Latitude) - min(graphingdata$Latitude)
width <- max(graphingdata$Longitude) - min(graphingdata$Longitude)
sac_borders <- c(bottom  = min(graphingdata$Latitude)  - 0.1 * height, 
                 top     = max(graphingdata$Latitude)  + 0.1 * height,
                 left    = min(graphingdata$Longitude) - 0.1 * width,
                 right   = max(graphingdata$Longitude) + 0.1 * width)

#Get the site map
map <- get_stamenmap(sac_borders, maptype = "terrain")
scale_dist <- ifelse(as.numeric(user.inputs$Value[user.inputs$Variable=="MaxD"])<=30000,5,20)
anchor_x <- ifelse(as.numeric(user.inputs$Value[user.inputs$Variable=="MaxD"])<=30000,sac_borders[[3]]+0.2,sac_borders[[3]]+0.6)
anchor_y <- ifelse(as.numeric(user.inputs$Value[user.inputs$Variable=="MaxD"])<=30000,sac_borders[[1]]+0.1,sac_borders[[1]]+0.2)
scale_height <- ifelse(as.numeric(user.inputs$Value[user.inputs$Variable=="MaxD"])<=30000,0.01,0.02)

site_map <- map
#create and save the map
pdf(file=paste0(wd,"/Output/", "AQ_", "Stations_Map_", user.inputs$Value[user.inputs$Variable=="Chemical"], "_",
                user.inputs$Value[user.inputs$Variable=="data-year"], "_", study_area,".pdf"))
map <- ggmap(site_map)+ 
  ggsn::scalebar(x.min = sac_borders[[3]]-1, x.max = sac_borders[[4]]+1,y.min = sac_borders[[1]]-1, y.max = sac_borders[[2]]+1,transform = TRUE,
                 dist = scale_dist,dist_unit = "km",model = "WGS84",anchor = c(x=anchor_x,y=anchor_y), st.size = 2, border.size = 0.5, height = scale_height)+
  geom_point(data = graphingdata, aes(x = as.numeric(Longitude), 
                                      y = as.numeric(Latitude)), color = "red", size = 2) + 
  ggtitle(paste0(user.inputs$Value[user.inputs$Variable=="data-year"]," ", 
                 study_area, " Study Area"), 
          subtitle = paste0("Air quality sites for ", tolower(user.inputs$Value[user.inputs$Variable=="Chemical"]),
                            " marked in red.")) 
print(map)
dev.off()
