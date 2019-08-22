# This script makes a flexible map to map the area defined by the user defined FIPS codes.
# Completed 8/20/2019 by ICF
graphingdata <- select(metadata, Latitude, Longitude)

# compute the bounding box
bc_bbox <- make_bbox(lat = as.numeric(graphingdata$Latitude), lon = as.numeric(graphingdata$Longitude))
bc_bbox

# grab the map from google
site_map <- get_map(location = bc_bbox, source = "google", maptype = "terrain")

#create and save the map
pdf(file=paste0(wd,"/Output/", "AQ_", "Stations_Map_", user.inputs$Value[user.inputs$Variable=="Chemical"], "_",
                                             user.inputs$Value[user.inputs$Variable=="data-year"], "_", study_area,".pdf"))
map <- ggmap(site_map, legend = "bottom") + 
  geom_point(data = graphingdata, aes(x = as.numeric(Longitude), y = as.numeric(Latitude)), color = "red", size = 2) 
print(map)
dev.off()