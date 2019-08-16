# This script is a test to see if we can make a flexible map to map the area defined by the user defined FIPS codes.

station_info <- read.csv(site_data_loc)
# station_info$State.Code <- sprintf("%02d",as.integer(station_info$State.Code))            #fix state code to 2 characters 
station_info$County.Code <- sprintf("%03d",as.integer(station_info$County.Code))           #fix county code to 3 characters 
station_info$Site.Number <- sprintf("%05d",as.integer(station_info$Site.Number))              #fix site number to 5 characters 
station_info$`Station ID`<- paste0(station_info$State.Code,station_info$County.Code, station_info$Site.Number) 
station_info <- station_info[station_info$`Station ID` %in% allstatnDataFracs$Station,]

# compute the bounding box
bc_bbox <- make_bbox(lat = station_info$Latitude, lon = station_info$Longitude)
bc_bbox

# grab the maps from google
site_map <- get_map(location = bc_bbox, source = "google", maptype = "terrain")
#> Warning: bounding box given to google - spatial extent only approximate.

# plot the points and color them by sector
pdf(file=paste0(wd,"/Output/Stations_Map.pdf"))
ggmap(site_map) + 
  geom_point(data = station_info, aes(x = Longitude, y = Latitude), color = "red", size = 1.5)
dev.off()