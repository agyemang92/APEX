#
#  Function for filling gaps in hourly data, by Graham Glen, ICF, August 2019
#

fill_data = function(preprocessed_data, sites_near, stations_good, stations_all) {

  hr_mat1 <- preprocessed_data
  hr_mat1$`Date Local` <- NULL
  hr_mat1$`Time Local` <- NULL
  hr_mat1 <- as.matrix(hr_mat1)                     # matrix of raw hourly data
  hr_mat2 <- hr_mat1                                # matrix of filled data
  sites_good <- sites_near[sites_near$`Station ID` %in% stations_good]
  sites_all  <- sites_near[sites_near$`Station ID` %in% stations_all]
  d_final <- distm(cbind(sites_all$Longitude,sites_all$Latitude),cbind(sites_good$Longitude,sites_good$Latitude))
  colnames(d_final) <- stations_good    
  num_good <- length(stations_good)                 # number of stations in core counties
  num_all  <- length(stations_all)                  # # stations including nearby ones outside core

  capped <- rep(0,num_good)                         # count of filled values at ends of time series, by station
  inter1 <- rep(0,num_good)                         # count of filled values on first interpolation, by station
  near1  <- rep(0,num_good)                         # count of filled values from neighbors (first pass), by station
  inter2 <- rep(0,num_good)                         # count of filled values on second interpolation, by station
  near2  <- rep(0,num_good)                         # count of filled values from neighbors (second pass), by station
  final_miss <- rep(0,num_good)


  # cap end hours with nearest non-missing neighbor
  last    <- numMeasurement
  closest <- matrix(as.character(NA),nrow=num_good,ncol=num_all)
  dist    <- matrix(as.numeric(NA),nrow=num_good,ncol=num_all)
  for (i in 1:num_good) {
    d_temp     <- d_final[,stations_good[i]]                         # distances to good station #i
    closest[i,1:num_all] <- sites_all$`Station ID`[order(d_temp)]    # for all good stations, order the sites by distance
    dist[i,1:num_all]    <- d_temp[order(d_temp)]                    # distances to above stations, for use with cutoff
    if (is.na(hr_mat1[1,stations_good[i]])) {                        # if first hour missing
      j <- 2                                                         # start with #2 because #1 is same as site i
      while(is.na(hr_mat1[1,closest[i,j]])) j <- j+1                 # as long as hour is missing, go to next nearest site  
      hr_mat2[1,stations_good[i]] <- hr_mat1[1,closest[i,j]]         # fill the first hour at good site #i with jth nearest site
      capped[i] <- capped[i]+1                                       # increment counter of filled values
    }
    if (is.na(hr_mat1[last,stations_good[i]])) {                     # repeat process for last hour of time series
      j <- 2  
      while(is.na(hr_mat1[last,closest[i,j]])) j <- j+1
      hr_mat2[last,stations_good[i]]<- hr_mat1[last,closest[i,j]]
      capped[i] <- capped[i]+1
    }
  }  


  #Interpolate each time series (first time)
  max_miss <- as.numeric(user.inputs$Value[user.inputs$Variable=="MaxConsMiss"])
  for (i in 1:num_good) {
    x <- hr_mat2[,stations_good[i]]                                 # raw hourly time series #i, plus caps at ends
    miss  <- which(is.na(x))                                        # list of missing hours
    while(length(miss)>0) {                                              
      lo <- miss[1]-1                                               # hour before gap starts  
      hi <- miss[1]+1                                               # default for hour after gap ends
      if (length(miss)>1) {
        while (any(miss[2:length(miss)]==hi)) hi <- hi+1            # if end hour is also missing, increase gap size
      }  
      gap1 <- hi-lo                                                 # gap size + 1
      if (gap1<=max_miss+1) {                                       # if gap is short enough
        for (j in 1:(gap1-1)) {
          x[lo+j] <- round(as.numeric(x[lo]*(gap1-j)/gap1 + x[hi]*j/gap1),decimals)   # linear interpolation between lo and hi values
        }
        inter1[i] <- inter1[i]+gap1-1                               # increment number of filled values
      }
      miss <- miss[miss>hi]                                         # shorten list of missing hours       
    }  
    hr_mat2[,stations_good[i]] <- x                                 # copy filled time series to hr_mat2
  }                                                                 # continue with next good station  
  

  # fill with nearest neighbors out to max_dist
  for (i in 1:num_good) {
    d   <- dist[i,]
    c   <- closest[i,]                                              # c contains station IDs sorted by distance 
    num <- length(d[d<=max_dist])                                   # number of stations within max_dist
    j   <- 2                                                        # skip first station because it is the same as #i 
    x   <- hr_mat2[,stations_good[i]]                               # start with data filled by previous steps
    c1  <- sum(is.na(x))                                            # initial number of missing hours
    c2  <- c1                                                       # current number of missing hours
    while (c2>0 & j<=num) {                                         # station still has missing hours and more stations to check
      x[is.na(x)] <- hr_mat1[is.na(x),c[j]]                         # fill with raw values from station #j
      j <- j+1                                                      # go to next nearest station
      c2 <- sum(is.na(x))                                           # number of values still missing
    }  
    near1[i] <- near1[i] + c1-c2                                    # number of values filled on this step
    hr_mat2[,stations_good[i]] <- x                                 # copy filled data to hr_mat2 
  }                                                                 # continue with next good station   


  #Interpolate each time series (second time)
  for (i in 1:num_good) {
    x <- hr_mat2[,stations_good[i]]                                 # start with data filled by previous steps
    miss  <- which(is.na(x))                                        # logic is the same as for inter1 step
    while(length(miss)>0) {                                         
      lo <- miss[1]-1
      hi <- miss[1]+1
      if (length(miss)>1) {
        while (any(miss[2:length(miss)]==hi)) hi <- hi+1
      }  
      gap1 <- hi-lo
      if (gap1<=max_miss+1) {
        for (j in 1:(gap1-1)) {
          x[lo+j] <- round(as.numeric(x[lo]*(gap1-j)/gap1 + x[hi]*j/gap1),decimals)
        }
        inter2[i] <- inter2[i]+gap1-1                               # update count for second interpolation
      }
      miss <- miss[miss>hi]
    }  
    hr_mat2[,stations_good[i]] <- x
  }                                                                 # continue with next good station  


  # fill with nearest neighbors out to 3*max_dist (all sites saved)
  for (i in 1:num_good) {
    d   <- dist[i,]
    c   <- closest[i,]
    num <- length(d)
    j   <- 2
    x   <- hr_mat2[,stations_good[i]]                       # start with data filled by earlier steps
    c1  <- sum(is.na(x))                                    # number of missing hours at start of step
    c2  <- c1                                               # current number of missing hours
    while (c2>0 & j<=num) {                                 # station still has missing hours
      x[is.na(x)] <- hr_mat2[is.na(x),c[j]]                 # this time, use filled data from nearby sites
      j <- j+1
      c2 <- sum(is.na(x))
    }  
    near2[i] <- near2[i] + c1-c2                            # count of values filled on this step
    hr_mat2[,stations_good[i]] <- x                         # copy data back to filled matrix
    final_miss[i] <- sum(is.na(x))
  }                                                         # continue with next good station 

  #summarize results of filling
  sites_far <- sites_all[!sites_all$`Station ID` %in% stations_good]
  hr_far   <- hr_mat2[,sites_far$`Station ID`]
  num_far  <- num_all-num_good
  hrs_miss_far <- rep(0,num_far)
  for (i in 1:num_far){
    hrs_miss_far[i] <- sum(is.na(hr_far[,i]))
  }
  status <- rep("Drop",num_good)
  status[final_miss==0] <- "Keep"
  x1 <- data.table(cbind(stations_good,sites_good$Latitude,sites_good$Longitude,status,hrs_missing, capped,inter1,near1,inter2,near2,final_miss))
  x2 <- data.table(cbind(sites_far$`Station ID`,sites_far$Latitude,sites_far$Longitude,"Drop",hrs_miss_far,0,0,0,0,0,hrs_miss_far))
  setnames(x1,1:3,c("Station ID", "Latitude","Longitude"))
  setnames(x2,1:11,names(x1)[1:11])
  metadata <<- rbind(x1,x2, fill=TRUE)
  
  return(hr_mat2)
}  
