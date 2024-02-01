# merge state wide indicators to germanwide grid

library(raster)

jahr <- "2018"
setwd(paste("V:/BÜK200/CLC",jahr,"/Indikatoren/",sep=""))

#files <- list.files(path="./", pattern=paste("abfl2018.tif$",sep=""), full.names = T)
files <- list.files(path="../", pattern=paste("abfl_",jahr,".tif$",sep=""),recursive = T, full.names = T)
lis <- lapply(files, raster)
#ras <- do.call(mosaic, fun=max,lis)
ras <- mosaic(lis[[1]],lis[[2]],lis[[3]],lis[[4]],lis[[5]],lis[[6]],lis[[7]],lis[[8]],lis[[9]],lis[[10]],lis[[11]],
              lis[[12]],lis[[13]],lis[[14]],lis[[15]],lis[[16]], fun=max)
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, paste("D_abfl",jahr,".tif",sep=""), format="GTiff", overwrite=TRUE)

#files <- list.files(path="./", pattern=paste("bio_2018.tif$",sep=""), full.names = T)
files <- list.files(path="../", pattern=paste("bio_",jahr,".tif$",sep=""),recursive = T, full.names = T)
lis <- lapply(files, raster)
#ras <- do.call(mosaic, fun=max,lis)
ras <- mosaic(lis[[1]],lis[[2]],lis[[3]],lis[[4]],lis[[5]],lis[[6]],lis[[7]],lis[[8]],lis[[9]],lis[[10]],lis[[11]],
              lis[[12]],lis[[13]],lis[[14]],lis[[15]],lis[[16]], fun=max)
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, paste("D_bio",jahr,".tif",sep=""), format="GTiff", overwrite=TRUE)

#files <- list.files(path="./", pattern=paste("erwa_2018.tif$",sep=""), full.names = T)
files <- list.files(path="../", pattern=paste("erwa_",jahr,".tif$",sep=""),recursive = T, full.names = T)
lis <- lapply(files, raster)
#ras <- do.call(mosaic, fun=max,lis)
ras <- mosaic(lis[[1]],lis[[2]],lis[[3]],lis[[4]],lis[[5]],lis[[6]],lis[[7]],lis[[8]],lis[[9]],lis[[10]],lis[[11]],
              lis[[12]],lis[[13]],lis[[14]],lis[[15]],lis[[16]], fun=max)
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, paste("D_erwa",jahr,".tif",sep=""), format="GTiff", overwrite=TRUE)

#files <- list.files(path="./", pattern=paste("e_wind.tif$",sep=""), full.names = T)
files <- list.files(path="../", pattern=paste("e_wind_",jahr,".tif$",sep=""),recursive = T, full.names = T)
lis <- lapply(files, raster)
#ras <- do.call(mosaic, fun=max,lis)
ras <- mosaic(lis[[1]],lis[[2]],lis[[3]],lis[[4]],lis[[5]],lis[[6]],lis[[7]],lis[[8]],lis[[9]],lis[[10]],lis[[11]],
              lis[[12]],lis[[13]],lis[[14]],lis[[15]],lis[[16]], fun=max)
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, paste("D_e_wind",jahr,".tif",sep=""), format="GTiff", overwrite=TRUE)

#files <- list.files(path="./", pattern=paste("MF.tif$",sep=""), full.names = T)
files <- list.files(path="../", pattern=paste("MF_",jahr,".tif$",sep=""),recursive = T, full.names = T)
lis <- lapply(files, raster)
#ras <- do.call(mosaic, fun=max,lis)
ras <- mosaic(lis[[1]],lis[[2]],lis[[3]],lis[[4]],lis[[5]],lis[[6]],lis[[7]],lis[[8]],lis[[9]],lis[[10]],lis[[11]],
              lis[[12]],lis[[13]],lis[[14]],lis[[15]],lis[[16]], fun=max)
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, paste("D_MF",jahr,".tif",sep=""), format="GTiff", overwrite=TRUE)

#files <- list.files(path="./", pattern=paste("SQR_5K.tif$",sep=""), full.names = T)
files <- list.files(path="../", pattern=paste("SQR_5K_",jahr,".tif$",sep=""),recursive = T, full.names = T)
lis <- lapply(files, raster)
#ras <- do.call(mosaic, fun=max,lis)
ras <- mosaic(lis[[1]],lis[[2]],lis[[3]],lis[[4]],lis[[5]],lis[[6]],lis[[7]],lis[[8]],lis[[9]],lis[[10]],lis[[11]],
              lis[[12]],lis[[13]],lis[[14]],lis[[15]],lis[[16]], fun=max)
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, paste("D_SQR_5K",jahr,".tif",sep=""), format="GTiff", overwrite=TRUE)

#files <- list.files(path="./", pattern=paste("pcF.tif$",sep=""), full.names = T)
files <- list.files(path="../", pattern=paste("pcF_",jahr,".tif$",sep=""),recursive = T, full.names = T)
lis <- lapply(files, raster)
#ras <- do.call(mosaic, fun=max,lis)
ras <- mosaic(lis[[1]],lis[[2]],lis[[3]],lis[[4]],lis[[5]],lis[[6]],lis[[7]],lis[[8]],lis[[9]],lis[[10]],lis[[11]],
              lis[[12]],lis[[13]],lis[[14]],lis[[15]],lis[[16]], fun=max)
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, paste("D_pcF",jahr,".tif",sep=""), format="GTiff", overwrite=TRUE)

hor<-"3"

files <- list.files(path="../", pattern=paste("pH_Hor",hor,".tif$",sep=""),recursive = T, full.names = T)
lis <- lapply(files, raster)
ras <- mosaic(lis[[1]],lis[[2]],lis[[3]],lis[[4]],lis[[5]],lis[[6]],lis[[7]],lis[[8]],lis[[9]],lis[[10]],lis[[11]],
              lis[[12]],lis[[13]],lis[[14]],lis[[15]],lis[[16]], fun=max)
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, paste("../D_pH_Hor",hor,".tif",sep=""), format="GTiff", overwrite=TRUE)
