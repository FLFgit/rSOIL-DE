library(data.table)
library(raster)
library(SpaDES.tools)
library(snow)
library(parallel)
library(doParallel)
library(sf)
library(fasterize)
library(stars)

# Rundung bei .5 wird aufgerundet
round2 = function(x, n) { 
  n=0
  posneg = sign(x)
  z = abs(x)*10^n
  z = z + 0.5 + sqrt(.Machine$double.eps)
  z = trunc(z)
  z = z/10^n
  z*posneg
}

# Parallel Processing
cl <- makeCluster(16)
registerDoParallel(cl)
#beginCluster(type="SOCK")
# Control Parallel Processing
getDoParWorkers()
getDoParName()
getDoParVersion()
#stopCluster(cl) 

BL <- "HB"

###################################################
# Verlust Flächen quantitativ (nur LW)
###################################################

files <- list.files(path=paste("./",BL,sep=""), pattern=paste(BL,"_merge_lw_",sep=""), full.names = T)
for (i in 1: length(files)){
    # Einladen merge von Versiegelung und Landnutzung
  merge <- stack(files[i])
  cor <- gsub("([0-9]+).*$","\\1",strsplit(files[i],"_" )[[1]][4] ) # Jahreszahlen
  names(merge) <- c("imcc","nutz") # Reihenfolge beachten
  
  splitRaster(merge$imcc, 4,4,path= "Split",fExt=".grd")
  splitRaster(merge$nutz, 4,4,path= "Split",fExt=".grd")

li <- parLapply(cl,c(1:16), function(y) {
  library(raster)
  imcc_t <- raster(paste("./Split/imcc_tile",y,".gri", sep=""))
  nutz_t <-  raster(paste("./Split/nutz_tile",y,".gri", sep=""))
  
# nur Imperviousness Kacheln mit Wert 1 (=new cover) verwenden
  BL <- "HB"
	st <- stack(nutz_t, imcc_t)
	tile <- calc(st, function(x) ifelse(x[2]==1 ,x[1] ,NA))
	crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
	writeRaster(tile, filename=paste("./",BL,"/li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
})  
	
ras <- mergeRaster(li) 
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, filename=paste("./",BL,"/",BL,"_verlust_lw_",cor,".tif",sep=""), format="GTiff", overwrite=TRUE)
}

# Einzelnes Abspeichern der einzelnen Zeitschnitte 2006-2018
############################################################

files <- list.files(path=paste("./soil_loss/",BL,sep=""), pattern=paste(BL,"_verlust_",sep=""), full.names = T)
# 0 aus Raster entfernen
lis <- lapply(files, raster)
#2006-2009
ges <- calc(lis[[1]], function(x) ifelse(x>0,x,NA))
writeRaster(ges,paste("./soil_loss/",BL,"/",BL,"_verlust_lw_0609.tif",sep=""), format="GTiff", overwrite=TRUE)
#2009-2012
ges <- calc(lis[[2]], function(x) ifelse(x>0,x,NA))
writeRaster(ges,paste("./soil_loss/",BL,"/",BL,"_verlust_lw_0912.tif",sep=""), format="GTiff", overwrite=TRUE)
#2012-2015
ges <- calc(lis[[3]], function(x) ifelse(x>0,x,NA))
writeRaster(ges,paste("./soil_loss/",BL,"/",BL,"_verlust_lw_1215.tif",sep=""), format="GTiff", overwrite=TRUE)
#2015-2018
ges <- calc(lis[[4]], function(x) ifelse(x>0,x,NA))
writeRaster(ges,paste("./soil_loss/",BL,"/",BL,"_verlust_lw_1518.tif",sep=""), format="GTiff", overwrite=TRUE)

# Zusammenfügen verschiedener Zeitschnitte von 2006-2018
##############################################################################

files <- list.files(path=paste("./soil_loss/",BL,sep=""), pattern=paste(BL,"_verlust_",sep=""), full.names = T)
lis <- lapply(files, raster)

#2006-2012
ges <- mosaic(lis[[1]], lis[[2]], fun=max)
writeRaster(ges,paste("./soil_loss/",BL,"/",BL,"_verlust_lw_0612.tif",sep=""), format="GTiff", overwrite=TRUE)

#2012-2018
lis[[3]] <- resample(lis[[3]],lis[[2]]  )
lis[[4]] <- resample(lis[[4]],lis[[2]]  )
ges <- mosaic(lis[[3]], lis[[4]], fun=max)
writeRaster(ges,paste("./",BL,"_verlust_lw_1218.tif",sep=""), format="GTiff", overwrite=TRUE)

#2006-2015
ges <- mosaic(lis[[1]], lis[[2]], lis[[3]], fun=max)
writeRaster(ges,paste("./",BL,"_verlust_lw_0615.tif",sep=""), format="GTiff", overwrite=TRUE)

#2006-2018
ges <- mosaic(lis[[1]], lis[[2]],lis[[3]], lis[[4]], fun=max)
#ges <- calc(ges, function(x) ifelse(x>0,x,NA))
writeRaster(ges,paste("./",BL,"_verlust_lw_gesamt.tif",sep=""), format="GTiff", overwrite=TRUE)

#########################################
# Anteile Flächennutzung (LW)
##########################################

files <- list.files(path=paste("./",BL,sep=""), pattern=paste(BL,"_verlust_lw_",sep=""), full.names = T)
intervals <- list(c(1,2), c(2,3))

flpr <- matrix(NA,2,length(files))

for (i in 1: length(files)){
  ras <- raster(files[i])
  flpr[,i] <- sapply(intervals, function(x) { 
  			  sum(ras[] >= x[1] & ras[] < x[2], na.rm=T) * res(ras)[1]^2/1000000 # km² Berechnung Fläche über Rasterzellengröße
  			})
}

flpr <- cbind(c("Acker","Grünland"),flpr)
colnames(flpr) <- c("ID","0609", "0912","1215","1518")
write.table(flpr,paste("./",BL,"/",BL,"_verlust_lw.csv",sep=""),sep=";",col.names = NA)

###################################################
# Verlust qualitativ Zeitschnitte BÜK200
###################################################

verluste <- list.files(path=paste("./",BL,sep=""), pattern=paste(BL,"_verlust_lw_",sep=""), full.names = T)

files <- list.files(paste("./",BL,"/Indikatoren",sep=""))
files2006 <- files[ grep(files,pattern="2006")] 
files2012 <- files[ grep(files,pattern="2012")]

# nur die Fläche wo Boden verloren gegangen ist aus Indikatoren ausschneiden

for (i in 1: length(verluste)){ 
  ges <- raster(verluste[i])
  cor <- gsub("([0-9]+).*$","\\1",strsplit(verluste[i],"_" )[[1]][4] ) # Jahreszahlen
  if (cor %in% c("0609","0912")){
    li <- lapply(1:length(grep(files,pattern="2006")), function(x) {

       ras <- raster(paste("./",BL,"/Indikatoren/",files2006[x],sep="")) 
    	#raster <- mosaic(ras,ras_be,fun=max)
    	raste <- crop(ras, ges)
    	raste <- resample(raste, ges, method='bilinear')
    	mer <- mask(raste, ges)
    	writeRaster(mer,paste("./",BL,"/",files2006[x],"_Q_",cor,sep=""), format="GTiff", overwrite=T)
    })
    } else {  if (cor %in% c("1215","1518")) {
    li <- lapply(1:length(grep(files,pattern="2012")), function(x) {

       ras <- raster(paste("./",BL,"/Indikatoren/",files2012[x],sep="")) 
    	#raster <- mosaic(ras,ras_be,fun=max)
    	raste <- crop(ras, ges)
    	raste <- resample(raste, ges, method='bilinear')
    	mer <- mask(raste, ges)
    	writeRaster(mer,paste("./",BL,"/",files2012[x],"_Q_",cor,sep=""), format="GTiff", overwrite=T)
    })
    }} # else{} 
  

# Flächenprozent je Bewertungsstufe
qs <- list.files(path=paste("./",BL,sep=""), pattern=paste("_Q_",cor, sep=""), full.names = T)

intervals <- list(c(1,2), c(2,3), c(3,4), c(4,5), c(5,6), c(6,7))
flpr <- matrix(NA,6,length(qs))

for (i in 1: length(qs)){
	r <- raster(qs[i])
	flpr[,i] <- sapply(intervals, function(x) { 
			  sum(r[] >= x[1] & r[] < x[2], na.rm=T) * res(r)[1]^2/1000000 # km² Berechnung Fläche über Rasterzellengröße
			}) 
}

flpr <- cbind(c(1:6),flpr)
colnames(flpr) <- c("Stufe","Abfluss", "Bio", "Wasser", "Wind", "MF", "pcF","SQR")
write.table(flpr,paste("./",BL,"/Anteile_Q_",cor,".csv",sep=""),sep=";",col.names = NA)
} 

cors <- c("0609","0912","1215","1518")
bla <- list()
for (i in 1:4){
  cor <- cors[i] 
  bla[[i]]  <- fread(paste("./",BL,"/Anteile_Q_",cor,".csv",sep=""),sep=";")[,2:9] 
  bla[[i]]  <- cbind(c(rep(cor,6)),bla[[i]] )
} 
ant_ges <- rbind(bla[[1]],bla[[2]],bla[[3]],bla[[4]])
write.table(ant_ges,paste("./",BL,"/Anteile_Q_gesamt.csv",sep=""),sep=";",col.names = NA)
