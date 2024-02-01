#.rs.restartR()

setwd("./CLC18_BÜK200/")

library(data.table)
library(raster)
#library(rgdal)
library(snow)
library(parallel)
library(doParallel)
library(sf)
library(fasterize)
library(SpaDES.tools)

#creates unique filepath for temp directory
dir.create (file.path("~/",'temp_Indices'), showWarnings = FALSE)
#sets temp directory
rasterOptions(tmpdir=file.path("~/",'temp_Indices'))

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

free.kernel <- function(cl){
	stopCluster(cl)
	cl <- makeCluster(16)
	registerDoParallel(cl)
	#beginCluster(type="SOCK")
	# Control Parallel Processing
	getDoParWorkers()
	getDoParName()
	getDoParVersion()}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # Zuschneiden auf Bundesland
BL <- "SH"
land <- st_read(paste("../Verwaltungsgrenzen/",BL,".shp",sep=""))

files <- list.files("./",pattern=".tif")

cro <- parLapply(cl,c(1:length(files)), function(x){
  library(sf)
  library(raster)
  BL <- "SH"
  land <- st_read(paste("../Verwaltungsgrenzen/",BL,".shp",sep=""))
  files <- list.files("./",pattern=".tif")
  ras <- raster(paste("./",files[x],sep=""))
  crops <- crop(ras,land)
  mas <- mask(crops,land)
  writeRaster(mas, filename=paste("./",BL,"/",BL,"_",files[x], sep=""), format="GTiff", overwrite=TRUE)})

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
BL <- "SH" # alle ersetzen

setwd(paste("~/Dokumente/CLC18_BÜK200/",BL, sep=""))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Einladen Profilinformationen
kombi <- fread("../tblProfile.csv", sep=";", dec=".",na.strings = c("",NA))
# GW-Stufe nach KA5
kombi[,GW_Stufe:= sub('GWS',"",GWS)] # nur Ziffer der GW-Stufe verwenden

# Einladen Horizontinformationen
hor <- fread("../tblHorizonte.csv", sep=";", dec=".",na.strings = c("",NA))

# Zuweisung Skelettanteil (Vol-%) 1 = <1% | 2 = 1 - <10% | 3 = 10 - <30% | 4 = 30 - <50% |5 = 50 - <75% | 6 > 75%
hor[GROBBOD_K %in% c(0,1), Skelett:= 1][is.na(GROBBOD_K), Skelett:= 1][GROBBOD_K==2, Skelett:= 10][GROBBOD_K==3, Skelett:= 30][GROBBOD_K==4, Skelett:= 50][GROBBOD_K==5, Skelett:= 75][GROBBOD_K==6, Skelett:= 100]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Bodenauflagen werden ignoriert
dat <- hor[!OTIEF<0] 
# neue Nummerierung Horizonte nach Anzahl der HOR_NR --> weniger Durchläufe nötig
dat[, NR:=rep(1:.N), by=BF_ID] # Maximum: 10 Horizonte

# Mächtigkeit der Horizonte
dat[,MAECHT:=UTIEF*10-OTIEF*10] # in cm

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Einladen Basisraster

buk200 <- st_read(paste(BL,"_key_BF_ID.shp", sep=""))
key <- raster(paste(BL,"_key_BF_ID.tif", sep=""))

#pH Wert Raster

dat$pHWert <- as.numeric(as.factor(dat[,PH]))
#cbind(dat[pHWert %in% unique(pHWert),unique(PH)],unique(dat$pHWert))# Übersetzungsschüssel

hor_nr <- lapply(c(1:10), function(x){
  ph.sf <- merge(buk200, dat[NR==x,.(BF_ID, pHWert)], by="BF_ID")
  ph.raster <- fasterize(ph.sf, key, field="pHWert")
  writeRaster(ph.raster, paste(BL,"_pH_Hor",x,".tif", sep=""))
  rm(ph.sf)
  rm(ph.raster)})


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Abflussregulationsfunktion
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# wenn Horizont flacher als 1 dm wird darunter liegender Horizont hinzugezogen
flach <- merge(dat[OTIEF==0 & UTIEF<1,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
flach2 <- merge(flach[UTIEF<1,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
flach3 <- merge(flach2[UTIEF<1,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
#flach4 <- merge(flach3[UTIEF<1,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))

oben <- merge(dat[OTIEF==0,BF_ID, HOR_NR],dat,by=c("BF_ID", "HOR_NR"))

# alle Oberbodenhorizonte zusammen
ob <- rbindlist(list(oben,flach,flach2,flach3))

# Gesamtmächtigkeit als Raster
GesMae <- ob[, sum(UTIEF-OTIEF), by=BF_ID]
GesMae.sf <- merge(buk200, GesMae, by="BF_ID")
GesMae.raster <- fasterize(GesMae.sf, key, field="V1")
writeRaster(GesMae.raster, filename="ab_GesMae.tif", format="GTiff", overwrite=TRUE)

# Horizontnummer der abgespeicherten Raster
# wie viele Horizonte werden benötigt für Berechnung?

nr_hor <- ob[, max(NR), by=BF_ID]
nr_hor.sf <- merge(buk200, nr_hor, by="BF_ID")
nr_hor.raster <- fasterize(nr_hor.sf, key, field="V1")
writeRaster(nr_hor.raster, filename="ab_nr_hor.tif", format="GTiff", overwrite=TRUE)
hormax <- max(values(nr_hor.raster),na.rm=T) # benötigte Anzahl an Horizonten für Berechnung

hor_nr <- raster("ab_nr_hor.tif")
splitRaster(hor_nr,4,4,path="Split")

free.kernel (cl)

  cl <- makeCluster(hormax[1])
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

# Infiltrationskapazität + Skelettzuschlag

hor_nr <- parLapply(cl,c(1:hormax[1]), function(x){
  library(parallel)
  library(doParallel)
  library(raster)
  library(SpaDES.tools)
  
  cl <- makeCluster(8)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
  BL <- "SH" 
  
  # Unterteilen der Raster je Horizont
  
  bokl <- raster(paste(BL,"_Boklasse_Hor",x,".tif", sep=""))
  skel <- raster(paste(BL,"_Skelett_Hor",x,".tif", sep=""))
  maecht <- raster(paste(BL,"_Mächtigkeit_Hor",x,".tif", sep=""))
  splitRaster(skel,4,4,path= "Split")
  splitRaster(maecht,4,4,path= "Split")
  splitRaster(bokl,4,4,path= "Split")
  
  li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    
    BL <- "SH" 
    skel_t <- raster(paste("./Split/",BL,"_Skelett_Hor",x,"_tile",y,".gri", sep=""))
    maecht_t <- raster(paste("./Split/",BL,"_Mächtigkeit_Hor",x,"_tile",y,".gri", sep=""))
    hor_nr_t <- raster(paste("./Split/ab_nr_hor_tile",y,".gri", sep=""))
    bokl_t <- raster(paste("./Split/",BL,"_Boklasse_Hor",x,"_tile",y,".gri", sep=""))
    
    # wenn Horizont nicht nötig für Berechnung dann Boklasse und Skelett auf NA setzen
    # zu berechnender Horizont muss kleinergleich der maximalen Horizontnummer sein
    st <- stack(bokl_t, hor_nr_t)
    bokl_tk <- calc(st, fun=function(z){ifelse(z[2]>=x,z[1],NA)})
    
    st <- stack(skel_t, hor_nr_t)
    skel_tk <- calc(st, fun=function(z){ifelse(z[2]>=x,z[1],NA)})
    
    st <- stack(bokl_tk,maecht_t)
    tile <- calc(st, fun=function(z) {ifelse(z[1]==1, 1*z[2], 
                                             ifelse(z[1]==2, 2*z[2],
                                                    ifelse(z[1] %in% c(3,4,10), 3*z[2],
                                                           ifelse(z[1] %in% c(5:7), 4*z[2],
                                                                  ifelse(z[1] %in% c(8,9), 5*z[2],NA)))))})
    st <- stack(skel_tk,maecht_t)
    tile2 <- calc(st, fun=function(z) {ifelse(z[1] %in% c(4:6), 1*z[2],NA)})
    
    st <- stack(tile, tile2)
    tile <- calc(st, fun=function(z) sum(z,na.rm=T))
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",x,"_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
  })
  
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_ab_hor_",x,".tif",sep=""), format="GTiff", overwrite=TRUE)
  
})

free.kernel (cl)

files <- list.files(path=".", pattern=paste(BL,"_ab_hor_",sep=""), full.names = T)
#Gesamtmächtigkeit
GM <- raster("ab_GesMae.tif") # dm
splitRaster(GM,4,4,path= "Split")

for(i in 1:length(files)){ 
splitRaster(raster(files[i]),4,4,path= "Split")} 

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    hor_nr <- raster("ab_nr_hor.tif")
    hormax <- max(values(hor_nr),na.rm=T) # benötigte Anzahl an Horizonten für Berechnung
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
    
    BL <- "SH" 
    
    files <- list.files(path="./Split", pattern=paste("_ab_hor_._tile",y,".gri", sep=""), full.names = T)

    hor_t <- stack(lapply(files, raster))
  
    gm_t <- raster(paste("./Split/ab_GesMae_tile",y,".gri", sep=""))
    
    hor_st <- stack(hor_t, gm_t)
    tile <- calc(hor_st, fun=function(x) {round2(sum(x[1:hormax[1]], na.rm=T)/(x[hormax[1]+1]*10))}) # gewichtetes Mittel
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
 })   
    ras <- mergeRaster(li) 
    crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(ras,filename=paste(BL,"_ab_Hor.tif", sep=""), format="GTiff", overwrite=TRUE)

free.kernel (cl)

# Bewertung nFK und Zuschläge mit Auflagehorizont
nFK <- raster(paste(BL,"_nFK.tif", sep=""))
splitRaster(nFK,4,4,path= "Split")
auflage <- raster(paste(BL,"_Auflage.tif", sep=""))
splitRaster(auflage,4,4,path= "Split")

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
li <- parLapply(cl, c(1:16), function(y) {
  library(raster)
  BL <- "SH" 
  nfk_t <- raster(paste("./Split/",BL,"_nFK_tile",y,".gri", sep=""))
  auf_t <- raster(paste("./Split/",BL,"_Auflage_tile",y,".gri", sep=""))
  
  # Bestimmung der Wertstufe
  tile <- calc(nfk_t, fun=function(x) ifelse(x ==1,5,ifelse(x==2,4, ifelse(x==3,3, ifelse(x==4, 2,ifelse(x==5 ,1,NA))))))
  
  st <- stack(auf_t, tile)
  tile <- calc(st, fun=function(x) ifelse(x[1]==1,x[2]-1,x[2]))
  
  crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
})
#files <- list.files(path=".", pattern="li_..[0-9]", full.names = T)
ras <- mergeRaster(li) 
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, filename=paste(BL,"_ab_nfk_O.tif",sep=""), format="GTiff", overwrite=TRUE)

free.kernel (cl)

# Bewertung der Hangneigung

# Einladen von Neigung als Rasterdaten

slope_r<-raster(paste(BL,"_Slope_10.tif",sep=""))

# Gruppierung der Neigungsklassen nach Tab 58 a)
recl <- matrix(data=NA,11,2)
recl[,1] <- c(1:11)
recl[,2] <- c(1,1,1,2,2,2,3,3,4,4,5)
slope_rc <- reclassify(slope_r,recl,rigth=NA)

splitRaster(slope_rc,4,4,path= "Split")

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
li <- parLapply(cl, c(1:16), function(y) {
  library(raster)
  BL <- "SH" 
  slope_t <- raster(paste("./Split/",BL,"_Slope_10_tile",y,".gri", sep=""))

  # Bestimmung der Wertstufe
  tile <- calc(slope_t, fun=function(x)ifelse(x==1,5,
                                      ifelse(x==2,4,
                                      ifelse(x==3, 3,
                                      ifelse(x==4,2,
                                      ifelse(x==5, 1,NA))))))
  
  crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
})
#files <- list.files(path=".", pattern="li_", full.names = T)
ras <- mergeRaster(li) 
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, filename=paste(BL,"_ab_neig.tif",sep=""), format="GTiff", overwrite=TRUE)

free.kernel (cl)

# Einladen bisherige Raster

neig <- raster(paste(BL,"_ab_neig.tif",sep=""))
splitRaster(neig,4,4,path= "Split")
nfk <- raster(paste(BL,"_ab_nfk_O.tif",sep=""))
splitRaster(nfk,4,4,path= "Split")
hor <- raster(paste(BL,"_ab_Hor.tif",sep=""))
splitRaster(hor,4,4,path= "Split")

#Bewertung von Versiegelungsgrad/Bodenbedeckung

nutz<-raster(paste(BL,"_Nutzung_2018.tif",sep=""))

# Gruppierung der Bodenbedeckung nach Tab 26 A = 2, GL = 3
recl <- matrix(data=c(1,2,2,3),2,2) 
nutz_rc <- reclassify(nutz,recl,rigth=NA)
splitRaster(nutz_rc,4,4,path= "Split")

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
li <- parLapply(cl, c(1:16), function(y) {
  library(raster)
  BL <- "SH" 
  hor_t <- raster(paste("./Split/",BL,"_ab_Hor_tile",y,".gri", sep=""))
  neig_t <- raster(paste("./Split/",BL,"_ab_neig_tile",y,".gri", sep=""))
  nfk_t <- raster(paste("./Split/",BL,"_ab_nfk_O_tile",y,".gri", sep=""))
  nutz_rc_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))


st <- stack(hor_t,neig_t,nfk_t,nutz_rc_t)
abfluss <- calc(st, fun=function(x) sum(x))

# Klassifizierung

tile <- calc(abfluss, fun=function(x) ifelse(x <=6,5,
                                      ifelse(x %in% c(7:9),4,
                                      ifelse(x %in% c(10:13), 3,
                                      ifelse(x %in% c(14:17) ,2,
                                      ifelse(x>=18, 1,NA))))))

crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
})

ras <- mergeRaster(li) 
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"

# Umkehren der Bewertung
recl <- matrix(data=NA,5,2)
recl[,1] <- c(1:5)
recl[,2] <- c(5,4,3,2,1)
abfl <- reclassify(ras, recl, right=NA)

writeRaster(abfl, filename=paste(BL,"_abfl_2018.tif",sep=""), format="GTiff", overwrite=TRUE)

free.kernel (cl)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Erosionswiderstand Wasser
#~~~~~~~~~~~~~~~~~~~~~~~~~~~

# !!! Moore werden nicht ber?cksichtigt in BA LVL !!! --> haben aber höchsten Erosionswiderstand gegen Wasser --> Klasse 2

# wenn Horizont flacher als 1 dm wird darunter liegender Horizont hinzugezogen
# siehe Abflussregulation für Gesamtmächtigkeit und Anzahl Horizonte
flach <- merge(dat[OTIEF==0 & UTIEF<1,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
flach2 <- merge(flach[UTIEF<1,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
flach3 <- merge(flach2[UTIEF<1,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))

oben <- merge(dat[OTIEF==0,BF_ID, HOR_NR],dat,by=c("BF_ID", "HOR_NR"))

# alle Oberbodenhorizonte zusammen
ob <- rbindlist(list(oben,flach,flach2,flach3))

# Bewertung des Bodenartbedingten Erosionswiderstandes

tab1<- fread("../Tab1.csv", sep=";", dec=",", header = T)

# Zuweisung BEW nur f?r Oberb?den (mind. 0-10 cm)
eros.a <- ob[,.(BEW=(tab1$Klasse[which(tab1[,2:15]==BOART, arr.ind = T)][1])),by=.(BF_ID,NR,GROBBOD_K,HUMUS)]

hor_nr <- lapply(c(1:4), function(x){
  bew.sf <- merge(buk200, eros.a[NR==x,.(BF_ID, BEW)], by="BF_ID")
  bew.raster <- fasterize(bew.sf, key, field="BEW")
  writeRaster(bew.raster, paste("BEW_Hor",x,".tif", sep=""))
  rm(bew.sf)
  rm(bew.raster)})

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Code aus Humus, Skelett und BEW für korr. BEW (vierstellig, dann zweistellig)
# erfolgt für Gesamtheit der BÜK200 Daten, numerischer Schlüssel ist damit immer der Gleiche
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Zuweisung"Humusstufe" <2 % = 1 | 2 - <4 = 2 | >4 = 3 
eros.a <- eros.a[HUMUS %in% c("h0","h1","h2"), HUMUS_K:= 1][HUMUS %in% c("h3"), HUMUS_K:= 2][HUMUS %in% c("h4","h5","h6","h7"), HUMUS_K:= 3]
eros.a[is.na(HUMUS_K), HUMUS_K:=1] # alle ohne Humusgehaltangabe sind h0

# Zuweisung Skelettanteil (Vol-%) 1,2 = 10 | 3 = 30 | 4,5 = 75 | 6 = 100
eros.a[grep("0|1|2",GROBBOD_K), Skelett:= 10][is.na(GROBBOD_K), Skelett:=10][grep("3",GROBBOD_K), Skelett:= 30][grep("4|5",GROBBOD_K), Skelett:= 75][grep("6",GROBBOD_K), Skelett:= 100]
eros.a[is.na(GROBBOD_K), Skelett:=10] # alle ohne Skelettangabe sind <10%

eros.a[, korrBEW:=ifelse(is.na(HUMUS_K)|is.na(Skelett)|is.na(BEW),NA,paste(HUMUS_K,Skelett, BEW,sep=""))]

eros.a$korr <- as.numeric(as.factor(eros.a[,korrBEW]))
#cbind(eros.a[korr %in% unique(korr),unique(korrBEW)],unique(eros.a$korr))# Übersetzungsschüssel

hor_nr <- lapply(c(1:4), function(x){
  bew.sf <- merge(buk200, eros.a[NR==x,.(BF_ID, korr)], by="BF_ID")
  bew.raster <- fasterize(bew.sf, key, field="korr")
  writeRaster(bew.raster, paste("korrBEW_Hor",x,".tif", sep=""))
  rm(bew.sf)
  rm(bew.raster)})
  
  # Zuschneiden auf Bundesland

land <- st_read(paste("../../Verwaltungsgrenzen/",BL,".shp",sep=""))
files <- list.files("./",pattern="korrBEW_Hor")

for (i in 1: length(files)){
  ras <- raster(paste("./",files[i],sep=""))
  crops <- crop(ras,land)
  mas <- mask(crops,land)
  writeRaster(mas, filename=paste("./",BL,"_",files[i], sep=""), format="GTiff", overwrite=TRUE)}

#############################
# Einladen benötigte Raster

GM <- raster("ab_GesMae.tif") # dm
hor_nr <- raster("ab_nr_hor.tif")
splitRaster(hor_nr,4,4,path= "Split")

hormax <- max(values(hor_nr),na.rm=T) # benötigte Anzahl an Horizonten für Berechnung

  cl <- makeCluster(hormax[1])
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

# korr. BEW mit Humus und Skelettgehalt

hor_nr <- parLapply(cl,c(1:hormax[1]), function(x){
  library(snow)
  library(parallel)
  library(doParallel)
  library(raster)
  library(SpaDES.tools)
  
  cl <- makeCluster(8)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
  BL <- "SH" 
  
  # Unterteilen der Raster je Horizont
  
  maecht <- raster(paste(BL,"_Mächtigkeit_Hor",x,".tif", sep=""))
  korrbew <- raster(paste("korrBEW_Hor",x,".tif", sep=""))
  ba <- raster(paste(BL,"_BA_Hor",x,".tif", sep=""))
  splitRaster(maecht,4,4,path= "Split")
  splitRaster(korrbew,4,4,path= "Split")
  splitRaster(ba,4,4,path= "Split")
  
  li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "SH" 
    maecht_t <- raster(paste("./Split/",BL,"_Mächtigkeit_Hor",x,"_tile",y,".gri", sep=""))
    hor_nr_t <- raster(paste("./Split/ab_nr_hor_tile",y,".gri", sep=""))
    ba_t <- raster(paste("./Split/",BL,"_BA_Hor",x,"_tile",y,".gri", sep=""))
    kbew_t <- raster(paste("./Split/korrBEW_Hor",x,"_tile",y,".gri", sep=""))

    # wenn Horizont nicht nötig für Berechnung dann Boklasse und Skelett auf NA setzen
    # zu berechnender Horizont muss kleinergleich der maximalen Horizontnummer sein
    st <- stack(kbew_t, hor_nr_t)
    kbew_tk <- calc(st, fun=function(z){ifelse(z[2]>=x,z[1],NA)})
    
    st <- stack(kbew_t, maecht_t, ba_t)
   
    tile <- calc(st, fun=function(z) {
      ifelse(z[1] %in% c(8,16,24,4:7,15,22,23,1:3), 1*z[2],
      ifelse(z[1] %in% c(9,17,25:27), 2*z[2],
      ifelse(z[1] %in% c(10,18,28), 3*z[2],
      ifelse(z[1] %in% c(11,19,20), 4*z[2],
      ifelse(z[1] %in% c(12,21), 5*z[2],       
      ifelse(z[1] ==13, 6*z[2],       
      ifelse(z[1] ==14, 7*z[2],       
      ifelse(z[1] %in% c(11,12) & z[3] %in% c(13,41) , 5*z[2], #Tu3|Ls2
      ifelse(z[1] ==13 & z[3] %in% c(19,47) , 6*z[2],         # Ut4|Lu
          ifelse(z[1] %in% c(33,40,41,47:49,29:32), 1*z[2],
          ifelse(z[1] %in% c(34,42,50:52), 2*z[2],
          ifelse(z[1] %in% c(35,43,44), 3*z[2],
          ifelse(z[1] %in% c(36,45), 4*z[2],
          ifelse(z[1] %in% c(37,46), 5*z[2], 
          ifelse(z[1] ==38, 6*z[2],
          ifelse(z[1] ==39, 7*z[2],
          ifelse(z[1] ==45 & z[3] ==46 , 4*z[2], #Ut3
               ifelse(z[1] %in% c(58,66,67,73:76,53:57), 1*z[2],
               ifelse(z[1] %in% c(59,60,68,69,77:79), 2*z[2],
               ifelse(z[1] %in% c(70,71), 3*z[2],
               ifelse(z[1] %in% c(61,62,72), 4*z[2],
               ifelse(z[1] ==63, 5*z[2],       
               ifelse(z[1] %in% c(64,65), 6*z[2],       
               ifelse(z[1] %in% c(60,61) & z[3] %in% c(32,33,14,17,18) , 3*z[2], #Su2|Lt3|Lts|Su3|Ls3
               ifelse(z[1] ==63 & z[3] ==46 , 5*z[2],         # Ut3
               ifelse(z[1] ==71 & z[3] ==46 , 3*z[2],       # Ut3
                      ifelse(z[3] %in% c(9:12), 2*z[2],  #Moore
               NA)))))))))))))))))))))))))))})

    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",x,"_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
  })
  
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_Eaqua_hor_",x,".tif",sep=""), format="GTiff", overwrite=TRUE)
  
})

free.kernel (cl)

# gewichtetes Mittel

files <- list.files(path=".", pattern="Eaqua_hor_", full.names = T)

for(i in 1:length(files)){ 
splitRaster(raster(files[i]),4,4,path= "Split")} 

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    hor_nr <- raster("ab_nr_hor.tif")
    hormax <- max(values(hor_nr),na.rm=T) # benötigte Anzahl an Horizonten für Berechnung
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
    
    BL <- "SH" 
    
    files <- list.files(path="./Split", pattern=paste("_Eaqua_hor_._tile",y,".gri", sep=""), full.names = T)

    hor_t <- stack(lapply(files, raster))

    gm_t <- raster(paste("./Split/ab_GesMae_tile",y,".gri", sep=""))
    
    hor_st <- stack(hor_t, gm_t)
    tile <- calc(hor_st, fun=function(x) {round2(sum(x[1:hormax[1]], na.rm=T)/(x[hormax[1]+1]*10))}) # gewichtetes Mittel
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
 })   
    ras <- mergeRaster(li) 
    crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(ras,filename=paste(BL,"_korrBEW_Eaqua.tif",sep=""), format="GTiff", overwrite=TRUE)

    
free.kernel (cl)

###
# Einladen Wölbung
    
wölb <- raster(paste(BL,"_wölb_10.tif", sep="")) 
splitRaster(wölb,4,4,path= "Split")
eaqua_r <- raster(paste(BL,"_korrBEW_Eaqua.tif",sep=""))
splitRaster(eaqua_r,4,4,path= "Split")

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "SH" 
  
    w_t <- raster(paste("./Split/",BL,"_wölb_10_tile",y,".gri", sep=""))
    e_t <- raster(paste("./Split/",BL,"_korrBEW_Eaqua_tile",y,".gri", sep=""))
    
  st <- stack(w_t,e_t)
  eaqua_r <- calc(st, fun=function(x) x[1]+x[2])
# keine Bewertung >9
  tile <- calc(eaqua_r, fun=function(x) ifelse(x>9, 9,x))
    # keine Bewertung <1
  tile <- calc(eaqua_r, fun=function(x) ifelse(x<1, 1,x))
  
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
})  
ras <- mergeRaster(li) 
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, filename="eaqua_w.tif", format="GTiff", overwrite=TRUE)

free.kernel (cl)

# Einladen von ID, Neigung (Gruppiert nach Tab 5) und eros.a als Rasterdaten

key_r<-raster(paste(BL,"_key_BF_ID.tif",sep=""))
slope_r<-raster(paste(BL,"_Slope_10.tif",sep=""))

# Gruppierung der Neigungsklassen nach Tab 5
recl <- matrix(data=NA,11,2)
recl[,1] <- c(1:11)
recl[,2] <- c(1,1,1,2,3,4,5,6,6,6,6)
slope_rc <- reclassify(slope_r,recl,rigth=NA)

writeRaster(slope_rc, filename="slope_eaqua_rc.tif", format="GTiff", overwrite=TRUE)
slope <- raster("slope_eaqua_rc.tif")
splitRaster(slope,4,4,path= "Split")

# Einladen Sommerniederschlag

# Stacking der Rasterdaten

p_so <- raster(paste(BL,"_p_so_10.tif",sep=""))
eaqua_r <- raster("eaqua_w.tif")

# Raster unterteilen und speichern
splitRaster(eaqua_r,4,4,path= "Split")

splitRaster(p_so,4,4,path= "Split")

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
li <- parLapply(cl,c(1:16), function(y) {
  library(raster)
  BL <- "SH" 
	eaqua_t <- raster(paste("Split/eaqua_w_tile",y,".gri",sep=""))
	p_t <- raster(paste("Split/",BL,"_p_so_10_tile",y,".gri",sep=""))
	st <- stack(eaqua_t,p_t)
	# Hinzufügen des R-Faktors, abhängig vom Sommerniederschlag
	tile <- calc(st, fun= function(x) ifelse(x[2] <= 300 & !is.na(x[1]), 40, ifelse(x[2] >= 540 & !is.na(x[1]), 80, ifelse(x[2] > 300 & x[2] < 540 & !is.na(x[1]), 60, NA))))

    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
  })
  
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_R.tif",sep=""), format="GTiff", overwrite=TRUE)

free.kernel (cl)  
  
R<-raster(paste(BL,"_R.tif",sep=""))
splitRaster(R,4,4,path= "Split")
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

li <- parLapply(cl,c(1:16), function(y) {
  library(raster)
  BL <- "SH" 
  # Stacking der Rasterdaten
  slope_t <- raster(paste("Split/slope_eaqua_rc_tile",y,".gri",sep=""))
  eaqua_t <- raster(paste("Split/eaqua_w_tile",y,".gri",sep=""))
  R_t <- raster(paste("Split/",BL,"_R_tile",y,".gri",sep=""))
  
  # R-Wert 40
############## 
  # R-Wert 40
  
#nur Raster mit R=40
  st <- stack(eaqua_t,R_t)
  tile40_e <- calc(st, function(z){
  	ifelse(z[2]==40,z[1],NA)})
  st <- stack(slope_t,R_t)
  tile40_s <- calc(st, function(z){
  	ifelse(z[2]==40,z[1],NA)})

  # nur Raster mit R=40 und Slope=1
  	st <- stack(tile40_s, tile40_e)
  	tile40_es <- calc(st, function(z) ifelse(z[1]==1,z[2],NA))

  tile40_1 <- calc(tile40_es, function(z){
    ifelse(z==1, 0.1,
    ifelse(z==2, 0.2,
    ifelse(z==3, 0.3,
    ifelse(z==4, 0.4,
    ifelse(z==5, 0.6,
    ifelse(z==6, 0.7,
    ifelse(z==7, 0.8,
    ifelse(z==8, 1.0,
    ifelse(z==9, 1.1,	NA)))))))))})

  # nur Raster mit R=40 und Slope=2
  	st <- stack(tile40_s, tile40_e)
  	tile40_es <- calc(st, function(z) ifelse(z[1]==2,z[2],NA))


  tile40_2 <- calc(tile40_es, function(z){
    	ifelse(z==1, 0.2,
    	ifelse(z==2, 0.5,
    	ifelse(z==3, 0.9,
    	ifelse(z==4, 1.2,
    	ifelse(z==5, 1.6,
    	ifelse(z==6, 1.9,
    	ifelse(z==7, 2.3,
    	ifelse(z==8, 2.6,
    	ifelse(z==9, 3.0,	NA)))))))))})

  # nur Raster mit R=40 und Slope=3
  	st <- stack(tile40_s, tile40_e)
  	tile40_es <- calc(st, function(z) ifelse(z[1]==3,z[2],NA))

   tile40_3 <- calc(tile40_es, function(z){
	      ifelse(z==1, 0.5,
	    	ifelse(z==2, 1.4,
	    	ifelse(z==3, 2.4,
	    	ifelse(z==4, 3.3,
	    	ifelse(z==5, 4.2,
	    	ifelse(z==6, 5.2,
	    	ifelse(z==7, 6.1,
	    	ifelse(z==8, 7.1,
	    	ifelse(z==9, 8.0,	NA)))))))))})

	 	      # nur Raster mit R=40 und Slope=4
  	st <- stack(tile40_s, tile40_e)
  	tile40_es <- calc(st, function(z) ifelse(z[1]==4,z[2],NA))

  	tile40_4 <- calc(tile40_es, function(z){
  				ifelse(z==1, 1.2,
		    	ifelse(z==2, 3.5,
		    	ifelse(z==3, 5.9,
		    	ifelse(z==4, 8.3,
		    	ifelse(z==5, 10.6,
		    	ifelse(z==6, 13.0,
		    	ifelse(z==7, 15.3,
		    	ifelse(z==8, 17.7,
		    	ifelse(z==9, 20.1,	NA)))))))))})

  	# nur Raster mit R=40 und Slope=5
  	st <- stack(tile40_s, tile40_e)
  	tile40_es <- calc(st, function(z) ifelse(z[1]==5,z[2],NA))

  	tile40_5 <- calc(tile40_es, function(z){
	    	 	  ifelse(z==1, 2.2,
			    	ifelse(z==2, 6.6,
			    	ifelse(z==3, 10.9,
			    	ifelse(z==4, 15.3,
			    	ifelse(z==5, 19.7,
			    	ifelse(z==6, 24.0,
			    	ifelse(z==7, 28.4,
			    	ifelse(z==8, 32.8,
			    	ifelse(z==9, 37.1,	NA)))))))))})

  		# nur Raster mit R=40 und Slope=6
  	st <- stack(tile40_s, tile40_e)
  	tile40_es <- calc(st, function(z) ifelse(z[1]==6,z[2],NA))

  	tile40_6 <- calc(tile40_es, function(z){
	           	ifelse(z==1, 2.8,
				    	ifelse(z==2, 8.4,
				    	ifelse(z==3, 14.0,
				    	ifelse(z==4, 19.5,
				    	ifelse(z==5, 25.1,
				    	ifelse(z==6, 30.7,
				    	ifelse(z==7, 36.3,
				    	ifelse(z==8, 41.9,
				    	ifelse(z==9, 47.5, NA)))))))))})

st <- stack(tile40_1, tile40_2, tile40_3, tile40_4, tile40_5, tile40_6) # Zusammenfügen der einzelnen Slope-Werte
		 tile40 <- calc(st, function(z) {sum(z,na.rm=T)})

		  crs(tile40) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile40, filename=paste("40_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)

 
  # R-Wert 60 
############## 
  # R-Wert 60
  
# nur Raster mit R=60
  st <- stack(eaqua_t,R_t)
  tile60_e <- calc(st, function(z){
  	ifelse(z[2]==60,z[1],NA)})
  st <- stack(slope_t,R_t)
  tile60_s <- calc(st, function(z){
  	ifelse(z[2]==60,z[1],NA)})
  
  # nur Raster mit R=60 und Slope=1
  	st <- stack(tile60_s, tile60_e)
  	tile60_es <- calc(st, function(z) ifelse(z[1]==1,z[2],NA))
  	
  tile60_1 <- calc(tile60_es, function(z){
    ifelse(z==1, 0.1,
    ifelse(z==2, 0.3,
    ifelse(z==3, 0.5,
    ifelse(z==4, 0.7,
    ifelse(z==5, 0.9,
    ifelse(z==6, 1.1,
    ifelse(z==7, 1.3,
    ifelse(z==8, 1.5,
    ifelse(z==9, 1.7,	NA)))))))))})
  
    # nur Raster mit R=60 und Slope=2
  	st <- stack(tile60_s, tile60_e)
  	tile60_es <- calc(st, function(z) ifelse(z[1]==2,z[2],NA))
  	
  	tile60_2 <- calc(tile60_es, function(z){
    	ifelse(z==1, 0.3,
    	ifelse(z==2, 0.8,
    	ifelse(z==3, 1.4,
    	ifelse(z==4, 1.9,
    	ifelse(z==5, 2.4,
    	ifelse(z==6, 2.9,
    	ifelse(z==7, 3.5,
    	ifelse(z==8, 4.0,
    	ifelse(z==9, 4.5,		NA)))))))))})
    		
    # nur Raster mit R=60 und Slope=3
  	st <- stack(tile60_s, tile60_e)
  	tile60_es <- calc(st, function(z) ifelse(z[1]==3,z[2],NA))
  	
  	tile60_3 <- calc(tile60_es, function(z){
	      ifelse(z==1, 0.7,
	    	ifelse(z==2, 2.1,
	    	ifelse(z==3, 3.6,
	    	ifelse(z==4, 5.0,
	    	ifelse(z==5, 6.4,
	    	ifelse(z==6, 7.8,
	    	ifelse(z==7, 9.2,
	    	ifelse(z==8, 10.6,
	    	ifelse(z==9, 12.0,	NA)))))))))})
    		
    # nur Raster mit R=60 und Slope=4
  	st <- stack(tile60_s, tile60_e)
  	tile60_es <- calc(st, function(z) ifelse(z[1]==4,z[2],NA))
  	
  	tile60_4 <- calc(tile60_es, function(z){
	 	      ifelse(z==1, 1.8,
		    	ifelse(z==2, 5.3,
		    	ifelse(z==3, 8.9,
		    	ifelse(z==4, 12.4,
		    	ifelse(z==5, 15.9,
		    	ifelse(z==6, 19.5,
		    	ifelse(z==7, 23.0,
		    	ifelse(z==8, 26.6,
		    	ifelse(z==9, 30.1, NA)))))))))})
    		
    # nur Raster mit R=60 und Slope=5
  	st <- stack(tile60_s, tile60_e)
  	tile60_es <- calc(st, function(z) ifelse(z[1]==5,z[2],NA))
  	
  	tile60_5 <- calc(tile60_es, function(z){
	    	 	  ifelse(z==1, 3.3,
			    	ifelse(z==2, 9.9,
			    	ifelse(z==3, 16.4,
			    	ifelse(z==4, 23.0,
			    	ifelse(z==5, 29.5,
			    	ifelse(z==6, 36.0,
			    	ifelse(z==7, 42.6,
			    	ifelse(z==8, 49.2,
			    	ifelse(z==9, 55.7, NA)))))))))})
    		
    # nur Raster mit R=60 und Slope=6
  	st <- stack(tile60_s, tile60_e)
  	tile60_es <- calc(st, function(z) ifelse(z[1]==6,z[2],NA))
  	
  	tile60_6 <- calc(tile60_es, function(z){
	           	ifelse(z==1, 4.2,
				    	ifelse(z==2, 12.6,
				    	ifelse(z==3, 21.0,
				    	ifelse(z==4, 29.3,
				    	ifelse(z==5, 37.7,
				    	ifelse(z==6, 46.1,
				    	ifelse(z==7, 54.5,
				    	ifelse(z==8, 62.9,
				    	ifelse(z==9, 71.2, NA)))))))))})
  	
  	st <- stack(tile60_1, tile60_2, tile60_3, tile60_4, tile60_5, tile60_6) # Zusammenfügen der einzelnen Slope-Werte
		 tile60 <- calc(st, function(z) {sum(z,na.rm=T)})
		 
		  crs(tile60) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile60, filename=paste("60_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)


  # R-Wert 80 
############## 
  # R-Wert 80
  
  # nur Raster mit R=80
  st <- stack(eaqua_t,R_t)
  tile80_e <- calc(st, function(z){
  	ifelse(z[2]==80,z[1],NA)})
  st <- stack(slope_t,R_t)
  tile80_s <- calc(st, function(z){
  	ifelse(z[2]==80,z[1],NA)})
  
  # nur Raster mit R=80 und Slope=1
  	st <- stack(tile80_s, tile80_e)
  	tile80_es <- calc(st, function(z) ifelse(z[1]==1,z[2],NA))
  	
  tile80_1 <- calc(tile80_es, function(z){
    ifelse(z==1, 0.1,
    ifelse(z==2, 0.4,
    ifelse(z==3, 0.6,
    ifelse(z==4, 0.9,
    ifelse(z==5, 1.2,
    ifelse(z==6, 1.4,
    ifelse(z==7, 1.7,
    ifelse(z==8, 1.9,
    ifelse(z==9, 2.2,	NA)))))))))})
  
  # nur Raster mit R=80 und Slope=2
  	st <- stack(tile80_s, tile80_e)
  	tile80_es <- calc(st, function(z) ifelse(z[1]==2,z[2],NA))
  	
  tile80_2 <- calc(tile80_es, function(z){
    	ifelse(z==1, 0.4,
    	ifelse(z==2, 1.1,
    	ifelse(z==3, 1.8,
    	ifelse(z==4, 2.5,
    	ifelse(z==5, 3.2,
    	ifelse(z==6, 3.9,
    	ifelse(z==7, 4.6,
    	ifelse(z==8, 5.3,
    	ifelse(z==9, 6.0,		NA)))))))))})
  
    # nur Raster mit R=80 und Slope=3
  	st <- stack(tile80_s, tile80_e)
  	tile80_es <- calc(st, function(z) ifelse(z[1]==3,z[2],NA))
  	
  tile80_3 <- calc(tile80_es, function(z){
	      ifelse(z==1, 0.9,
	    	ifelse(z==2, 2.8,
	    	ifelse(z==3, 4.7,
	    	ifelse(z==4, 6.6,
	    	ifelse(z==5, 8.5,
	    	ifelse(z==6, 10.3,
	    	ifelse(z==7, 12.2,
	    	ifelse(z==8, 14.1,
	    	ifelse(z==9, 16.0,	NA)))))))))})
  
    # nur Raster mit R=80 und Slope=4
  	st <- stack(tile80_s, tile80_e)
  	tile80_es <- calc(st, function(z) ifelse(z[1]==4,z[2],NA))
  	
  tile80_4 <- calc(tile80_es, function(z){
	 	      ifelse(z==1, 2.4,
		    	ifelse(z==2, 7.1,
		    	ifelse(z==3, 11.8,
		    	ifelse(z==4, 16.5,
		    	ifelse(z==5, 21.2,
		    	ifelse(z==6, 26.0,
		    	ifelse(z==7, 30.7,
		    	ifelse(z==8, 35.4,
		    	ifelse(z==9, 40.1,NA)))))))))})
  
    # nur Raster mit R=80 und Slope=5
  	st <- stack(tile80_s, tile80_e)
  	tile80_es <- calc(st, function(z) ifelse(z[1]==5,z[2],NA))
  	
  tile80_5 <- calc(tile80_es, function(z){
	    	 	  ifelse(z==1, 4.4,
			    	ifelse(z==2, 13.1,
			    	ifelse(z==3, 21.8,	
			    	ifelse(z==4, 30.6,
			    	ifelse(z==5, 39.3,
			    	ifelse(z==6, 48.0,
			    	ifelse(z==7, 56.8,
			    	ifelse(z==8, 65.5,
			    	ifelse(z==9, 74.3,NA )))))))))})
  
    # nur Raster mit R=80 und Slope=6
  	st <- stack(tile80_s, tile80_e)
  	tile80_es <- calc(st, function(z) ifelse(z[1]==6,z[2],NA))
  	
  	tile80_6 <- calc(tile80_es, function(z){
	           	ifelse(z==1, 5.6,
				    	ifelse(z==2, 16.8,
				    	ifelse(z==3, 27.9,
				    	ifelse(z==4, 39.1,
				    	ifelse(z==5, 50.3,
				    	ifelse(z==6, 61.4,
				    	ifelse(z==7, 72.6,
				    	ifelse(z==8, 83.8,
				    	ifelse(z==9, 94.9, NA)))))))))})
  	
  	st <- stack(tile80_1, tile80_2, tile80_3, tile80_4, tile80_5, tile80_6) # Zusammenfügen der einzelnen Slope-Werte
		 tile80 <- calc(st, function(z) {sum(z,na.rm=T)})
		 
		  crs(tile80) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile80, filename=paste("80_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })  
# 		 st <- stack(tile40, tile60, tile80) # Zusammenfügen der einzelnen R-Werte
# 		 tile <- calc(st, function(z) {
# 		    ifelse(is.na(z[1]) & is.na(z[2]), z[3],
# 		    ifelse(is.na(z[1]) & is.na(z[3]), z[2],
# 		    ifelse(is.na(z[2]) & is.na(z[3]), z[1], NA)))})
# 		    		
# 		    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
#     writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)		
# 		    		

free.kernel (cl)  

Rnum <- unique(values(R))[!is.na(unique(values(R)))]

if(length(Rnum)==3){
# alle 3 R-Werte vorhanden

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

li <- parLapply(cl,c(1:16), function(y) {
  library(raster)
  # Stacking der Rasterdaten
  R_40_t <- raster(paste("40_",y,".tif",sep=""))
  R_60_t <- raster(paste("60_",y,".tif",sep=""))
  R_80_t <- raster(paste("80_",y,".tif",sep=""))

st <- stack(R_40_t, R_60_t, R_80_t) # Zusammenfügen der einzelnen R-Werte
		 tile <- calc(st, function(z) max(z))
		 # {
		 #    		ifelse(z[1]==0, z[2],ifelse(z[1]>0, z[1],
		 #    		ifelse(z[2]==0, z[1],ifelse(z[2]>0, z[2], NA))))})
		 
		crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
})

ras <- mergeRaster(li) 
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, filename=paste(BL,"_abtrag.tif",sep=""), format="GTiff", overwrite=TRUE)}

if(length(Rnum)==2){ 

# zwei R-Werte vorhanden

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

li <- parLapply(cl,c(1:16), function(y) {
  library(raster)
  BL <- "HE"
  R<-raster(paste(BL,"_R.tif",sep=""))
  Rnum <- unique(values(R))[!is.na(unique(values(R)))]
  # Stacking der Rasterdaten
  R_1_t <- raster(paste(Rnum[1],"_",y,".tif",sep=""))
  R_2_t <- raster(paste(Rnum[2],"_",y,".tif",sep=""))

	st <- stack(R_1_t, R_2_t) # Zusammenfügen der einzelnen R-Werte
		 tile <- calc(st, function(z) max(z))
		 
		crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
})

ras <- mergeRaster(li) 
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, filename=paste(BL,"_abtrag.tif",sep=""), format="GTiff", overwrite=TRUE)}

if (length(Rnum)==1){ 
# nur ein vorkommender R-Wert

files <- list.files(path=".", pattern=paste(Rnum[1],"_",sep=""), full.names = T)
li <- lapply(files,raster)
ras <- mergeRaster(li) 
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, filename=paste(BL,"_abtrag.tif",sep=""), format="GTiff", overwrite=TRUE)}

free.kernel (cl)

####################

# Nutzungsabhängige Korrektur der Bodenabtragswerte

nutz<-raster(paste(BL,"_Nutzung_2018.tif",sep=""))
splitRaster(nutz, 4,4,path= "Split")

# Nutzungsabhängige Korrektur der Bodenabtragswerte
abtrag <- raster(paste(BL,"_abtrag.tif",sep=""))
splitRaster(abtrag,4,4,path= "Split")

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

li <- parLapply(cl,c(1:16), function(y) {
  library(raster)
  BL <- "SH"
  abtrag_t <- raster(paste("./Split/",BL,"_abtrag_tile",y,".gri", sep=""))
  nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))
  st<- stack(abtrag_t,nutz_t)
  ab_korr <- calc(st, fun=function(x) ifelse(x[2]==1, x[1]*3,
	ifelse(x[2]==2, x[1]*0.5,NA)))

# Bestimmung des Erosionswiderstandes laut Tab 7
  ewa <- calc(ab_korr, fun=function(x) ifelse(x<=1, 1,
                                            ifelse(x>1 & x<=5,2,
                                                   ifelse(x>5 & x<=10,3,
                                                          ifelse(x>10 & x<=15,4,
                                                                 ifelse(x>15 & x<=30,5,
                                                                        ifelse(x>30,6, NA)))))))
  
    crs(ewa) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(ewa, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
})  

ras <- mergeRaster(li) 

#Umkehren der Bewertung
recl <- matrix(data=NA,7,2)
recl[,1] <- c(0:6)
recl[,2] <- c(NA,6,5,4,3,2,1)
ras <- reclassify(ras,recl,right=NA)
    
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"  
writeRaster(ras, filename=paste(BL,"_erwa_2018.tif",sep=""), format="GTiff", overwrite=TRUE)

free.kernel (cl)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Erosionswiderstand Wind
#~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ökologischer Feuchtegrad
#~~~~~~~~~~~~~~~~~~~~~~~~~~

# nicht für Torfe

# wenn Horizont flacher als 10 cm wird darunter liegender Horizont hinzugezogen

# Gesamtmächtigkeit von mind. 10 cm als Raster
GM <- raster("ab_GesMae.tif")

# Horizontnummer der abgespeicherten Raster

# wie viele Horizonte werden benötigt für Berechnung?

nr_hor <- raster("ab_nr_hor.tif")
hormax <- max(values(nr_hor),na.rm=T)

repeat{ 

files <- list.files(path="./ökoF", pattern="li_", full.names = T)

try({
if( length(files) <(hormax[1]*36)){ 
  if(length(files)>0){  
  ni <- lapply(files, function(x) gsub("([0-9]+).*$","\\1",strsplit(x,"_" )[[1]][2] ))
  niv <- unique(as.integer(unlist(ni)))
  horm <- c(1:hormax[1])
  horm <-horm[-niv] } else{
   horm <- c(1:hormax[1])  
  } 
  
  # Berechnung wiederholen

  cl <- makeCluster(hormax[1])
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

hor_nr <- parLapply(cl,horm, function(x){
  
  library(snow)
  library(parallel)
  library(doParallel)
  library(raster)
  library(SpaDES.tools)
  
  BL <- "SH" 
  
  nr_hor <- raster("ab_nr_hor.tif")
  gw <- raster(paste(BL,"_GW_Stufe_BÜK1000.tif",sep=""))
  bokl <- raster(paste(BL,"_Boklasse_Hor",x,".tif", sep=""))
  
  cl <- makeCluster(8)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
  #ba <- raster(paste(BL,"_Boklasse_Hor",x,".tif", sep=""))
  splitRaster(nr_hor,4,4,path= "Split",fExt=".grd")
  splitRaster(gw,4,4,path= "Split",fExt=".grd")
  splitRaster(bokl,4,4,path= "Split",fExt=".grd")
  
  li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "SH" 
    ba_t <- raster(paste("./Split/",BL,"_Boklasse_Hor",x,"_tile",y,".gri", sep=""))
    gw_t <- raster(paste("./Split/",BL,"_GW_Stufe_BÜK1000_tile",y,".gri", sep=""))
    hor_nr_t <- raster(paste("./Split/ab_nr_hor_tile",y,".gri", sep=""))
    
    # wenn Horizont nicht nötig für Berechnung dann Bodenklasse auf NA setzen
    st <- stack(ba_t, hor_nr_t)
    ba_tk <- calc(st, fun=function(z){ifelse(z[2]>=x,z[1],NA)})
    
    st <- stack(ba_tk,gw_t)
    
    tile <- calc(st, function(z) {ifelse(z[1] %in% c(1:4) & z[2] %in% c(1,2), 7,
                                  ifelse(z[1] %in% c(1:4) & z[2] == 3, 6, 
                                  ifelse(z[1] %in% c(1:4) & z[2] == 4, 5,
                                  ifelse(z[1] %in% c(1:4) & z[2] %in% c(5,6), 4,
                                  ifelse(z[1] %in% c(5:6) & z[2] == 1, 7,
                                  ifelse(z[1] %in% c(5:6) & z[2] == 2, 6, 
                                  ifelse(z[1] %in% c(5:6) & z[2] == 3, 5,
                                  ifelse(z[1] %in% c(5:6) & z[2] %in% c(4,5), 4,
                                  ifelse(z[1] %in% c(5:6) & z[2] == 6, 3,
                                  ifelse(z[1] == 7 & z[2] == 1, 7,
                                  ifelse(z[1] == 7 & z[2] == 2, 6,
                                  ifelse(z[1] == 7 & z[2] == 3, 5,
                                  ifelse(z[1] == 7 & z[2] == 4, 4,
                                  ifelse(z[1] == 7 & z[2] %in% c(5,6), 3,
                                  ifelse(z[1] %in% c(8:9) & z[2] == 1, 7,
                                  ifelse(z[1] %in% c(8:9) & z[2] == 2, 6,
                                  ifelse(z[1] %in% c(8:9) & z[2] == 3, 5,
                                  ifelse(z[1] %in% c(8:9) & z[2] == 4, 3,
                                  ifelse(z[1] %in% c(8:9) & z[2] %in% c(5,6), 2,
                                  ifelse(is.na(z[1]) | z[1]==10, 0,NA))))))))))))))))))))})
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("./ökoF/li_",x,"_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
  })
  
 })

free.kernel (cl)
}}) 


files <- list.files(path="./ökoF", pattern="li_", full.names = T)

if(length(files)==(hormax[1]*36)){break}

}

  cl <- makeCluster(hormax[1])
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
#~~~~~~~~~~~~~~~~~~~~~~~~~~

hor_nr <- parLapply(cl,c(hormax[1]), function(x){
  library(snow)
  library(parallel)
  library(doParallel)
  library(raster)
  library(SpaDES.tools)
  
  BL <- "SH" 
  
  cl <- makeCluster(8)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
 
    ba <- raster(paste(BL,"_BA_Hor",x,".tif", sep=""))
    humus <- raster(paste(BL,"_Humus_Hor",x,".tif", sep=""))
    ld <- raster(paste(BL,"_LD_Hor",x,".tif", sep=""))
    maecht <- raster(paste(BL,"_Mächtigkeit_Hor",x,".tif", sep=""))
  
    splitRaster(ba,4,4,path= "Split",fExt=".grd")
    splitRaster(humus,4,4,path= "Split",fExt=".grd")
    splitRaster(ld,4,4,path= "Split",fExt=".grd")
    splitRaster(maecht,4,4,path= "Split",fExt=".grd")
  
  
  li <- parLapply(cl,c(1,2,5,6,9,10,13,14), function(y) {
    library(raster)
    BL <- "SH" 
    ba_t <- raster(paste("./Split/",BL,"_BA_Hor",x,"_tile",y,".gri", sep=""))
    humus_t <- raster(paste("./Split/",BL,"_Humus_Hor",x,"_tile",y,".gri", sep=""))
    maecht_t <- raster(paste("./Split/",BL,"_Mächtigkeit_Hor",x,"_tile",y,".gri", sep=""))
    ewa_t <- raster(paste("./ökoF/li_",x,"_",y,".tif", sep=""))
    hor_nr_t <- raster(paste("./Split/ab_nr_hor_tile",y,".gri", sep=""))
    
    # wenn Horizont nicht nötig für Berechnung dann Bodenklasse auf NA setzen
    st <- stack(ba_t, hor_nr_t)
    ba_tk <- calc(st, fun=function(z){ifelse(z[2]>=x,z[1],NA)})
    
    st <- stack(ba_tk,humus_t,ewa_t,maecht_t)
    
    tile <- calc(st, function(z) {
      # Erosionswiderstand Humus < 4%
      ifelse(z[1] %in% c(13:19,35:48) & z[2] %in% c(0:3) & z[3] %in% c(1:4), 2*z[4],
      ifelse(z[1] %in% c(13:19,35:48) & z[2] %in% c(0:3) & z[3] %in% c(5:7), 1*z[4],
      ifelse(z[1] %in% c(26:28,30,31) & z[2] %in% c(0:3) & z[3] %in% c(1:3), 4*z[4],
      ifelse(z[1] %in% c(26:28,30,31) & z[2] %in% c(0:3) & z[3] == 4, 3*z[4],
      ifelse(z[1] %in% c(26:28,30,31) & z[2] %in% c(0:3) & z[3] == 5, 2*z[4],
      ifelse(z[1] %in% c(26:28,30,31) & z[2] %in% c(0:3) & z[3] %in% c(6,7), 1*z[4],
      ifelse(z[1] %in% c(25,32:34) & z[2] %in% c(0:3) & z[3] %in% c(1,2), 6*z[4],
      ifelse(z[1] %in% c(25,32:34) & z[2] %in% c(0:3) & z[3] == 3 , 5*z[4],
      ifelse(z[1] %in% c(25,32:34) & z[2] %in% c(0:3) & z[3] == 4, 4*z[4],
      ifelse(z[1] %in% c(25,32:34) & z[2] %in% c(0:3) & z[3] == 5 , 3*z[4],
      ifelse(z[1] %in% c(25,32:34) & z[2] %in% c(0:3) & z[3] %in% c(6,7), 1*z[4],
      ifelse(z[1] %in% c(20:22,29,6:8) & z[2] %in% c(0:3) & z[3] %in% c(1,3), 6*z[4],
      ifelse(z[1] %in% c(20:22,29,6:8) & z[2] %in% c(0:3) & z[3] == 4, 5*z[4],
      ifelse(z[1] %in% c(20:22,29,6:8) & z[2] %in% c(0:3) & z[3] == 5, 4*z[4],
      ifelse(z[1] %in% c(20:22,29,6:8) & z[2] %in% c(0:3) & z[3] %in% c(6,7), 1*z[4],
      # Erosionswiderstand Humus > 4%   
      ifelse(z[1] %in% c(13:19,35:48) & z[2] %in% c(4:7) & z[3] %in% c(1:4), 2*z[4],
      ifelse(z[1] %in% c(13:19,35:48) & z[2] %in% c(4:7) & z[3] %in% c(5:7), 1*z[4],
      ifelse(z[1] %in% c(26:28,30,31) & z[2] %in% c(4:7) & z[3] %in% c(1:3), 4*z[4],
      ifelse(z[1] %in% c(26:28,30,31) & z[2] %in% c(4:7) & z[3] %in% c(4,5), 3*z[4],
      ifelse(z[1] %in% c(26:28,30,31) & z[2] %in% c(4:7) & z[3] %in% c(6,7), 1*z[4],
      ifelse(z[1] %in% c(25,32:34) & z[2] %in% c(4:7) & z[3] %in% c(1,2), 6*z[4],
      ifelse(z[1] %in% c(25,32:34) & z[2] %in% c(4:7) & z[3] %in% c(3,4) , 5*z[4],
      ifelse(z[1] %in% c(25,32:34) & z[2] %in% c(4:7) & z[3] == 4 , 4*z[4],
      ifelse(z[1] %in% c(25,32:34) & z[2] %in% c(4:7) & z[3] %in% c(6,7), 1*z[4],
      ifelse(z[1] %in% c(20:22,29,6:8) & z[2] %in% c(4:7) & z[3] %in% c(1,4), 6*z[4],
      ifelse(z[1] %in% c(20:22,29,6:8) & z[2] %in% c(4:7) & z[3] == 5, 5*z[4],
      ifelse(z[1] %in% c(20:22,29,6:8) & z[2] %in% c(4:7) & z[3] %in% c(6,7), 1*z[4],NA)))))))))))))))))))))))))))})
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("eros_w_oM_",x,"_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
  })})

free.kernel (cl)

# Moore

hor_nr <- parLapply(cl,c(1:hormax[1]), function(x){
  library(snow)
  library(parallel)
  library(doParallel)
  library(raster)
  library(SpaDES.tools)
  
  BL <- "SH" 
  
  cl <- makeCluster(8)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
  li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "SH" 
    ba_t <- raster(paste("./Split/",BL,"_BA_Hor",x,"_tile",y,".gri", sep=""))
    maecht_t <- raster(paste("./Split/",BL,"_Mächtigkeit_Hor",x,"_tile",y,".gri", sep=""))
    hor_nr_t <- raster(paste("./Split/ab_nr_hor_tile",y,".gri", sep=""))
    
    
    # wenn Horizont nicht nötig für Berechnung dann Bodenklasse auf NA setzen
    st <- stack(ba_t, hor_nr_t)
    ba_tk <- calc(st, fun=function(z){ifelse(z[2]>=x,z[1],NA)})
    
    st <- stack(ba_tk,maecht_t)
    ras <- calc(st, function(z) ifelse(z[1] %in% c(7:10), 3*z[2], NA))
    # alles verbinden
    eros_t <- raster(paste("eros_w_oM_",x,"_",y,".tif", sep=""))
    st <- stack (eros_t, ras)
    
    tile <- calc(st, function(z) ifelse(is.na(z[1]), z[2], z[1]))
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",x,"_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })
  
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste("eros_w_hor_",x,".tif",sep=""), format="GTiff", overwrite=TRUE)
  
})

free.kernel (cl)

# gewichtetes Mittel

files <- list.files(path=".", pattern="eros_w_hor_", full.names = T)

for(i in 1:length(files)){ 
splitRaster(raster(files[i]),4,4,path= "Split",fExt=".grd")} 

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    hor_nr <- raster("ab_nr_hor.tif")
    hormax <- max(values(hor_nr),na.rm=T) # benötigte Anzahl an Horizonten für Berechnung
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
    
    BL <- "SH" 
    
    files <- list.files(path="./Split", pattern=paste("eros_w_hor_._tile",y,".gri", sep=""), full.names = T)

    hor_t <- stack(lapply(files, raster))

    gm_t <- raster(paste("./Split/ab_GesMae_tile",y,".gri", sep=""))
    hor_t <- resample(hor_t, gm_t, method="ngb")
    hor_st <- stack(hor_t, gm_t)
    tile <- calc(hor_st, fun=function(x) {round2(sum(x[1:hormax[1]], na.rm=T)/(x[hormax[1]+1]*10))}) # gewichtetes Mittel
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
 })   
  #li <- lapply(list.files(path=".", pattern="li_"), raster)  
ras <- mergeRaster(li) 
    
 #Umkehren der Bewertung
recl <- matrix(data=NA,7,2)
recl[,1] <- c(0:6)
recl[,2] <- c(NA,6,5,4,3,2,1)
ras <- reclassify(ras,recl,right=NA)
    
    crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(ras,filename=paste(BL,"_e_wind_2018.tif",sep=""), format="GTiff", overwrite=TRUE)

free.kernel (cl)    
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Filter-, Puffer- und Tranformatorfunktion 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Tiefe der GW-Oberfläche
# --> Bewertung erfolgt nur bis zur GW-Oberfläche

bük<- fread("../GW_Abstand_BÜK1000.csv")
bük <- bük[,max(GW),by=BF_ID] # maximale Tiefe je BF_ID
bük[is.na(V1), V1:=200] # wenn NA auf maximale Tiefe (2m) setzen

gw <- merge(bük, dat, by="BF_ID")
gw <- gw[UTIEF*10<=V1]

buk200 <- st_read(paste(BL,"_key_BF_ID.shp",sep=""))
# Einladen Basisraster
key <- raster(paste(BL,"_key_BF_ID.tif",sep=""))

# Gesamtmächtigkeit als Raster
GesMae <- gw[, sum(MAECHT), by=BF_ID]
GesMae.sf <- merge(buk200, GesMae, by="BF_ID")
GesMae.raster <- fasterize(GesMae.sf, key, field="V1")
writeRaster(GesMae.raster, filename="filter_GesMae.tif", format="GTiff", overwrite=TRUE)

# wie viele Horizonte werden benötigt für Berechnung?
nr_hor <- gw[, max(NR), by=BF_ID]
nr_hor.sf <- merge(buk200, nr_hor, by="BF_ID")
nr_hor.raster <- fasterize(nr_hor.sf, key, field="V1")
writeRaster(nr_hor.raster, filename="filter_nr_hor.tif", format="GTiff", overwrite=TRUE)
hormax <- max(values(nr_hor.raster),na.rm=T) # benötigte Anzahl an Horizonten für Berechnung

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# mechanische Filtereigenschaften
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Bewertung der mechanischen Filtereigenschaften

repeat{ 

files <- list.files(path=".", pattern="mF_hor_", full.names = T)
try({
if( length(files) <hormax[1]){ 
  if(length(files)>0){  
  ni <- lapply(files, function(x) gsub("([0-9]+).*$","\\1",strsplit(x,"_" )[[1]][4] ))
  niv <- as.integer(unlist(ni))
  horm <- c(1:hormax[1])
  horm <-horm[-niv] } else{
   horm <- c(1:hormax[1])  
  } 
  
  # Berechnung wiederholen
  
  
  cl <- makeCluster(hormax[1])
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

hor_nr <- parLapply(cl, horm, function(x){
  library(snow)
  library(parallel)
  library(doParallel)
  library(raster)
  library(SpaDES.tools)
  
  BL <- "SH" 
  
  BK <- raster(paste(BL,"_Boklasse_Hor",x,".tif", sep=""))
  maecht <- raster(paste(BL,"_Mächtigkeit_Hor",x,".tif", sep=""))
  nr_hor <- raster("filter_nr_hor.tif")
  LD <- raster(paste(BL,"_LD_Hor",x,".tif", sep=""))
  #
  splitRaster(BK,4,4,path= "./Split")
  splitRaster(maecht,4,4,path= "./Split")
  splitRaster(nr_hor,4,4,path= "./Split")
  splitRaster(LD,4,4,path= "./Split")
  
  cl <- makeCluster(4)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
  li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "SH" 
    BK_t <- raster(paste("./Split/",BL,"_Boklasse_Hor",x,"_tile",y,".gri", sep=""))
    maecht_t <- raster(paste("./Split/",BL,"_Mächtigkeit_Hor",x,"_tile",y,".gri", sep=""))
    LD_t <- raster(paste("./Split/",BL,"_LD_Hor",x,"_tile",y,".gri", sep=""))
    hor_nr_t <- raster(paste("./Split/filter_nr_hor_tile",y,".gri", sep=""))
    
    # wenn Horizont nicht nötig für Berechnung dann Bodenklasse auf NA setzen
    st <- stack(BK_t, hor_nr_t)
    BK_tk <- calc(st, fun=function(z){ifelse(z[2]>=x,z[1],NA)})
    
    st <- stack(BK_tk,LD_t,maecht_t)
    tile <- calc(st, function(x) ifelse(x[2] %in% c(3,4,5,7,8,9),ifelse(x[1]==1, 2*x[3], ifelse(x[1] %in% c(2,3,4), 3*x[3], ifelse(x[1] %in% c(5,6,7), 4*x[3], ifelse(x[1] %in% c(8,10), 5*x[3], ifelse(x[1]==9, 1*x[3],NA))))),
                                        ifelse(x[2] %in% c(1,2,6),ifelse(x[1]==1, 2*x[3], ifelse(x[1] %in% c(2:7,8), 4*x[3], ifelse(x[1] ==10, 3*x[3], ifelse(x[1] ==9, 1*x[3],NA)))),NA)))
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",x,"_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })
  #li <- lapply(list.files(path=".", pattern="li_3_"), raster) 
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_mF_hor_",x,".tif",sep=""), format="GTiff", overwrite=TRUE)
  
})

free.kernel (cl)
}} )
files <- list.files(path=".", pattern="mF_hor_", full.names = T)

if(length(files)==hormax[1]){break} 
}

# gewichtetes Mittel
#Gesamtmächtigkeit
GM <- raster("filter_GesMae.tif")
splitRaster(GM,4,4,path= "Split")

files <- list.files(path=".", pattern="mF_hor_", full.names = T)

for(i in 1:length(files)){ 
splitRaster(raster(files[i]),4,4,path= "Split")} 

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    hor_nr <- raster("filter_nr_hor.tif")
    hormax <- max(values(hor_nr),na.rm=T) # benötigte Anzahl an Horizonten für Berechnung
  
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
    
    BL <- "SH" 
    # alle Horizonte einladen und stapeln
    files <- list.files(path="./Split", pattern=paste("_mF_hor_.+_tile",y,".gri", sep=""), full.names = T)

    hor_t <- stack(lapply(files, raster))

    gm_t <- raster(paste("./Split/filter_GesMae_tile",y,".gri", sep=""))
    
    hor_st <- stack(hor_t, gm_t)
    tile <- calc(hor_st, fun=function(x) {round2(sum(x[1:hormax[1]], na.rm=T)/(x[hormax[1]+1]))}) # gewichtetes Mittel
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
 })   
    ras <- mergeRaster(li) 
    crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(ras,filename=paste(BL,"_mF_mittel.tif",sep=""), format="GTiff", overwrite=TRUE)
    
free.kernel (cl)    
    

# Zu- und Abschläge über Länge der Filterstrecke
mgw <- raster(paste(BL,"_GW_Abstand_BÜK1000.tif",sep=""))
splitRaster(mgw,4,4,path= "./Split")

mittel <- raster(paste(BL,"_mF_mittel.tif",sep=""))

splitRaster(mittel,4,4,path= "./Split")

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
  BL <- "SH" 
    mgw_t <- raster(paste("./Split/",BL,"_GW_Abstand_BÜK1000_tile",y,".gri", sep=""))
    mittel_t <- raster(paste("./Split/",BL,"_mF_mittel_tile",y,".gri", sep=""))

    mgw_st <- stack(mittel_t,mgw_t)

		tile <- calc(mgw_st, fun=function(x) { ifelse (x[2]<80,x[1]-1,
                                                ifelse (x[2]>1000 & x[2]<3000,x[1]+1,
                                                        ifelse (x[2]>=3000,x[1]+2,x[1])))})
		
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })

 # li <- lapply(list.files(path=".", pattern="li_"), raster) 
  zus_f <- mergeRaster(li) 
crs(zus_f) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(zus_f,filename=paste(BL,"_mF_zus_f.tif",sep=""), format="GTiff", overwrite=TRUE)

free.kernel (cl)

# Zuschläge wenn klimatischer Wasserbilanzüberschuss < 300 mm

klim_wb <- raster(paste(BL,"_klim_wb_10.tif",sep=""))
zus_f <- raster(paste(BL,"_mF_zus_f.tif",sep=""))
splitRaster(klim_wb,4,4,path= "./Split")
splitRaster(zus_f,4,4,path= "./Split")

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
  BL <- "SH" 
    klim_wb_t <- raster(paste("./Split/",BL,"_klim_wb_10_tile",y,".gri", sep=""))
    zus_f_t <- raster(paste("./Split/",BL,"_mF_zus_f_tile",y,".gri", sep=""))

	klim_wb_st <- stack(klim_wb_t,zus_f_t)

		tile <- calc(klim_wb_st, fun=function(x) {ifelse (x[1]<300, x[2]+1, x[2])}) 
		    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
		    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
		    
  })

  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_mF_klimWB.tif",sep=""), format="GTiff", overwrite=TRUE)
  
free.kernel (cl)  
  
  # keine Bewertung >5
  f_wb <- raster(paste(BL,"_mF_klimWB.tif",sep=""))
  splitRaster(f_wb,4,4,path= "./Split")
  
    cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
  li <- parLapply(cl,c(1:16), function(y) {
  	library(raster)
    BL <- "SH" 
    f_wb_t <- raster(paste("./Split/",BL,"_mF_klimWB_tile",y,".gri", sep=""))
		tile <- calc(f_wb_t, fun=function(x) { ifelse (x<=0,1,ifelse(x>5,5,x))})
		crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
		writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
		 })
  
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_MF_2018.tif",sep=""), format="GTiff", overwrite=TRUE)
  
free.kernel (cl)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# physiko-chemische Filtereigenschaften
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

repeat{ 

files <- list.files(path=".", pattern="cF_hor_", full.names = T)

try({
if( length(files) <hormax[1]){ 
  if(length(files)>0){  
  ni <- lapply(files, function(x) gsub("([0-9]+).*$","\\1",strsplit(x,"_" )[[1]][4] ))
  niv <- as.integer(unlist(ni))
  horm <- c(1:hormax[1])
  horm <-horm[-niv] } else{
   horm <- c(1:hormax[1])  
  } 
  
  # Berechnung wiederholen
  
    cl <- makeCluster(hormax[1])
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

hor_nr <- parLapply(cl, horm, function(x){
  library(snow)
  library(parallel)
  library(doParallel)
  library(raster)
  library(SpaDES.tools)
  
  BL <- "SH" 
  
  BK <- raster(paste(BL,"_Boklasse_Hor",x,".tif", sep=""))
  maecht <- raster(paste(BL,"_Mächtigkeit_Hor",x,".tif", sep=""))
  nr_hor <- raster("filter_nr_hor.tif")
  
  splitRaster(BK,4,4,path= "./Split",fExt=".grd")
  splitRaster(maecht,4,4,path= "./Split",fExt=".grd")
  splitRaster(nr_hor,4,4,path= "./Split",fExt=".grd")
  
  cl <- makeCluster(4)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
  li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "SH" 
    BK_t <- raster(paste("./Split/",BL,"_Boklasse_Hor",x,"_tile",y,".gri", sep=""))
    maecht_t <- raster(paste("./Split/",BL,"_Mächtigkeit_Hor",x,"_tile",y,".gri", sep=""))
    hor_nr_t <- raster(paste("./Split/filter_nr_hor_tile",y,".gri", sep=""))
    
    # wenn Horizont nicht nötig für Berechnung dann Bodenklasse auf NA setzen
    st <- stack(BK_t, hor_nr_t)
    BK_tk <- calc(st, fun=function(z){ifelse(z[2]>=x,z[1],NA)})
    
    st <- stack(BK_tk,maecht_t)
    e <- calc(st, function(x) ifelse(x[1]==9, 1*x[2], ifelse(x[1]==8, 2*x[2], ifelse(x[1] %in% c(6,7,10), 3*x[2], ifelse(x[1] %in% c(2:5), 4*x[2], ifelse(x[1]==1, 5*x[2], NA))))))
    crs(e) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(e, filename=paste("li_",x,"_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })
  #li <- lapply(list.files(path=".", pattern=paste("li_",x,"_",sep="")), raster)
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_cF_hor_",x,".tif",sep=""), format="GTiff", overwrite=TRUE)
  
})

free.kernel (cl)
}}) 
files <- list.files(path=".", pattern="cF_hor_", full.names = T)

if(length(files)==hormax[1]){break} 
}

# gewichtetes Mittel

files <- list.files(path=".", pattern="cF_hor_", full.names = T)

for(i in 1:length(files)){ 
splitRaster(raster(files[i]),4,4,path= "Split")} 

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    hor_nr <- raster("filter_nr_hor.tif")
    hormax <- max(values(hor_nr),na.rm=T) # benötigte Anzahl an Horizonten für Berechnung
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
    
    BL <- "SH" 
    # alle Horizonte einladen und stapeln
    files <- list.files(path="./Split", pattern=paste("_cF_hor_.+_tile",y,".gri", sep=""), full.names = T)

    hor_t <- stack(lapply(files, raster))

    gm_t <- raster(paste("./Split/filter_GesMae_tile",y,".gri", sep=""))
    
    hor_st <- stack(hor_t, gm_t)
    tile <- calc(hor_st, fun=function(x) {round2(sum(x[1:hormax[1]], na.rm=T)/(x[hormax[1]+1]))}) # gewichtetes Mittel
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
 })   
    ras <- mergeRaster(li) 
    crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(ras,filename=paste(BL,"_cF_mittel.tif",sep=""), format="GTiff", overwrite=TRUE)
    
free.kernel (cl)

# Zu- und Abschläge über Länge der Filterstrecke

mittel <- raster(paste(BL,"_cF_mittel.tif",sep=""))
splitRaster(mittel,4,4,path= "./Split")

 cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
  BL <- "SH" 
    mgw_t <- raster(paste("./Split/",BL,"_GW_Abstand_BÜK1000_tile",y,".gri", sep=""))
    mittel_t <- raster(paste("./Split/",BL,"_cF_mittel_tile",y,".gri", sep=""))

    mgw_st <- stack(mittel_t,mgw_t)

		tile <- calc(mgw_st, fun=function(x) { ifelse(x[2]<80,x[1]-2,
                                               ifelse (x[2]>80 & x[2]<200,x[1]-1,
                                                       ifelse (x[2]>1000 & x[2]<3000,x[1]+1,
                                                               ifelse (x[2]>=3000,x[1]+2,x[1]))))}) 
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })

  
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_pcF_zus_f.tif",sep=""), format="GTiff", overwrite=TRUE)

free.kernel (cl)

# wenn Bewertung 0 auf 1 setzen
# keine Bewertung >5
  
  zus_f <- raster(paste(BL,"_pcF_zus_f.tif",sep=""))
  splitRaster(zus_f,4,4,path= "./Split")
  
   cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
  li <- parLapply(cl,c(1:16), function(y) {
  	library(raster)
    BL <- "SH" 
    zus_f <- raster(paste("./Split/",BL,"_pcF_zus_f_tile",y,".gri", sep=""))
		tile <- calc(zus_f, fun=function(x) { ifelse (x<=0,1,ifelse(x>5,5,x))})
		crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
		writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
		 })
  
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_pcF_2018.tif",sep=""), format="GTiff", overwrite=TRUE)
  
free.kernel (cl)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Biotisches Ertragspotential - Gründigkeit, Frost, Überschwemmung fehlt
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Einladen Nutzung
nutz<-raster(paste(BL,"_Nutzung_2018.tif",sep="")) # Acker = 1, GL = 2

splitRaster(nutz,4,4,path= "Split")

# Gesamtmächtigkeit von mind. 30 cm als Raster
# wenn Horizont flacher als 30 cm wird darunter liegender Horizont hinzugezogen (HOR_NR
flach <- merge(dat[OTIEF==0 & UTIEF<3,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR")) # merge damit UTIEF von darunter liegendem Horizont verwendet wird
# wenn immer noch flacher als 30 cm wird nochmals darunter liegender Horizont hinzugezogen (HOR_NR+1)
flach2 <- merge(flach[UTIEF<3,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
# wenn immer noch flacher als 30 cm wird nochmals darunter liegender Horizont hinzugezogen (HOR_NR+1)
flach3 <- merge(flach2[UTIEF<3,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
# wenn immer noch flacher als 30 cm wird nochmals darunter liegender Horizont hinzugezogen (HOR_NR+1)
flach4 <- merge(flach3[UTIEF<3,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
#flach5 <- merge(flach4[UTIEF<3,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))

oben <- merge(dat[OTIEF==0,BF_ID, HOR_NR],dat,by=c("BF_ID", "HOR_NR"))

# alle Oberbodenhorizonte zusammen
ob <- rbindlist(list(oben,flach,flach2,flach3,flach4))

# Einladen Basisraster
buk200 <- st_read(paste(BL,"_key_BF_ID.shp",sep=""))
key <- raster(paste(BL,"_key_BF_ID.tif",sep=""))

# Gesamtmächtigkeit als Raster
GesMae <- ob[, sum(UTIEF-OTIEF), by=BF_ID]
GesMae.sf <- merge(buk200, GesMae, by="BF_ID")
GesMae.raster <- fasterize(GesMae.sf, key, field="V1")
writeRaster(GesMae.raster, filename="be_GesMae.tif", format="GTiff", overwrite=TRUE)

# benötigte Anzahl an Horizonten für Berechnung
nr_hor <- ob[, max(NR), by=BF_ID]
nr_hor.sf <- merge(buk200, nr_hor, by="BF_ID")
nr_hor.raster <- fasterize(nr_hor.sf, key, field="V1")
writeRaster(nr_hor.raster, filename="be_nr_hor.tif", format="GTiff", overwrite=TRUE)

hormax <- max(values(nr_hor.raster),na.rm=T) # benötigte Anzahl an Horizonten für Berechnung

hor_nr <- raster("be_nr_hor.tif")
splitRaster(hor_nr,4,4,path="Split")

  cl <- makeCluster(hormax[1])
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

# b) Skelettgehalt

hor_nr <- parLapply(cl,c(1:hormax[1]), function(x){
  library(snow)
  library(parallel)
  library(doParallel)
  library(raster)
  library(SpaDES.tools)
  
  cl <- makeCluster(8)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
  BL <- "SH" 
  
  # Unterteilen der Raster je Horizont
  skel <- raster(paste(BL,"_Skelett_Hor",x,".tif", sep=""))
  #maecht <- raster(paste("Maechtigkeit_Hor",x,".tif", sep=""))
  splitRaster(skel,4,4,path= "Split")
  #splitRaster(maecht,4,4,path= "Split")
  
  li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
  	BL <- "SH" 
    skel_t <- raster(paste("./Split/",BL,"_Skelett_Hor",x,"_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))
    maecht_t <- raster(paste("./Split/",BL,"_Mächtigkeit_Hor",x,"_tile",y,".gri", sep=""))
    hor_nr_t <- raster(paste("./Split/be_nr_hor_tile",y,".gri", sep=""))
    
    # wenn Horizont nicht nötig für BEP Berechnung dann Skelett auf NA setzen
    # zu berechnender Horizont muss kleinergleich der maximalen Horizontnummer sein
    st <- stack(skel_t, hor_nr_t)
    skel_tk <- calc(st, fun=function(z){ifelse(z[2]>=x,z[1],NA)})
    
    st <- stack(skel_tk,nutz_t,maecht_t)
    tile <- calc(st, fun=function(z) {ifelse(z[1]==1 & z[2] %in% c(1,2), 5*z[3], 
                                             ifelse(z[1]==10 & z[2]==1, 4*z[3],
                                                    ifelse(z[1]==10 & z[2]==2, 5*z[3],
                                                           ifelse(z[1]==30 & z[2]==1, 3*z[3],
                                                                  ifelse(z[1]==30 & z[2]==2, 4*z[3],
                                                                         ifelse(z[1] %in% c(50,75) & z[2]==1, 1*z[3],
                                                                                ifelse(z[1] %in% c(50,75) & z[2]==2, 3*z[3],
                                                                                       ifelse(z[1]==100 & z[2]==1, 0*z[3],
                                                                                              ifelse(z[1]==100 & z[2]==1, 1*z[3],NA)))))))))})
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",x,"_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
  })
  #li <- lapply(list.files(path=".", pattern=paste("li_",x,"_", sep="")), raster) 
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_beSkel_hor_",x,".tif",sep=""), format="GTiff", overwrite=TRUE)
  
})

free.kernel (cl)

# gewichtetes Mittel

files <- list.files(path=".", pattern="beSkel_hor_", full.names = T)

for(i in 1:length(files)){ 
splitRaster(raster(files[i]),4,4,path= "Split")} 

#Gesamtmächtigkeit
GM <- raster("be_GesMae.tif")
splitRaster(GM,4,4,path= "Split")

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    hormax<-4
  
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
    
    BL <- "SH" 
    # alle Horizonte einladen und stapeln
    files <- list.files(path="./Split", pattern=paste("beSkel_hor_._tile",y,".gri", sep=""), full.names = T)

    hor_t <- stack(lapply(files, raster))

    gm_t <- raster(paste("./Split/be_GesMae_tile",y,".gri", sep=""))
    
    hor_st <- stack(hor_t, gm_t)
    tile <- calc(hor_st, fun=function(x) {round2(sum(x[1:hormax[1]], na.rm=T)/(x[hormax[1]+1]*10))}) # gewichtetes Mittel
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
 })   
      ras <- mergeRaster(li) 
    crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(ras,filename=paste(BL,"_be_skel.tif",sep=""), format="GTiff", overwrite=TRUE)

free.kernel (cl)
    
# d) Bodenart des OB

  cl <- makeCluster(hormax[1])
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

hor_nr <- parLapply(cl,c(1:hormax[1]), function(x){
  library(snow)
  library(parallel)
  library(doParallel)
  library(raster)
  library(SpaDES.tools)
  
  cl <- makeCluster(4)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
  BL <- "SH" 
  
  #bok <- raster(paste("Boklasse_Hor",x,".tif", sep=""))
  #maecht <- raster(paste("Maechtigkeit_Hor",x,".tif", sep=""))
  #splitRaster(bok,4,4,path= "Split")
  #splitRaster(maecht,4,4,path= "Split")
  
  li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "SH" 
    bok_t <- raster(paste("./Split/",BL,"_Boklasse_Hor",x,"_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))
    maecht_t <- raster(paste("./Split/",BL,"_Mächtigkeit_Hor",x,"_tile",y,".gri", sep=""))
    hor_nr_t <- raster(paste("./Split/be_nr_hor_tile",y,".gri", sep=""))
    
    # wenn Horizont nicht nötig für BEP Berechnung dann Bodenklasse auf NA setzen
    st <- stack(bok_t, hor_nr_t)
    bok_tk <- calc(st, fun=function(z){ifelse(z[2]>=x,z[1],NA)})
    
    st <- stack(bok_tk,nutz_t,maecht_t)
    tile <- calc(st, fun=function(z) {ifelse(z[1] %in% c(3:7) & z[2] %in% c(1,2), 5*z[3], 
                                             ifelse(z[1]==8 & z[2] %in% c(1,2), 4*z[3],
                                                    ifelse(z[1] %in% c(1,2,10) & z[2] %in% c(1,2), 3*z[3],
                                                           ifelse(z[1]==9 & z[2]==1, 0*z[3],
                                                                  ifelse(z[1]==9 & z[2]==2, 1*z[3],NA)))))})
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",x,"_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
  })
  
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_beBAOB_hor_",x,".tif",sep=""), format="GTiff", overwrite=TRUE)
  
})

free.kernel (cl)

# gewichtetes Mittel

files <- list.files(path=".", pattern="beBAOB_hor_", full.names = T)

for(i in 1:length(files)){ 
splitRaster(raster(files[i]),4,4,path= "Split")} 

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
        hor_nr <- raster("be_nr_hor.tif")
    hormax <- max(values(hor_nr),na.rm=T) # benötigte Anzahl an Horizonten für Berechnung
    
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
    
    BL <- "SH" 
    # alle Horizonte einladen und stapeln
    files <- list.files(path="./Split", pattern=paste("beBAOB_hor_._tile",y,".gri", sep=""), full.names = T)

    hor_t <- stack(lapply(files, raster))

    gm_t <- raster(paste("./Split/be_GesMae_tile",y,".gri", sep=""))
    
    hor_st <- stack(hor_t, gm_t)
    tile <- calc(hor_st, fun=function(x) {round2(sum(x[1:hormax[1]], na.rm=T)/(x[hormax[1]+1]*10))}) # gewichtetes Mittel
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
 })   
    ras <- mergeRaster(li) 
    crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(ras,filename=paste(BL,"_be_BAOB.tif",sep=""), format="GTiff", overwrite=TRUE)

free.kernel (cl)    
    
# e) N?hrstoffangebot Tab.8 KA G?K 25 S. 85

# S-WERT!!!

 cl <- makeCluster(hormax[1])
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

hor_nr <- parLapply(cl,c(1:hormax[1]), function(x){
  library(snow)
  library(parallel)
  library(doParallel)
  library(raster)
  library(SpaDES.tools)
  
  cl <- makeCluster(4)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
  BL <- "SH" 
  
  ba <- raster(paste(BL,"_BA_Hor",x,".tif", sep=""))
  #maecht <- raster(paste(BL,"_Mächtigkeit_Hor",x,".tif", sep=""))
  ph <- raster(paste(BL,"_pH_Hor",x,".tif", sep=""))
  splitRaster(ba,4,4,path= "Split")
  splitRaster(ph,4,4,path= "Split")
  #splitRaster(maecht,4,4,path= "Split")
  
  li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "SH" 
    ba_t <- raster(paste("./Split/",BL,"_BA_Hor",x,"_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))
    ph_t <- raster(paste("./Split/",BL,"_pH_Hor",x,"_tile",y,".gri", sep=""))
    maecht_t <- raster(paste("./Split/",BL,"_Mächtigkeit_Hor",x,"_tile",y,".gri", sep=""))
    hor_nr_t <- raster(paste("./Split/be_nr_hor_tile",y,".gri", sep=""))
    
    # wenn Horizont nicht nötig für BEP Berechnung dann Bodenklasse auf NA setzen
    st <- stack(ba_t, hor_nr_t)
    ba_tk <- calc(st, fun=function(z){ifelse(z[2]>=x,z[1],NA)})
    
     # wenn Horizont nicht nötig für BEP Berechnung dann pH auf NA setzen
    st <- stack(ph_t, hor_nr_t)
    ph_tk <- calc(st, fun=function(z){ifelse(z[2]>=x,z[1],NA)})
    
    tile <- calc(ba_tk, fun=function(z) {ifelse(z %in% c(6:12,20:22), 1,  #sehr gering ss
                                      ifelse(z %in% c(32:34,25,26), 2,  # us, ls
                                      ifelse(z %in% c(44,48,28,27,31,13:15), 3,  # us, ls, su, sl
                                      ifelse(z %in% c(45:47,43,19), 4,    # lu, sl 
                                      ifelse(z %in% c(16:18,36:38,40:42,35,39), 5, NA)))))}) # tl, tu, lt, ut
    st <- stack(tile, ph_tk)
       tile2 <- calc(st, fun=function(z) {ifelse(is.na(z[2]),z[1],
        															ifelse(z[2] %in% c(11,12), 1,  #s5 s6
                                      ifelse(z[2] ==10, 2,  # s4
                                      ifelse(z[2] ==9, 3,  # s3
                                      ifelse(z[2] %in% c(7,8),4,   # s2 s1
                                      ifelse(z[2] %in% c(1:6), 5, # a0 s0
                                      ))))))})
    
    st <- stack(tile2,nutz_t,maecht_t)
    tile3 <- calc(st, fun=function(z){ifelse(z[2]==2 & z[1]<5, (z[1]+1)*z[3], z[1]*z[3])})
    
    crs(tile2) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
   writeRaster(tile3, filename=paste("li_",x,"_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    })
    #li <- lapply(list.files(path=".", pattern=paste("li_",x,"_",sep="")), raster)
    ras <- mergeRaster(li) 
    crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(ras, filename=paste(BL,"_beNaehr_hor_",x,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })

free.kernel (cl)

# gewichtetes Mittel

files <- list.files(path=".", pattern="beNaehr_hor_", full.names = T)

for(i in 1:length(files)){ 
splitRaster(raster(files[i]),4,4,path= "Split")} 

  cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    hor_nr <- raster("be_nr_hor.tif")
    hormax <- max(values(hor_nr),na.rm=T) # benötigte Anzahl an Horizonten für Berechnung
  
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
    
    BL <- "SH" 
    # alle Horizonte einladen und stapeln
    files <- list.files(path="./Split", pattern=paste("beNaehr_hor_._tile",y,".gri", sep=""), full.names = T)

    hor_t <- stack(lapply(files, raster))

    gm_t <- raster(paste("./Split/be_GesMae_tile",y,".gri", sep=""))
    
    hor_st <- stack(hor_t, gm_t)
    tile <- calc(hor_st, fun=function(x) {round2(sum(x[1:hormax[1]], na.rm=T)/(x[hormax[1]+1]*10))}) # gewichtetes Mittel
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
 })   
    ras <- mergeRaster(li) 
    crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(ras,filename=paste(BL,"_be_Naehr.tif",sep=""), format="GTiff", overwrite=TRUE)
  
 free.kernel (cl)
 
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ab hier ist Horizont unwichtig, nur Standortinformationen
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  
  # a) Hangneigung
  
  # Einladen von Neigung als Rasterdaten
    slope_r<-raster(paste(BL,"_Slope_10.tif",sep=""))
  
  # Gruppierung der Neigungsklassen nach Tab 58 a)
  recl <- matrix(data=NA,11,2)
  recl[,1] <- c(1:11)
  recl[,2] <- c(1,1,1,2,2,2,3,3,4,4,5)
  slope_rc <- reclassify(slope_r,recl,right=NA)
  
  writeRaster(slope_rc,paste(BL,"_slope_10_bep.tif",sep=""))
  slope_rc <- raster(paste(BL,"_slope_10_bep.tif",sep=""))
  splitRaster(slope_rc,4,4,path= "Split")
  
     cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
  
  li <- parLapply(cl, c(1:16), function(y) {
    library(raster)
    
    BL <- "SH" 
    
    slope_t <- raster(paste("./Split/",BL,"_slope_10_bep_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))
    
    # Stacking der Rasterdaten
    st <- stack(nutz_t,slope_t)
    # Bestimmung des Wertstufe
    tile <- calc(st, fun=function(x) ifelse(x[1] %in% c(1,2) & x[2]==1,5,
                                     ifelse(x[1]==1 & x[2]==2 ,4, ifelse(x[1]==2 & x[2]==2 ,5,
                                     ifelse(x[1]==1 & x[2]==3, 2,ifelse(x[1]==2 & x[2]==3 ,4,
                                     ifelse(x[1]==1 & x[2]==4 ,0,ifelse(x[1]==2 & x[2]==4 ,2,
                                     ifelse(x[1]==1 & x[2]==5 ,0, ifelse(x[1]==2 & x[2]==5 ,1, NA))))))))))
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
  })
  
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_be_neig.tif", sep=""), format="GTiff", overwrite=TRUE)
  
free.kernel (cl)
  
  # f) GW-Flurabstand
  
  gw <- raster(paste(BL,"_GW_Abstand_BÜK1000.tif", sep=""))
  splitRaster(gw,4,4,path= "Split")
  
    cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
  
  li <- parLapply(cl, c(1:16), function(y) {
    library(raster)
    BL <- "SH" 
    
    gw_t <- raster(paste("./Split/",BL,"_GW_Abstand_BÜK1000_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))
    
    st <- stack(gw_t,nutz_t)
    tile <- calc(st, fun=function(z) {ifelse(z[2] %in% c(1,2) & z[1] >130, 5,
                                      ifelse(z[2] ==1 & z[1]<130 & z[1] >80, 4,
                                      ifelse(z[2] ==2 & z[1]<130 & z[1] >80, 5,
                                      ifelse(z[2] ==1 & z[1]<80 & z[1] >40, 3,
                                      ifelse(z[2] ==2 & z[1]<80 & z[1] >40, 4,
                                      ifelse(z[2] ==1 & z[1]<40 & z[1] >20, 1,
                                      ifelse(z[2] ==2 & z[1]<40 & z[1] >20, 3,
                                      ifelse(z[2] ==1 & z[1]<20, 0,
                                      ifelse(z[2] ==2 & z[1]<20, 1,NA)))))))))}) 
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
  })
  
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_be_GW.tif",sep=""), format="GTiff", overwrite=TRUE)

free.kernel (cl)  
    
  # g) Staunässe
  
  stau <- raster(paste(BL,"_Staunass.tif",sep=""))
  splitRaster(stau,4,4,path= "Split")
  
    cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
    
  li <- parLapply(cl, c(1:16), function(y) {
    library(raster)
    BL <- "SH" 
    stau_t <- raster(paste("./Split/",BL,"_Staunass_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))
    
    st <- stack(stau_t,nutz_t)
    tile <- calc(st, function (z) { ifelse(z[2] %in% c(1,2) & z[1] ==4,5,
                                    ifelse(z[2] ==1 & z[1]==3, 4, ifelse(z[2] ==2 & z[1]==3, 5,
                                    ifelse(z[2] ==1 & z[1]==2, 3, ifelse(z[2] ==2 & z[1]==2, 4,
                                    ifelse(z[2] ==1 & z[1]==1, 1, ifelse(z[2] ==2 & z[1]==1, 2, NA)))))))})
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
  })
  
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_be_staunass.tif",sep=""), format="GTiff", overwrite=TRUE)
  
free.kernel (cl) 
  
  # h) nutzbare Feldkapazität
  
  nfk <- raster(paste(BL,"_nFK.tif",sep=""))
  splitRaster(nfk,4,4,path= "Split")
  
    cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
    li <- parLapply(cl, c(1:16), function(y) {
    library(raster)
    BL <- "SH" 
    nfk_t <- raster(paste("./Split/",BL,"_nFK_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))
    
    st <- stack(nfk_t,nutz_t)
    tile <- calc(st, function (z) { ifelse(z[2] %in% c(1,2) & z[1] %in% c(1,2),5,
                                    ifelse(z[2] ==1 & z[1]==3, 4, ifelse(z[2] ==2 & z[1]==3, 5,
                                    ifelse(z[2] ==1 & z[1]==4, 3, ifelse(z[2] ==2 & z[1]==4, 4,
                                    ifelse(z[2] ==1 & z[1]==5, 1, ifelse(z[2] ==2 & z[1]==5, 3, NA)))))))})
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
  })
  
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_be_nFK.tif",sep=""), format="GTiff", overwrite=TRUE)
  
free.kernel (cl)
  
  # j) Jahresmitteltemperatur
  
  Temp <- raster(paste(BL,"_Temp_10.tif",sep=""))
  NAvalue(Temp) <- -999
  splitRaster(Temp,4,4,path= "Split")
  
    cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
    
  li <- parLapply(cl, c(1:16), function(y) {
    library(raster)
    BL <- "SH" 
    Temp_t <- raster(paste("./Split/",BL,"_Temp_10_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))
    
    st <- stack(Temp_t,nutz_t)
    
    tile <- calc(st, function(z) {ifelse(z[2] %in% c(1,2) & z[1] >85 , 5,
                                  ifelse(z[2] ==1 & z[1] <85 & z[1] >80, 4, ifelse(z[2] ==2 & z[1] <85 & z[1] >80, 5,
                                  ifelse(z[2] ==1 & z[1] <80 & z[1] >75, 3, ifelse(z[2] ==2 & z[1] <80 & z[1] >75, 5,
                                  ifelse(z[2] ==1 & z[1] <75 & z[1] >65,2,ifelse(z[2] ==2 & z[1] <75 & z[1] >65, 4,
                                  ifelse(z[2] ==1 & z[1] <65 & z[1] >60, 1,ifelse(z[2] ==2 & z[1] <65 & z[1] >60, 3,
                                  ifelse(z[2] ==1 & z[1] <60, 0,ifelse(z[2] ==2 & z[1] <60, 2, NA)))))))))))})
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
  })
  
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_be_Temp.tif",sep=""), format="GTiff", overwrite=TRUE)
  
free.kernel (cl)
  
  # k) mittlerer Jahresniederschlag
  
  P <- raster(paste(BL,"_P_10.tif",sep=""))
  NAvalue(P) <- -999
  splitRaster(P,4,4,path= "Split")
  
    cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
  li <- parLapply(cl, c(1:16), function(y) {
    library(raster)
    BL <- "SH" 
    P_t <- raster(paste("./Split/",BL,"_P_10_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))
    
    st <- stack(P_t,nutz_t)
    
    tile <- calc(st, function (z) {ifelse(z[2] %in% c(1,2) & z[1] <700 ,5,
                                   ifelse(z[2] ==1 & z[1] <1000 & z[1] >700, 4,
                                   ifelse(z[2] ==2 & z[1] <1000 & z[1] >700, 5,
                                   ifelse(z[2] ==1 & z[1] >1000,3, 
                                   ifelse(z[2] ==2 & z[1] >1000, 4,NA)))))})
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
  })
  
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_be_P.tif",sep=""), format="GTiff", overwrite=TRUE)
  
free.kernel (cl)
  
  # l) Frostgef?hrdung
  
  # m) ?berschwemmungen
  
  # n) Erosionsgef?hrdung
  
  ewa <- raster(paste(BL,"_erwa_2018.tif",sep=""))
  splitRaster(ewa,4,4,path= "Split")
  
    cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
    
  li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "SH" 
    ewa_t <- raster(paste("./Split/",BL,"_erwa_2018_tile",y,".gri", sep=""))
    
    tile <- calc(ewa_t, function (z) {ifelse(z==6, 5, ifelse(z==5, 4, 
                                      ifelse(z %in% c(3:4), 3, ifelse(z==2, 2, 
                                      ifelse(z==1, 1, NA)))))})
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
  })
  
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_be_erg.tif",sep=""), format="GTiff", overwrite=TRUE)
  
free.kernel (cl)
   
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # alles zusammen betrachten - Gründigkeit, Staunässe, Frost, Überschwemmung fehlt
  #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  files <- list.files(path=".", pattern=paste(BL,"_be_",sep=""), full.names = T)
  
  cl <- makeCluster(length(files))
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
  for(i in 1:length(files)){ 
  splitRaster(raster(files[i]),4,4,path= "Split")} 
  
    cl <- makeCluster(16)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
  li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "SH" 
    files <- list.files(path="./Split", pattern=paste("^",BL,"_be_.*tile",y,".gri$",sep=""), full.names = T)
  
    ras_st <- stack(lapply(files, raster))
  
  # minimale Bewertung der Rasterparzelle ist Endbewertung
  bio <- calc(ras_st,fun=function(x) min(x,na.rm=T))
  bio <- calc(bio, fun=function(x) ifelse(x==0,NA,x)) # keine Bewertung mit 0
  
  crs(bio) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(bio, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
  })
  
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_bio_2018.tif",sep=""), format="GTiff", overwrite=TRUE)
   
  