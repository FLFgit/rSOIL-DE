library(data.table)
library(raster)
library(rgdal)
library(SpaDES)
library(snow)
library(parallel)
library(doParallel)
library(sf)
library(fasterize)

##################################
### SQR Ordner anlegen #######
###############################


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

#creates unique filepath for temp directory
dir.create (file.path('temp_Indices'), showWarnings = FALSE)
#sets temp directory
rasterOptions(tmpdir=file.path("~/",'temp_Indices'))

# Parallel Processing
cl <- makeCluster(16)
registerDoParallel(cl)
#beginCluster(type="SOCK")
# Control Parallel Processing
getDoParWorkers()
getDoParName()
getDoParVersion()

free.kernel <- function(cl){
	stopCluster(cl)
	cl <- makeCluster(16)
	registerDoParallel(cl)
	#beginCluster(type="SOCK")
	# Control Parallel Processing
	getDoParWorkers()
	getDoParName()
	getDoParVersion()}

# Einladen Profilinformationen
kombi <- fread("~/Dokumente/Grundlagen_BÜK200/tblProfile.csv", sep=";", dec=".",na.strings = c("",NA))
# GW-Stufe nach KA5
kombi[,GW_Stufe:= sub('GWS',"",GWS)] # nur Ziffer der GW-Stufe verwenden

# Einladen Horizontinformationen
hor <- fread("~/Dokumente/Grundlagen_BÜK200/tblHorizonte.csv", sep=";", dec=".",na.strings = c("",NA))

# Zuweisung Skelettanteil (Vol-%) 1 = <1% | 2 = 1 - <10% | 3 = 10 - <30% | 4 = 30 - <50% |5 = 50 - <75% | 6 > 75%
hor[GROBBOD_K %in% c(0,1), Skelett:= 1][is.na(GROBBOD_K), Skelett:= 1][GROBBOD_K==2, Skelett:= 10][GROBBOD_K==3, Skelett:= 30][GROBBOD_K==4, Skelett:= 50][GROBBOD_K==5, Skelett:= 75][GROBBOD_K==6, Skelett:= 100]


# Einladen Basisraster
BL <- "BW"
setwd(paste("~/Dokumente/Grundlagen_BÜK200/",BL, sep=""))
buk200 <- st_read(paste(BL,"_key_BF_ID.shp", sep=""))
key <- raster(paste(BL,"_key_BF_ID.tif", sep=""))

# Bodenauflagen werden ignoriert
dat <- hor[!OTIEF<0] 
# neue Nummerierung Horizonte nach Anzahl der HOR_NR --> weniger Durchläufe nötig
dat[, NR:=rep(1:.N), by=BF_ID] # Maximum: 10 Horizonte

# Mächtigkeit der Horizonte
dat[,MAECHT:=UTIEF*10-OTIEF*10] # in cm


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Basisindikatoren
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~
# Slope
#~~~~~~~

# Einladen Neigung als Rasterdaten
slope_r<-raster(paste(BL,"_Slope_10.tif", sep=""))

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

splitRaster(slope_r, 4,4, path="Split")

# Bestimmung des Wertstufe

li <- parLapply(cl, c(1:16), function(y) {
  library(raster)
  BL <- "BW"
  slope_t <- raster(paste("./Split/",BL,"_Slope_10_tile",y,".gri", sep=""))
  nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))
  
  # Stacking der Rasterdaten
  st <- stack(nutz_t,slope_t)
  # Bestimmung des Wertstufe
	tile <- calc(st, fun=function(x) ifelse(x[1] ==1 & x[2] %in% c(1,2), 2,
                                   ifelse(x[1] ==2 & x[2] %in% c(1,2,3), 2,
                                   ifelse(x[1] ==1 & x[2] == 3 ,1.5,
                                   ifelse(x[1]==2 & x[2] %in% c(4,5) ,1.5,
                                   ifelse(x[1]==1 & x[2] %in% c(4,5), 1,
                                   ifelse(x[1]==2 & x[2]==6 ,1,
                                   ifelse(x[1]==1 & x[2]==6 ,0.5,
                                   ifelse(x[1]==2 & x[2] %in% c(7,8) ,0.5,
                                   ifelse(x[1]==1 & x[2] >=7 ,0, 
                                   ifelse(x[1]==2 & x[2] >=9 ,0, NA)))))))))))
  
  crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
})

ras <- mergeRaster(li) 
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
ras <- calc(ras,fun=function(x) x*2)
writeRaster(ras, filename="SQR/hang.tif", format="GTiff", overwrite=TRUE)

free.kernel (cl)

#~~~~~~~~~~~~~~~~~~~
# Soil substrate
#~~~~~~~~~~~~~~~~~~~

# Bodenauflagen werden ignoriert

# Ackerland bis 80 cm
oben <- merge(dat[OTIEF==0,BF_ID, HOR_NR],dat,by=c("BF_ID", "HOR_NR"))

flach <- merge(dat[OTIEF==0 & UTIEF<8,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
flach2 <- merge(flach[UTIEF<8,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
flach3 <- merge(flach2[UTIEF<8,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
flach4 <- merge(flach3[UTIEF<8,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
flach5 <- merge(flach4[UTIEF<8,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
flach6 <- merge(flach5[UTIEF<8,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
#flach7 <- merge(flach6[UTIEF<8,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))

# alle Oberbodenhorizonte zusammen
ob <- rbindlist(list(oben,flach,flach2,flach3,flach4,flach5,flach6))

# Einladen Basisraster
buk200 <- st_read(paste(BL,"_key_BF_ID.shp",sep=""))
key <- raster(paste(BL,"_key_BF_ID.tif",sep=""))

# Gesamtmächtigkeit von mind. 80 cm als Raster
GesMae <- ob[, sum(UTIEF-OTIEF)*10, by=BF_ID]
GesMae.sf <- merge(buk200, GesMae, by="BF_ID")
GesMae.raster <- fasterize(GesMae.sf, key, field="V1")
writeRaster(GesMae.raster, filename="sqr_GesMae_A.tif", format="GTiff", overwrite=TRUE)

# wie viele Horizonte werden benötigt für Berechnung?
nr_hor <- ob[, max(NR), by=BF_ID]
nr_hor.sf <- merge(buk200, nr_hor, by="BF_ID")
nr_hor.raster <- fasterize(nr_hor.sf, key, field="V1")
writeRaster(nr_hor.raster, filename="sqr_nr_hor_A.tif", format="GTiff", overwrite=TRUE)
hormaxA <- max(values(nr_hor.raster),na.rm=T) # benötigte Anzahl an Horizonten für Berechnung

# Grünland bis 50 cm
oben <- merge(dat[OTIEF==0,BF_ID, HOR_NR],dat,by=c("BF_ID", "HOR_NR"))

flach <- merge(dat[OTIEF==0 & UTIEF<5,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
flach2 <- merge(flach[UTIEF<5,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
flach3 <- merge(flach2[UTIEF<5,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
flach4 <- merge(flach3[UTIEF<5,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
flach5 <- merge(flach4[UTIEF<5,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))
#flach6 <- merge(flach5[UTIEF<5,BF_ID, HOR_NR+1],dat,by=c("BF_ID", "HOR_NR"))


# alle Oberbodenhorizonte zusammen
ob <- rbindlist(list(oben,flach,flach2,flach3,flach4,flach5))

# Gesamtmächtigkeit von mind. 80 cm als Raster
GesMae <- ob[, sum(UTIEF-OTIEF)*10, by=BF_ID]
GesMae.sf <- merge(buk200, GesMae, by="BF_ID")
GesMae.raster <- fasterize(GesMae.sf, key, field="V1")
writeRaster(GesMae.raster, filename="sqr_GesMae_GL.tif", format="GTiff", overwrite=TRUE)

# wie viele Horizonte werden benötigt für Berechnung?
nr_hor <- ob[, max(NR), by=BF_ID]
nr_hor.sf <- merge(buk200, nr_hor, by="BF_ID")
nr_hor.raster <- fasterize(nr_hor.sf, key, field="V1")
writeRaster(nr_hor.raster, filename="sqr_nr_hor_GL.tif", format="GTiff", overwrite=TRUE)
hormaxG <- max(values(nr_hor.raster),na.rm=T) # benötigte Anzahl an Horizonten für Berechnung

# Zuordnung Acker/GL zu Gesamtmächtigkeit und Horizontanzahl

nutz <- raster(paste(BL,"_Nutzung_2018.tif", sep=""))
GM_A <- raster("sqr_GesMae_A.tif")
GM_GL <- raster("sqr_GesMae_GL.tif")
splitRaster(GM_A,4,4,path= "Split")
splitRaster(GM_GL,4,4,path= "Split")

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
	  BL <- "BW"
    GM_A_t <- raster(paste("./Split/sqr_GesMae_A_tile",y,".gri", sep=""))
    GM_GL_t <- raster(paste("./Split/sqr_GesMae_GL_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))

		st <- stack(nutz_t,GM_A_t, GM_GL_t)

		tile <- calc(st, function(x) ifelse(x[1]==1,x[2],ifelse(x[1]==2,x[3],NA)))
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    })

ras <- mergeRaster(li) 
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, filename="sqr_GesMae.tif", format="GTiff", overwrite=TRUE)

free.kernel (cl)

nr_A <- raster("sqr_nr_hor_A.tif")
nr_GL <- raster("sqr_nr_hor_GL.tif")
splitRaster(nr_A,4,4,path= "Split")
splitRaster(nr_GL,4,4,path= "Split")

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
	  BL <- "BW"
    nr_A_t <- raster(paste("./Split/sqr_nr_hor_A_tile",y,".gri", sep=""))
    nr_GL_t <- raster(paste("./Split/sqr_nr_hor_GL_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))

		st <- stack(nutz_t,nr_A_t, nr_GL_t)

		tile <- calc(st, function(x) ifelse(x[1]==1,x[2],ifelse(x[1]==2,x[3],NA)))
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    })

ras <- mergeRaster(li) 
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, filename="sqr_nr_hor.tif", format="GTiff", overwrite=TRUE)

free.kernel (cl)

hor_nr <- raster("sqr_nr_hor.tif")
splitRaster(hor_nr,4,4,path= "Split")

hormax <- max(values(hor_nr), na.rm =T)

hor_nr <- parLapply(cl,c(1:hormax[1] ), function(x){
  library(snow)
  library(parallel)
  library(doParallel)
  library(raster)
  library(SpaDES)
  
  cl <- makeCluster(4)
  registerDoParallel(cl)
  # Control Parallel Processing
  getDoParWorkers()
  getDoParName()
  getDoParVersion()
  
  BL <- "BW"
  
  ba <- raster(paste(BL,"_BA_Hor",x,".tif", sep=""))
  maecht <- raster(paste(BL,"_Mächtigkeit_Hor",x,".tif", sep=""))
  
  splitRaster(ba,4,4,path= "Split")
  splitRaster(maecht,4,4,path= "Split")
  
  li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "BW"
    ba_t <- raster(paste("./Split/",BL,"_BA_Hor",x,"_tile",y,".gri", sep=""))
    maecht_t <- raster(paste("./Split/",BL,"_Mächtigkeit_Hor",x,"_tile",y,".gri", sep=""))
    hor_nr_t <- raster(paste("./Split/sqr_nr_hor_tile",y,".gri", sep=""))
    
    # wenn Horizont nicht nötig für Berechnung dann Bodenklasse auf NA setzen
    st <- stack(ba_t, hor_nr_t)
    ba_tk <- calc(st, fun=function(z){ifelse(z[2]>=x,z[1],NA)})
    
    st <- stack(ba_tk,maecht_t)
    tile <- calc(st, fun=function(z) {ifelse(z[1] %in% c(43:48,13:16,19,27,28,31), 2*z[2], 
                                      ifelse(z[1] %in% c(41,42,17,18,37,38,11,12), 1.5*z[2],
                                      ifelse(z[1] %in% c(25,26,30:33,40,36,35,10), 1*z[2],
                                      ifelse(z[1] %in% c(39,7),0.5*z[2],  
                                      ifelse(z[1] %in% c(6,8,29,20:22),0,NA)))))})
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",x,"_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    })
    
    ras <- mergeRaster(li) 
    crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(ras, filename=paste("sqr_subst_hor_",x,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })

free.kernel (cl)

# gewichtetes Mittel
#Gesamtmächtigkeit
GM <- raster("sqr_GesMae.tif")
splitRaster(GM,4,4,path= "Split")

files <- list.files(path=".", pattern="sqr_subst_hor_", full.names = T)

for(i in 1:length(files)){ 
splitRaster(raster(files[i]),4,4,path= "Split")} 

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    hormax <- 6
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
    
    BL <- "BW"
    # alle Horizonte einladen und stapeln
    files <- list.files(path="./Split", pattern=paste("sqr_subst_hor_._tile",y,".gri", sep=""), full.names = T)

    hor_t <- stack(lapply(files, raster))

    gm_t <- raster(paste("./Split/sqr_GesMae_tile",y,".gri", sep=""))
    
    hor_st <- stack(hor_t, gm_t)
    tile <- calc(hor_st, fun=function(x) {round2(sum(x[1:hormax[1]], na.rm=T)/x[hormax[1]+1])*3}) # gewichtetes Mittel
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
 })   
    ras <- mergeRaster(li) 
    crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(ras,filename="SQR/subst.tif", format="GTiff", overwrite=TRUE)

free.kernel (cl)
    
#~~~~~~~~~~~~~~~~~~~
# Depth of A-horizon
#~~~~~~~~~~~~~~~~~~~
bokl <- dat[HORIZ %like% "A" & OTIEF >=0 |HORIZ %like% "p" & OTIEF >=0 ,.(BF_ID,NR,OTIEF,UTIEF,HORIZ)]  
ahor <- bokl[,max(UTIEF)*10,by=.(BF_ID)] 
names(ahor) <- c("BF_ID","MAXA")
ahor[MAXA>25, Punkte:=2][MAXA<=25 & MAXA>20, Punkte:= 1.5] [MAXA<=20 & MAXA>15, Punkte:= 1.0] [MAXA<=15 & MAXA>10, Punkte:= 0.5] [MAXA<=10, Punkte:= 0]

ahor.sf <- merge(buk200, ahor, by="BF_ID")
ahor.raster <- fasterize(ahor.sf, key, field="Punkte")
writeRaster(ahor.raster, filename="sqr_depth_A.tif", format="GTiff", overwrite=TRUE)


#~~~~~~~~~~~~~~~~~~~
# Depth of humic soil
#~~~~~~~~~~~~~~~~~~~


# SOM > 4%
bokl <- dat[HUMUS %in% c("h4","h5","h6","h7") & OTIEF >=0 ,.(BF_ID,NR,OTIEF,UTIEF,HUMUS)]
hum <- bokl[,max(UTIEF)*10,by=.(BF_ID)]
hum[V1>=60,Punkte:=2][V1>=30 & V1<60,Punkte:=1.5][V1>=15 & V1<30,Punkte:=1][V1>=5 & V1<15,Punkte:=0.5][V1<5,Punkte:=0]

# SOM < 4%
wen_hum <- dat[!HUMUS %in% c("h4","h5","h6","h7") & OTIEF >=0 & OTIEF<=3 ,.(BF_ID,NR,OTIEF,UTIEF,HUMUS)]
wen_hum <- wen_hum[!BF_ID %in% hum[,BF_ID],] # wenn Standort bereits mit SOM>4% abgedeckt ist, verwerfen
wen_hum <-wen_hum[,min(UTIEF)*10,by=.(BF_ID)]
wen_hum[V1>=15, Punkte:=1][V1<15 & V1>=5, Punkte:=0.5][V1<5, Punkte:=0]

hum <- rbind(hum,wen_hum)

humic.sf <- merge(buk200, hum, by="BF_ID")
humic.raster <- fasterize(humic.sf, key, field="Punkte")
writeRaster(humic.raster, filename="sqr_depth_GL.tif", format="GTiff", overwrite=TRUE)

# Zusammenfassen Acker und Grünland

depth_A <- raster("sqr_depth_A.tif")
depth_GL <- raster("sqr_depth_GL.tif")
splitRaster(depth_A,4,4,path= "Split")
splitRaster(depth_GL,4,4,path= "Split")

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
	  BL <- "BW"
    hor_A_t <- raster(paste("./Split/sqr_depth_A_tile",y,".gri", sep=""))
    hor_GL_t <- raster(paste("./Split/sqr_depth_GL_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))

		st <- stack(nutz_t,hor_A_t, hor_GL_t)

		tile <- calc(st, function(x) ifelse(x[1]==1,x[2]*1,ifelse(x[1]==2,x[3]*2,NA)))
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    })

ras <- mergeRaster(li) 
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, filename="./SQR/sqr_depth.tif", format="GTiff", overwrite=TRUE)

free.kernel (cl)

#~~~~~~~~~~~~~~~~~~~
# Topsoil structure
#~~~~~~~~~~~~~~~~~~~

top <- dat[OTIEF>=0 & UTIEF <=3,.(BF_ID,NR,OTIEF,UTIEF,GEFUEGE,LD)]

# Bewertung nach Diez & Weigelt 1997

top[GEFUEGE == "kru", top:=2][
		GEFUEGE %in% c("sub","bro"), top:=1.5][
		GEFUEGE %in% c("pri","pol","pla") & LD %in% c("Ld1","Ld2"), top:=1.5][
  	GEFUEGE == "koh" & LD =="Ld1", top:=1.5][
  	GEFUEGE %in% c("pri","pol","pla") & LD %in% c(NA,"Ld3"), top:=1.0][
			GEFUEGE == "koh" & LD %in% c(NA,"Ld2"), top:=1.0][
			GEFUEGE == "ein" & LD %in% c("Ld1","Ld2"), top:=1.0][
    	GEFUEGE %in% c("ris","klu"), top:=1][
    		GEFUEGE %in% c("sau","shi") , top:=0.5][
    		GEFUEGE == "koh" & LD %in% c("Ld3","Ld4"), top:=0.5][
    		GEFUEGE == "ein" & LD %in% c(NA,"Ld3","Ld4","Ld5"), top:=0.5][
    		GEFUEGE == "kit" & LD %in% c(NA,"Ld1","Ld2","Ld3"), top:=0.5][
    		GEFUEGE %in% c("pri","pol","pla") & LD %in% c("Ld4","Ld5"), top:=0.5][
      		GEFUEGE == "koh" & LD == "Ld5", top:=0][
      		GEFUEGE == "kit" & LD %in% c("Ld4","Ld5"), top:=0]


# Was ist mit Mooren und NA?
# Einteilung anhand von LD
top[is.na(top)& LD %in% c("Ld1","Ld2"), top:=1.5][is.na(top)& LD %in% c("Ld3","Ld4","Ld5"), top:=0.5]
# wenn LD NA ist, Median nehmen
median(top$top, na.rm=T)
top[is.na(GEFUEGE)&is.na(LD), top:=1.5]

topM <- top[,mean(top), by=.(BF_ID)]

top.sf <- merge(buk200, topM, by="BF_ID")
top.raster <- fasterize(top.sf, key, field="V1")
writeRaster(top.raster, filename="./SQR/top.tif", format="GTiff", overwrite=TRUE)

#~~~~~~~~~~~~~~~~~~~
# Subsoil structure
#~~~~~~~~~~~~~~~~~~~

sub <- dat[OTIEF>0 & UTIEF >3,.(BF_ID,NR,OTIEF,UTIEF,GEFUEGE,LD)]

# Bewertung nach Diez & Weigelt 1997

sub[GEFUEGE == "kru", sub:=2][
		GEFUEGE %in% c("sub","bro"), sub:=1.5][
		GEFUEGE %in% c("pri","pol","pla") & LD %in% c("Ld1","Ld2"), sub:=1.5][
  	GEFUEGE == "koh" & LD =="Ld1", sub:=1.5][
  	GEFUEGE %in% c("pri","pol","pla") & LD %in% c(NA,"Ld3"), sub:=1.0][
			GEFUEGE == "koh" & LD %in% c(NA,"Ld2"), sub:=1.0][
			GEFUEGE == "ein" & LD %in% c("Ld1","Ld2"), sub:=1.0][
    	GEFUEGE %in% c("ris","klu"), sub:=1][
    		GEFUEGE %in% c("sau","shi") , sub:=0.5][
    		GEFUEGE == "koh" & LD %in% c("Ld3","Ld4"), sub:=0.5][
    		GEFUEGE == "ein" & LD %in% c(NA,"Ld3","Ld4","Ld5"), sub:=0.5][
    		GEFUEGE == "kit" & LD %in% c(NA,"Ld1","Ld2","Ld3"), sub:=0.5][
    		GEFUEGE %in% c("pri","pol","pla") & LD %in% c("Ld4","Ld5"), sub:=0.5][
      		GEFUEGE == "koh" & LD == "Ld5", sub:=0][
      		GEFUEGE == "kit" & LD %in% c("Ld4","Ld5"), sub:=0]


# Was ist mit Mooren und NA?

# Einteilung anhand von LD
sub[is.na(sub)& LD %in% c("Ld1","Ld2"), sub:=1.5][is.na(sub)& LD %in% c("Ld3","Ld4","Ld5"), sub:=0.5]
# wenn LD NA ist, Median nehmen
median(sub$sub, na.rm=T)
sub[is.na(GEFUEGE)&is.na(LD), sub:=0.5]

subM <- sub[,mean(sub), by=.(BF_ID)]

sub.sf <- merge(buk200, subM, by="BF_ID")
sub.raster <- fasterize(sub.sf, key, field="V1")
writeRaster(sub.raster, filename="./SQR/sub.tif", format="GTiff", overwrite=TRUE)


#~~~~~~~~~~~~~~~
# Rooting depth
#~~~~~~~~~~~~~~~

# KA5 Tab. 81 effektive Wurzeltiefe über Bodenart und Lagerungsdichte

root <- dat[OTIEF>=0 & UTIEF <=20,.(BF_ID,NR,OTIEF,UTIEF,BOART,LD)]

root[BOART == "gS" & LD %in% c("Ld1","Ld2"), We:=7][BOART == "gS" & LD %in% c(NA,"Ld3","Ld4","Ld5"), We:=7][
		BOART %in% c("Ss", "mS", "mSfs", "mSgs", "fs", "fSms", "fSgs") & LD %in% c("Ld1","Ld2"), We:=8][BOART %in% c("Ss", "mS", "mSfs", "mSgs", "fs", "fSms", "fSgs") & LD %in% c(NA,"Ld3","Ld4","Ld5"), We:=6][
		BOART %in% c("Sl2", "Su2", "Su3", "Su4") & LD %in% c("Ld1","Ld2"), We:=9][BOART %in% c("Sl2", "Su2", "Su3", "Su4") & LD %in% c(NA,"Ld3"), We:=7][BOART %in% c("Sl2", "Su2", "Su3", "Su4") & LD %in% c("Ld4","Ld5"), We:=6][
  	BOART %in% c("Sl3", "St2") & LD %in% c("Ld1","Ld2"), We:=10][BOART %in% c("Sl3", "St2") & LD %in% c(NA,"Ld3"), We:=8][BOART %in% c("Sl3", "St2") & LD %in% c("Ld4","Ld5"), We:=7][
  		BOART %in% c("Sl4", "St3", "Slu") & LD %in% c("Ld1","Ld2"), We:=10][BOART %in% c("Sl4", "St3", "Slu") & LD %in% c(NA,"Ld3"), We:=9][BOART %in% c("Sl4", "St3", "Slu") & LD %in% c("Ld4","Ld5"), We:=8][
  		BOART %in% c("Ls2", "Ls3", "Ls4", "Lt2", "Lt3", "Lts", "Uu", "Us", "Tu2", "Tl", "Tt","Ts2","Ts3","Ts4") & LD %in% c("Ld1","Ld2"), We:=13][BOART %in% c("Ls2", "Ls3", "Ls4", "Lt2", "Lt3", "Lts", "Uu", "Us", "Tu2", "Tl", "Tt","Ts2","Ts3","Ts4") & LD %in% c(NA,"Ld3"), We:=10][BOART %in% c("Ls2", "Ls3", "Ls4", "Lt2", "Lt3", "Lts", "Uu", "Us", "Tu2", "Tl", "Tt","Ts2","Ts3","Ts4") & LD %in% c("Ld4","Ld5"), We:=8][
  		BOART %in% c("Uls", "Ut2", "Ut3", "Ut4", "Lu", "Tu3", "Tu4") & LD %in% c("Ld1","Ld2"), We:=14][BOART %in% c("Uls", "Ut2", "Ut3", "Ut4", "Lu", "Tu3", "Tu4") & LD %in% c(NA,"Ld3"), We:=11][BOART %in% c("Uls", "Ut2", "Ut3", "Ut4", "Lu", "Tu3", "Tu4") & LD %in% c("Ld4","Ld5"), We:=9][
  			BOART %in% c("Hh","Hn","Hu"), We:=6]
  		
root[BOART %in% c("Ts2","Ts3","Ts4")]
rootM <- root[,mean(We, na.rm=T), by=.(BF_ID)]

root.sf <- merge(buk200, rootM, by="BF_ID")
root.raster <- fasterize(root.sf, key, field="V1")
writeRaster(root.raster, filename="root.tif", format="GTiff", overwrite=TRUE)

# Korrektur Landnutzung

root <- raster("root.tif")
splitRaster(root,4,4,path= "./Split")

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "BW"
    root_t <- raster(paste("./Split/root_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))

    # wenn GL 2dm abziehen
    st <- stack(root_t,nutz_t)
    tile <- calc(st, function(x) ifelse(x[2]==2, x[1]-2,x[1]))
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste(BL,"_We.tif",sep=""), format="GTiff", overwrite=TRUE)
  
  free.kernel (cl)
  
# Bewertung
  
root <- raster(paste(BL,"_We.tif",sep=""))
splitRaster(root,4,4,path= "./Split")

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "BW"
    root_t <- raster(paste("./Split/",BL,"_We_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))

   st <- stack(root_t,nutz_t)
    tile <- calc(st, function(x) ifelse(x[2]==1,ifelse(x[1]>=13,2*3, ifelse(x[1]<13 & x[1]>= 10,1.5*3, ifelse(x[1]<10 & x[1]>= 8,1*3, ifelse(x[1]<8 & x[1]>= 5,0.5*3, ifelse(x[1]<5,0,NA))))),
    	ifelse(x[2]==2,ifelse(x[1]>=6,2*2, ifelse(x[1]<6 & x[1]>= 4,1.5*2, ifelse(x[1]<4 & x[1]>= 3,1*2, ifelse(x[1]<3 & x[1]>= 2,0.5*2, ifelse(x[1]<2,0,NA))))),NA)))
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename="./SQR/r_depth.tif", format="GTiff", overwrite=TRUE)
  
  free.kernel (cl)
  
#~~~~~~~~~~~~~~~~~~~~~~~~~
# Profile available water
#~~~~~~~~~~~~~~~~~~~~~~~~~
nfk <- raster(paste(BL,"_nFK_mm.tif",sep=""))
splitRaster(nfk,4,4,path= "./Split")

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "BW"
    nfk_t <- raster(paste("./Split/",BL,"_nFK_mm_tile",y,".gri", sep=""))

    tile <- calc(nfk_t, function(x) ifelse(x>=220, 2*3,
    	ifelse(x<220 & x>= 160, 1.5*3, ifelse(x<160 & x>= 100, 1*3,
    		ifelse(x<100 & x>= 60, 0.5*3, ifelse(x<60,0, NA))))))
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename="./SQR/PAW.tif", format="GTiff", overwrite=TRUE)
  
  free.kernel (cl)
  
#~~~~~~~~~~~~~~~~~~~
# Wetness & Ponding
#~~~~~~~~~~~~~~~~~~~

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "BW"
    gw_t <- raster(paste("./Split/",BL,"_GW_Abstand_BÜK1000_tile",y,".gri", sep=""))

    tile <- calc(gw_t, function(x) ifelse(x>=100, 2*3,
    	ifelse(x<100 & x>= 80, 1.5*3, ifelse(x<80 & x>= 60, 1*3,
    		ifelse(x<60 & x>= 40, 0.5*3, ifelse(x<40,0, NA))))))
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename="./SQR/Wet.tif", format="GTiff", overwrite=TRUE)
  
  free.kernel (cl)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 

# Raster von allen Indikatoren
  files <- list.files(path="./SQR",pattern=".tif", full.names = T)
  for(i in 1:length(files)){ 
  splitRaster(raster(files[i]),4,4,path= "Split")} 
  
  li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    h_t <- raster(paste("./Split/hang_tile",y,".gri", sep=""))
    p_t <- raster(paste("./Split/PAW_tile",y,".gri", sep=""))
    r_t <- raster(paste("./Split/r_depth_tile",y,".gri", sep=""))
    d_t <- raster(paste("./Split/sqr_depth_tile",y,".gri", sep=""))
    s_t <- raster(paste("./Split/sub_tile",y,".gri", sep=""))
    t_t <- raster(paste("./Split/top_tile",y,".gri", sep=""))
    u_t <- raster(paste("./Split/subst_tile",y,".gri", sep=""))
    w_t <- raster(paste("./Split/Wet_tile",y,".gri", sep=""))

    ras_st <- stack(h_t,p_t,r_t,d_t,s_t,t_t,u_t,w_t)

    tile <- calc(ras_st,fun=function(x) sum(x)) 
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })
  ras <- mergeRaster(li) 
  crs(ras) <-  "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename="./SQR/SQR_basic.tif", format="GTiff", overwrite=TRUE)

free.kernel (cl)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Gefährdungsindikatoren
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### Ordner hazard anlegen!! ####

#~~~~~~~~~
# Drought
#~~~~~~~~~

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Einladen KWB Vegetationsperiode
# Bestimmung des Wertstufe 

klim <- raster(paste(BL,"_klim_wb_veg_10.tif", sep=""))
splitRaster(klim,4,4,path= "./Split")

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
  BL <- "BW"
    klim_t <- raster(paste("./Split/",BL,"_klim_wb_veg_10_tile",y,".gri", sep=""))

    tile <- calc(klim_t, fun=function(x) ifelse(x >= -100, 2, 
                                       ifelse(x < -100 & x >= -200, 1.5, 
                                       ifelse(x < -200 & x >= -300 , 1.0,
                                       ifelse(x < -300 & x >= -500, 0.5,
                                       ifelse(x < -500, 0, NA))))))
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename="./sqr_drought_risk.tif", format="GTiff", overwrite=TRUE)

  free.kernel (cl)
  
  # Zusammenfassen Acker und Grünland

risk <- raster("./sqr_drought_risk.tif")
splitRaster(risk,4,4,path= "Split")

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
	  BL <- "BW"
    risk_t <- raster(paste("./Split/sqr_drought_risk_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))

		st <- stack(nutz_t,risk_t)

		tile <- calc(st, function(x)  ifelse(x[1] %in% c(1,2) & x[2]==2,3,
																	ifelse(x[1]==1 & x[2]==1.5, 2.7,ifelse(x[1]==2 & x[2]==1.5, 2.5,
																	ifelse(x[1]==1 & x[2]==1.0, 1.7,ifelse(x[1]==2 & x[2]==1.0, 1.5,
																	ifelse(x[1]==1 & x[2]==0.5, 0.5,ifelse(x[1]==2 & x[2]==0.5, 0.7,
																	ifelse(x[1]==1 & x[2]==0, 0.1,ifelse(x[1]==2 & x[2]==0, 0.5,NA))))))))))
		
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    })

ras <- mergeRaster(li) 
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, filename="./SQR/hazard/drought.tif", format="GTiff", overwrite=TRUE)

free.kernel (cl)
  
#~~~~~~~~~~~~~~~~~~~
# Acidification
#~~~~~~~~~~~~~~~~~~~

ph <- dat[,.(BF_ID,NR,PH)]

ph[!PH %in% c("s3", "s4","s5","s6")& !is.na(PH), Punkte:=2][PH=="s3", Punkte:=1.5][PH=="s4", Punkte:=1.0][PH=="s5", Punkte:=0.5][PH=="s6", Punkte:=0]
ph <- ph[,mean(Punkte), by=.(BF_ID)]

ph.sf <- merge(buk200, ph, by="BF_ID")
ph.raster <- fasterize(ph.sf, key, field="V1")
writeRaster(ph.raster, filename="sqr_pH.tif", format="GTiff", overwrite=TRUE)

# Korrektur Landnutzung

ph <- raster("sqr_pH.tif")
splitRaster(ph,4,4,path= "./Split")

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "BW"
    ph_t <- raster(paste("./Split/sqr_pH_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))
    
    st <- stack(nutz_t,ph_t)

    tile <- calc(st, function(x)  ifelse(x[1] %in% c(1,2) & x[2]==2,3,
																	ifelse(x[1]==1 & x[2]==1.5, 2.7,ifelse(x[1]==2 & x[2]==1.5, 3,
																	ifelse(x[1]==1 & x[2]==1.0, 2.5,ifelse(x[1]==2 & x[2]==1.0, 2.7,
																	ifelse(x[1]==1 & x[2]==0.5, 2.2,ifelse(x[1]==2 & x[2]==0.5, 2.5,
																	ifelse(x[1]==1 & x[2]==0, 1.7,ifelse(x[1]==2 & x[2]==0, 2.2,NA))))))))))
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename="./SQR/hazard/Sauer.tif", format="GTiff", overwrite=TRUE)
  
free.kernel (cl)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Soil depth above hard rock
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# We aus Berechnung der Durchwurzeltiefe s.o.

we <- raster(paste(BL,"_We.tif",sep=""))
splitRaster(we,4,4,path= "./Split")

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "BW"
    we_t <- raster(paste("./Split/",BL,"_We_tile",y,".gri", sep=""))
    
    tile <- calc(we_t, function(x)  ifelse(x>=12,2,
																	ifelse(x<12 & x >=6, 1.5,
																	ifelse(x<6 & x >=3, 1.0,
																	ifelse(x<3 & x >=1, 0.5,
																	ifelse(x<1, 0,NA))))))
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename="sqr_hardrock.tif", format="GTiff", overwrite=TRUE)
  
free.kernel (cl)
  
  # Korrektur Landnutzung

rock <- raster("sqr_hardrock.tif")
splitRaster(rock,4,4,path= "./Split")

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "BW"
    rock_t <- raster(paste("./Split/sqr_hardrock_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))
    
    st <- stack(nutz_t,rock_t)

    tile <- calc(st, function(x)  ifelse(x[1] %in% c(1,2) & x[2]==2,3,
																	ifelse(x[1]==1 & x[2]==1.5, 2.5,ifelse(x[1]==2 & x[2]==1.5, 2.7,
																	ifelse(x[1]==1 & x[2]==1.0, 1.2,ifelse(x[1]==2 & x[2]==1.0, 1.7,
																	ifelse(x[1]==1 & x[2]==0.5, 0.3,ifelse(x[1]==2 & x[2]==0.5, 0.7,
																	ifelse(x[1]==1 & x[2]==0, 0.1,ifelse(x[1]==2 & x[2]==0, 0.5,NA))))))))))
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename="./SQR/hazard/hard_rock.tif", format="GTiff", overwrite=TRUE)
 
  free.kernel (cl)
  
#~~~~~~~~~~~~~~
# Coarse rocks
#~~~~~~~~~~~~~~
ske<- dat[OTIEF>=0 & UTIEF <=150,.(BF_ID,NR,GROBBOD_K)]

ske[GROBBOD_K<=2, Punkte:=2][GROBBOD_K==3, Punkte:=1.5][GROBBOD_K==4, Punkte:=1.0][GROBBOD_K==5, Punkte:=0.5][GROBBOD_K==6, Punkte:=0]
  
ske <- ske[, mean(Punkte), by=.(BF_ID)]
ske.sf <- merge(buk200, ske, by="BF_ID")
ske.raster <- fasterize(ske.sf, key, field="V1")
writeRaster(ske.raster, filename="sqr_ske.tif", format="GTiff", overwrite=TRUE)

ske <- raster("sqr_ske.tif")
splitRaster(ske,4,4,path= "./Split")

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    BL <- "BW"
    ske_t <- raster(paste("./Split/sqr_ske_tile",y,".gri", sep=""))
    nutz_t <- raster(paste("./Split/",BL,"_Nutzung_2018_tile",y,".gri", sep=""))

    st <- stack(ske_t,nutz_t)
    tile <- calc(st, function(x)  ifelse(x[1] %in% c(1,2) & x[2]==2,3,
																	ifelse(x[1]==1 & x[2]==1.5, 2.5,ifelse(x[1]==2 & x[2]==1.5, 2.7,
																	ifelse(x[1]==1 & x[2]==1.0, 1.5,ifelse(x[1]==2 & x[2]==1.0, 2.2,
																	ifelse(x[1]==1 & x[2]==0.5, 0.7,ifelse(x[1]==2 & x[2]==0.5, 1.7,
																	ifelse(x[1]==1 & x[2]==0, 0.5,ifelse(x[1]==2 & x[2]==0, 1.3,NA))))))))))
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })
  ras <- mergeRaster(li) 
  crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename="./SQR/hazard/coarse_rocks.tif", format="GTiff", overwrite=TRUE)

  free.kernel (cl)
  
###############################
# Raster von allen Indikatoren

files <- list.files(path="./SQR/hazard",pattern=".tif", full.names = T)
  for(i in 1:length(files)){ 
  splitRaster(raster(files[i]),4,4,path= "Split")} 
  
  li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    h_t <- raster(paste("./Split/drought_tile",y,".gri", sep=""))
    p_t <- raster(paste("./Split/coarse_rocks_tile",y,".gri", sep=""))
    r_t <- raster(paste("./Split/Sauer_tile",y,".gri", sep=""))
    d_t <- raster(paste("./Split/hard_rock_tile",y,".gri", sep=""))

    ras_st <- stack(h_t,p_t,r_t,d_t)

    tile <- calc(ras_st,fun=function(x) min(x, na.rm=T)) 
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })
  ras <- mergeRaster(li) 
  crs(ras) <-  "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename="./SQR/SQR_hazard.tif", format="GTiff", overwrite=TRUE)
  
  free.kernel (cl)

hazard <- raster("./SQR/SQR_hazard.tif")
splitRaster(hazard,4,4,path= "Split")
basic <- raster("./SQR/SQR_basic.tif")
splitRaster(basic,4,4,path= "Split")  

li <- parLapply(cl,c(1:16), function(y) {
    library(raster)
    h_t <- raster(paste("./Split/SQR_hazard_tile",y,".gri", sep=""))
    p_t <- raster(paste("./Split/SQR_basic_tile",y,".gri", sep=""))

    ras_st <- stack(h_t,p_t)

    tile <- calc(ras_st, fun=function(x) x[1]*x[2]) 
    
    crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
    
  })
  ras <- mergeRaster(li) 
  crs(ras) <-  "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  writeRaster(ras, filename=paste("./SQR/",BL,"_SQR.tif",sep=""), format="GTiff", overwrite=TRUE)

  # Einteilung in 5 Klassen
  recl <- matrix(data=NA,5,3)
  recl[,1] <- c(0,20,40,60,80)
  recl[,2] <- c(20,40,60,80,100)
  recl[,3] <- c(1:5)
  ras <- reclassify(ras,recl)
  writeRaster(ras, filename=paste("./SQR/",BL,"_SQR_5K.tif",sep=""), format="GTiff", overwrite=TRUE)
