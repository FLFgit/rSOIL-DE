setwd()
	
library(data.table)
library(raster)
library(rgdal)
library(SpaDES)
library(snow)
library(parallel)
library(doParallel)
library(sf)
library(fasterize)
	
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
cl <- makeCluster(3)
registerDoParallel(cl)
#beginCluster(type="SOCK")
# Control Parallel Processing
getDoParWorkers()
getDoParName()
getDoParVersion()
#stopCluster(cl) 

# Einladen Profilinformationen
kombi <- fread("tblProfile.csv", sep=";", dec=".",na.strings = c("",NA))
# GW-Stufe nach KA5
kombi[,GW_Stufe:= sub('GWS',"",GWS)] # nur Ziffer der GW-Stufe verwenden

# Einladen Horizontinformationen
hor <- fread("tblHorizonte.csv", sep=";", dec=".",na.strings = c("",NA))

# Zuweisung Skelettanteil (Vol-%) 1 = <1% | 2 = 1 - <10% | 3 = 10 - <30% | 4 = 30 - <50% |5 = 50 - <75% | 6 > 75%
hor[GROBBOD_K %in% c(0,1), Skelett:= 1][is.na(GROBBOD_K), Skelett:= 1][GROBBOD_K==2, Skelett:= 10][GROBBOD_K==3, Skelett:= 30][GROBBOD_K==4, Skelett:= 50][GROBBOD_K==5, Skelett:= 75][GROBBOD_K==6, Skelett:= 100]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

############################
# Abspeichern Bodendaten 
# Horizontweise als Raster
############################

# Einladen Basisshape
buk200 <- st_read("clc06_key_BF_ID.shp")

# Einladen Basisraster
key <- raster("clc06_key_BF_ID.tif")

### Erstellung der dat datatable (wichtig für nFK Berechnung weiter unten)

# Bodenauflagen werden ignoriert
dat <- hor[!OTIEF<0] 

# neue Nummerierung Horizonte nach Anzahl der HOR_NR --> weniger Durchläufe nötig
dat[, NR:=rep(1:.N), by=BF_ID] # Maximum: 10 Horizonte

# Mächtigkeit der Horizonte
############################

dat[,MAECHT:=UTIEF*10-OTIEF*10] # in cm

hor_nr <- parLapply(cl,c(1:10), function(x){
	library(sf)
	library(raster)
	library(data.table)
	library(fasterize)

	buk200 <- st_read("clc06_key_BF_ID.shp")
	key <- raster("clc06_key_BF_ID.tif")
	hor <- fread("tblHorizonte.csv", sep=";", dec=".",na.strings = c("",NA))

	# Bodenauflagen werden ignoriert
	dat <- hor[!OTIEF<0] 

	# neue Nummerierung Horizonte nach Anzahl der HOR_NR --> weniger Durchläufe nötig
	dat[, NR:=rep(1:.N), by=BF_ID] # Maximum: 10 Horizonte

	# Mächtigkeit der Horizonte
	dat[,MAECHT:=UTIEF*10-OTIEF*10] # in cm

	maecht.sf <- merge(buk200, dat[NR==x,.(BF_ID, MAECHT)], by="BF_ID")
	maecht.raster <- fasterize(maecht.sf, key, field="MAECHT")
	writeRaster(maecht.raster, paste("MÃ¤chtigkeit_Hor",x,".tif", sep=""),overwrite=T)

	rm(maecht.sf)
	rm(maecht.raster)})

# Bodenartenklasse
##################

# Zuordnung der Bodenklasse nach Tab.1 Handbuch GÖK25
klba <- fread("../Klassen_Bodenart.csv", sep=";", dec=".")
bokl <- dat[,.(BOKLASSE=(klba$Klasse[which(klba==BOART, arr.ind = T)][1])),by=.(BF_ID,NR,BOART)]

bokl[grep("H",BOART), BOKLASSE:=10] # Moore
bokl[grep("F", BOART), BOKLASSE:= 2] # Mudden 

hor_nr <- parLapply(cl,c(1:10), function(x){
	library(sf)
	library(raster)
	library(data.table)
	library(fasterize)

	buk200 <- st_read("clc06_key_BF_ID.shp")
	key <- raster("clc06_key_BF_ID.tif")
	hor <- fread("tblHorizonte.csv", sep=";", dec=".",na.strings = c("",NA))

	# Bodenauflagen werden ignoriert
	dat <- hor[!OTIEF<0] 

	# neue Nummerierung Horizonte nach Anzahl der HOR_NR --> weniger Durchläufe nötig
	dat[, NR:=rep(1:.N), by=BF_ID] # Maximum: 10 Horizonte

	# Zuordnung der Bodenklasse nach Tab.1 Handbuch GÖK25
	klba <- fread("../Klassen_Bodenart.csv", sep=";", dec=".")
	bokl <- dat[,.(BOKLASSE=(klba$Klasse[which(klba==BOART, arr.ind = T)][1])),by=.(BF_ID,NR,BOART)]

	bokl[grep("H",BOART), BOKLASSE:=10] # Moore
	bokl[grep("F", BOART), BOKLASSE:= 2] # Mudden 

	Boklasse.sf <- merge(buk200, bokl[NR==x,.(BF_ID, BOKLASSE)], by="BF_ID")
	Boklasse.raster <- fasterize(Boklasse.sf, key, field="BOKLASSE")
	writeRaster(Boklasse.raster, paste("Boklasse_Hor",x,".tif", sep=""),overwrite=T)

	rm(Boklasse.sf)
	rm(Boklasse.raster)})

# Lagerungsdichte numerisch 
###########################

dat[is.na(LD), LD:=SV] # wenn Moor, dann SV statt LD

dat$trd <- as.numeric(as.factor(dat[,LD]))
#cbind(dat[trd %in% unique(trd),unique(LD)],unique(dat$trd)) # Schlüssel

hor_nr <- parLapply(cl,c(1:10), function(x){
	library(sf)
	library(raster)
	library(data.table)
	library(fasterize)

	buk200 <- st_read("clc06_key_BF_ID.shp")
	key <- raster("clc06_key_BF_ID.tif")
	hor <- fread("tblHorizonte.csv", sep=";", dec=".",na.strings = c("",NA))

	# Bodenauflagen werden ignoriert
	dat <- hor[!OTIEF<0] 

	# neue Nummerierung Horizonte nach Anzahl der HOR_NR --> weniger Durchläufe nötig
	dat[, NR:=rep(1:.N), by=BF_ID] # Maximum: 10 Horizonte

	dat[is.na(LD), LD:=SV] # wenn Moor, dann SV statt LD
	dat$trd <- as.numeric(as.factor(dat[,LD]))

	LD.sf <- merge(buk200, dat[NR==x,.(BF_ID, trd)], by="BF_ID")
	LD.raster <- fasterize(LD.sf, key, field="trd")
	writeRaster(LD.raster, paste("LD_Hor",x,".tif", sep=""),overwrite=T)

	rm(LD.sf)
	rm(LD.raster)})

# Bodenart numerisch 
####################

dat$ba <- as.numeric(as.factor(dat[,BOART]))
#cbind(dat[ba %in% unique(ba),unique(BOART)],unique(dat$ba))# Schlüssel

hor_nr <- parLapply(cl,c(1:10), function(x){
	library(sf)
	library(raster)
	library(data.table)
	library(fasterize)

	buk200 <- st_read("clc06_key_BF_ID.shp")
	key <- raster("clc06_key_BF_ID.tif")
	hor <- fread("tblHorizonte.csv", sep=";", dec=".",na.strings = c("",NA))

	# Bodenauflagen werden ignoriert
	dat <- hor[!OTIEF<0] 

	# neue Nummerierung Horizonte nach Anzahl der HOR_NR --> weniger Durchläufe nötig
	dat[, NR:=rep(1:.N), by=BF_ID] # Maximum: 10 Horizonte

	dat$ba <- as.numeric(as.factor(dat[,BOART]))

	ba.sf <- merge(buk200, dat[NR==x,.(BF_ID, ba)], by="BF_ID")
	ba.raster <- fasterize(ba.sf, key, field="ba")
	writeRaster(ba.raster, paste("BA_Hor",x,".tif", sep=""), overwrite=T)

	rm(ba.sf)
	rm(ba.raster)})

# Bodenart für Berechnung nFK 
###############################

# wird verwendet für die Erstellung der Tabellen 70-73 KA5

# Hochmoorhorizonte mit Zersetzungsstufe verbinden für nFK Ermittlung
dat[grep("Hh",BOART), BOART:= ifelse(HORIZ %like% c("Hr|Hw|hH"), "Hh1", ifelse(HORIZ %like% c("Ha|Hv|Ht|Hm"), "Hh2", NA))]

# Niedermoorhorizonte (incl. Übergangsmoor) mit Horizontsymbol verbinden für nFK Ermittlung
dat[grep("Hn|Hu",BOART), BOART:= ifelse(HORIZ %like% c("Hv"), "Hn1", ifelse(HORIZ %like% c("Hm|Ha"), "Hn2", ifelse(HORIZ %like% c("Ht"), "Hn3", ifelse(HORIZ %like% c("Hr|Hw|nH|uH"), "Hn4",NA))))]

dat$ba <- as.numeric(as.factor(dat[,BOART]))
#cbind(dat[ba %in% unique(ba),unique(BOART)],unique(dat$ba))# Schlüssel


### Humus numerisch ###

dat[grep("h1",HUMUS), org:=1]
dat[grep("h2",HUMUS), org:=2]
dat[grep("h3",HUMUS), org:=3]
dat[grep("h4",HUMUS), org:=4]
dat[grep("h5",HUMUS), org:=5]
dat[grep("h6",HUMUS), org:=6]
dat[grep("h7",HUMUS), org:=7]
dat[is.na(HUMUS) , org:=0]
dat[grep("h0",HUMUS), org:=0]

hor_nr <- parLapply(cl,c(1:10), function(x){
	library(sf)
	library(raster)
	library(data.table)
	library(fasterize)

	buk200 <- st_read("clc06_key_BF_ID.shp")
	key <- raster("clc06_key_BF_ID.tif")
	hor <- fread("tblHorizonte.csv", sep=";", dec=".",na.strings = c("",NA))

	# Bodenauflagen werden ignoriert
	dat <- hor[!OTIEF<0] 
	# neue Nummerierung Horizonte nach Anzahl der HOR_NR --> weniger Durchläufe nötig
	dat[, NR:=rep(1:.N), by=BF_ID] # Maximum: 10 Horizonte

	dat[grep("h1",HUMUS), org:=1]
	dat[grep("h2",HUMUS), org:=2]
	dat[grep("h3",HUMUS), org:=3]
	dat[grep("h4",HUMUS), org:=4]
	dat[grep("h5",HUMUS), org:=5]
	dat[grep("h6",HUMUS), org:=6]
	dat[grep("h7",HUMUS), org:=7]
	dat[is.na(HUMUS) , org:=0]
	dat[grep("h0",HUMUS), org:=0]

	humus.sf <- merge(buk200, dat[NR==x,.(BF_ID, org)], by="BF_ID")
	humus.raster <- fasterize(humus.sf, key, field="org")
	writeRaster(humus.raster, paste("Humus_Hor",x,".tif", sep=""),overwrite=T)

	rm(humus.sf)
	rm(humus.raster)})

# Zuweisung"Humusstufe" <2 % = 1 | 2 - <4 = 2 | >4 = 3 
##########################################################

dat[, HUMUS_K:=as.numeric(HUMUS)][grep("h0|h1|h2",HUMUS), HUMUS_K:= 1][grep("h3",HUMUS), HUMUS_K:= 2][grep("h4|h5|h6|h7",HUMUS), HUMUS_K:= 3]
dat[is.na(HUMUS_K), HUMUS_K:=1] # alle ohne Humusgehaltangabe sind h0

hor_nr <- parLapply(cl,c(1:10), function(x){
	library(sf)
	library(raster)
	library(data.table)
	library(fasterize)

	buk200 <- st_read("clc06_key_BF_ID.shp")
	key <- raster("clc06_key_BF_ID.tif")
	hor <- fread("tblHorizonte.csv", sep=";", dec=".",na.strings = c("",NA))

	# Bodenauflagen werden ignoriert
	dat <- hor[!OTIEF<0] 

	# neue Nummerierung Horizonte nach Anzahl der HOR_NR --> weniger Durchläufe nötig
	dat[, NR:=rep(1:.N), by=BF_ID] # Maximum: 10 Horizonte

	# Zuweisung"Humusstufe" <2 % = 1 | 2 - <4 = 2 | >4 = 3 
	dat[, HUMUS_K:=as.numeric(HUMUS)][grep("h0|h1|h2",HUMUS), HUMUS_K:= 1][grep("h3",HUMUS), HUMUS_K:= 2][grep("h4|h5|h6|h7",HUMUS), HUMUS_K:= 3]
	dat[is.na(HUMUS_K), HUMUS_K:=1] # alle ohne Humusgehaltangabe sind h0

	humus.sf <- merge(buk200, dat[NR==x,.(BF_ID, HUMUS_K)], by="BF_ID")
	humus.raster <- fasterize(humus.sf, key, field="HUMUS_K")
	writeRaster(humus.raster, paste("Humus_Klasse",x,".tif", sep=""),overwrite=T)

	rm(humus.sf)
	rm(humus.raster)})

# Skelettanteil
##################

hor_nr <- parLapply(cl,c(1:10), function(x){
	library(sf)
	library(raster)
	library(data.table)
	library(fasterize)

	buk200 <- st_read("clc06_key_BF_ID.shp")
	key <- raster("clc06_key_BF_ID.tif")
	hor <- fread("tblHorizonte.csv", sep=";", dec=".",na.strings = c("",NA))

	# Zuweisung Skelettanteil (Vol-%) 1 = <1% | 2 = 1 - <10% | 3 = 10 - <30% | 4 = 30 - <50% |5 = 50 - <75% | 6 > 75%
	hor[GROBBOD_K %in% c(0,1), Skelett:= 1][is.na(GROBBOD_K), Skelett:= 1][GROBBOD_K==2, Skelett:= 10][GROBBOD_K==3, Skelett:= 30][GROBBOD_K==4, Skelett:= 50][GROBBOD_K==5, Skelett:= 75][GROBBOD_K==6, Skelett:= 100]

	# Bodenauflagen werden ignoriert
	dat <- hor[!OTIEF<0] 

	# neue Nummerierung Horizonte nach Anzahl der HOR_NR --> weniger Durchläufe nötig
	dat[, NR:=rep(1:.N), by=BF_ID] # Maximum: 10 Horizonte

	skel.sf <- merge(buk200, dat[NR==x,.(BF_ID, Skelett)], by="BF_ID")
	skel.raster <- fasterize(skel.sf, key, field="Skelett")
	writeRaster(skel.raster, paste("Skelett_Hor",x,".tif", sep=""),overwrite=T)

	rm(skel.sf)
	rm(skel.raster)})

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#####################################
# Abspeichern Bodendaten per Standort
#####################################

# GW-Abstand
#############
# über BÜK1000

gw <- raster("../GW_Abstand.tif")

# Umwandeln 100 m in 10 m Auflösung

#gw_1000 <- raster("../MNGW_100.tif")
#key_r<-raster("clc06_key_BF_ID.tif")

# 10x10 Raster
# b <- raster(nrow=88965, ncol=97782)
# extent(b) <- extent(key_r)
# crs(b) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
# gw_10 <- resample(gw_1000, b, method="bilinear")
# 
# writeRaster(gw_10, filename="MNGW_10.tif", format="GTiff", overwrite=TRUE)

gw_10 <- raster("../../MNGW_10.tif")

splitRaster(gw,4,4,path= "Split")
splitRaster(gw_10,4,4,path= "Split")

	cl <- makeCluster(16)
	registerDoParallel(cl)
	# Control Parallel Processing
	getDoParWorkers()
	getDoParName()
	getDoParVersion()

li <- parLapply(cl,c(1:16), function(y) {
	library(raster)

	gw_t <- raster(paste("./Split/GW_Abstand_tile",y,".gri", sep=""))
	gw10_t <- raster(paste("./Split/MNGW_10_tile",y,".gri", sep="")) #Name bleibt bei 100, nicht 10

	st <- stack(gw_t,gw10_t)
	tile <- calc(st, function(x) ifelse(is.na(x[1]),x[2],x[1]))
	crs(tile) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
	writeRaster(tile, filename=paste("li_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
})
	
ras <- mergeRaster(li) 
crs(ras) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(ras, filename="../GW_Abstand_BÜK1000.tif", format="GTiff", overwrite=TRUE)

# Zuordnung GW-Stufe nach KA5
##############################

gw <- raster("GW_Abstand_BÜK1000.tif")

recl <- matrix(data=NA,6,3)
recl[,1] <- c(0,20,40,80,130,200) # from
recl[,2] <- c(20,40,80,130,200,300) # to
recl[,3] <- c(1,2,3,4,5,6) # beocomes
gw_st <- reclassify(gw,recl,right=F)

crs(gw_st) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
writeRaster(gw_st, filename="GW_Stufe_BÜK1000.tif", format="GTiff", overwrite=TRUE)

# Berechnung nFK mit KA5 in Vol%
#################################

Tab71 <- read.csv("Tab71_KA5.csv",sep=";", header=F,row.names = 1)
Tab70 <- read.csv("Tab70_KA5.csv",sep=";", header=F,row.names = 1)
Tab72 <- read.csv("Tab72_KA5.csv",sep=";", header=F,row.names = 1)
Tab73 <- read.csv("Tab73_KA5.csv",sep=";", header=F,row.names = 1)

NFK <- dat[, .(BF_ID,NR,UTIEF,MAECHT, ba, trd, org)]
NFK <- NFK[UTIEF<=200] # dm in cm

# Ermittlung TRD über LD und Bodenart 
NFK[, ba_trd:=paste(ba,trd,sep="")]
NFK[,pt:=Tab71[ba_trd,1]]

# Ermittlung nFK über TRD für Mineralböden
NFK[, ba_pt:=paste(ba,pt,sep="")]
NFK[, nfk:=Tab70[ba_pt,1]]

# Korrektur mittels Humusgehalt
NFK[, ba_org:=paste(ba,org,sep="")]
NFK[, nfk_hum:=Tab72[ba_org,1]]
NFK[is.na(nfk_hum), nfk_hum:=0] # betrifft Humusstufe h0

# Aufaddieren Korrektur
NFK[, nfk_kor:=nfk+nfk_hum]

# Ermittlung nFK über Horizontsymbol und Zersetzungsstufe für Moorböden
NFK[, nfk_m:=Tab73[ba_trd,1]]

# Zusammenlegung der nFK von Mineralböden und Moorböden
NFK[is.na(nfk_kor)&!is.na(nfk_m), nfk_kor:=nfk_m]

# Gewichtung nach Mächtigkeit der Horizonte bis max Tiefe    

# NA Horizonte werden nicht in Gesamtmächtigkeit einbezogen
NFK[!is.na(ba), GesMae:=sum(MAECHT), by=BF_ID]

nfk <- NFK[,max(round2(sum(nfk_kor*MAECHT)/GesMae)),by=BF_ID]
# mm
nfk <-nfk[,V1mm:=V1*10]
nfk.sf <- merge(buk200, nfk, by="BF_ID")
nfk.raster <- fasterize(nfk.sf, key, field="V1mm")
writeRaster(nfk.raster, filename="nFK_mm.tif", format="GTiff", overwrite=TRUE)

#Einteilung in NFK-Klassen
##########################

nfk[V1 >=30, nfk:=1][V1 <30 & V1>=22, nfk:=2][V1 <22 & V1>=14, nfk:=3][V1 <14 & V1>=6, nfk:=4][V1<6, nfk:=5]

#Raster erzeugen
nfk.sf <- merge(buk200, nfk, by="BF_ID")
nfk.raster <- fasterize(nfk.sf, key, field="nfk")
writeRaster(nfk.raster, filename="nFK.tif", format="GTiff", overwrite=TRUE)

# Staunässegrad
################

nass <- kombi[,.(BF_ID, VNGRAD)]
nass[VNGRAD %like% c("0|1"), nass:=4][VNGRAD =="Vn2", nass:=3][VNGRAD %like% c("3|4"), nass:=2][VNGRAD %like% c("5|6"), nass:=1][is.na(VNGRAD), nass:=4]

#Raster erzeugen
nass.sf <- merge(buk200, nass[,.(BF_ID,nass)], by="BF_ID")
nass.raster <- fasterize(nass.sf, key, field="nass")
writeRaster(nass.raster, filename="Staunass.tif", format="GTiff", overwrite=TRUE)


# Auflagehorizont - für Abflussregulation
##########################################

auflage <- hor[BOART =="O",L:=1][L==1,lapply(.SD, max), by=BF_ID]
auflage.sf <- merge(buk200, auflage[,.(BF_ID,L)], by="BF_ID",all.x=T)
naIndex <- which(is.na(auflage.sf$L)) # alle Profiile ohne Auflage auf 0 setzen anstatt NA
auflage.sf[naIndex, "L"] <- 0
auflage.raster <- fasterize(auflage.sf, key, field="L")
writeRaster(auflage.raster, filename="Auflage.tif", format="GTiff", overwrite=TRUE)