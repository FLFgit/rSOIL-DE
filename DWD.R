library("RPostgreSQL")
library(rpostgis)
library(raster)

# Einladen Niederschlagsdaten

P_1989_2018 <- raster("C:/Users/annelie.saeurich/Documents/DWD/P_Sum_annual_UTM32_1989-2018.tif")
crs(P_1989_2018) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
NAvalue(P_1989_2018) <- -999

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# mit Datenbank verbinden

drv <- dbDriver("PostgreSQL")

pw <- {"Wol!Rea?21"}

con <- dbConnect(drv, dbname = "dwd",
                 host = "agro-de-db.julius-kuehn.de", port = 5432,
                 user = "qgisdwdro", password = pw)

rm(pw)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Berechnung potentielle Verdunstung

year <- c(1991:2018)

etp <- lapply(year, function(y) {

if (y %in% c(1992,1996,2000,2004,2008,2012,2016)){ # Schaltjahr
  day <- c("001","032","061","092","122","153","183","214","245","275","306","336")}

else {day <- c("001","032","060","091","121","152","182","213","244","274","305","335")}  
  
rast <- lapply(day, function(x) {
  dbRemoveTable(con, "dwd_verdunstung_test") # Tabelle löschen, da Überschreiben nicht möglich
  query_paste <- paste("CREATE TABLE public.dwd_verdunstung_test AS SELECT rid, filename,doy, ST_Transform(rast, 25832, 'NearestNeighbor') AS rast FROM dwd.verdunstung_p_monatlich WHERE doy = ",y,x,sep="")
  dbSendQuery(con, query_paste) # Abfrage Datenbank -> schreibe Tabelle für raster mit gesuchtem DOY
  print(x)
  pgGetRast(con,"dwd_verdunstung_test","rast") # Raster einladen
  
})

rast_st <- stack(rast)
print(y)
calc(rast_st,sum)  # Summe aller Monate

})

etp_st <- stack(etp)
etp_rast_1991_2018 <- calc(etp_st,mean) # Mittelwert aus allen Jahren berechnen

extent(etp_rast_1991_2018) <- extent(P_1989_2018) # Extent an P Raster anpassen
writeRaster(etp_rast_1991_2018, filename="C:/Users/annelie.saeurich/Documents/DWD/etp_monatl_mw_1991_2018.tif", format="GTiff", overwrite=TRUE)


# Klimatische Wasserbilanz = P - ETP

klim_wb_st <- stack(etp_rast_1991_2018,P_1989_2018)
klim_wb <- calc(klim_wb_st, fun=function (x) x[[2]]-(x[[1]]/10)) # klimatische Wasserbilanz berechnen, ETP Werte müssen durch 10 geteilt werden
writeRaster(klim_wb, filename="C:/Users/annelie.saeurich/Documents/DWD/klim_wb.tif", format="GTiff", overwrite=TRUE)

# jährlich

key_r<-raster("C:/Users/annelie.saeurich/Documents/ArcGIS/Raster/key_1000.tif")

year <- c(1991:2018)

etp <- lapply(year, function(y) {
  
  if (y %in% c(1992,1996,2000,2004,2008,2012,2016)){ # Schaltjahr
    day <- c("001","032","061","092","122","153","183","214","245","275","306","336")}
  
  else {day <- c("001","032","060","091","121","152","182","213","244","274","305","335")}  
  
  rast <- lapply(day, function(x) {
    dbRemoveTable(con, "dwd_verdunstung_test") # Tabelle löschen, da Überschreiben nicht möglich
    query_paste <- paste("CREATE TABLE public.dwd_verdunstung_test AS SELECT rid, filename,doy, ST_Transform(rast, 25832, 'NearestNeighbor') AS rast FROM dwd.verdunstung_p_monatlich WHERE doy = ",y,x,sep="")
    dbSendQuery(con, query_paste) # Abfrage Datenbank -> schreibe Tabelle für raster mit gesuchtem DOY
    print(x)
    pgGetRast(con,"dwd_verdunstung_test","rast") # Raster einladen
    
  })
  
  rast_st <- stack(rast)
  print(y)
  etp_sum <- calc(rast_st,sum)  # Summe aller Monate
  
  path <- paste("Z:/DWD/Zeitreihen/Jahresmittel/P/raw/RSMS_17_",y,"_01.asc",sep="")
  
  P <- raster(path)
  crs(P) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  #P <- reclassify(P, cbind(-999, -998, NA), right=FALSE)
  extent(P) <- extent(key_r) # Extent an P Raster anpassen
  extent(etp_sum) <- extent(P) # Extent an P Raster anpassen
  
  klim_wb_st <- stack(etp_sum,P)
  klim_wb <- calc(klim_wb_st, fun=function (x) x[[2]]-(x[[1]]/10)) # kliamtische Wasserbilanz berechnen, ETP Werte müssen durch 10 geteilt werden
  writeRaster(klim_wb, filename=paste("C:/Users/annelie.saeurich/Documents/DWD/klim_wb_",y,".tif", sep=""), format="GTiff", overwrite=TRUE)
  
})


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Sommerniederschlag

key_r<-raster("C:/Users/annelie.saeurich/Documents/ArcGIS/Raster/key_1000.tif")

year <- c(1961:2018)

bla <- lapply(year, function(y) {
  month <- c("05","06","07","08","09","10")
  
  p_rast <- lapply(month, function(x) {
    p <- raster(paste("Z:/DWD/Zeitreihen/Monatsmittel/P/",x,"/raw/RSMS_",x,"_",y,"_01.asc",sep=""))})
  p_rast_st <- stack(p_rast)
  p_sommer <- calc(p_rast_st,sum)
  
  crs(p_sommer) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
  extent(p_sommer) <- extent(key_r)
  writeRaster(p_sommer, filename=paste("C:/Users/annelie.saeurich/Documents/DWD/p_sommer_",y,".tif",sep=""), format="GTiff", overwrite=TRUE)
})

month <- c("05","06","07","08","09","10")

p_rast <- lapply(month, function(x) {
  p <- raster(paste("Z:/DWD/Zeitreihen/Monatsmittel/P/",x,"/P_SUM_monthly_UTM32_",x,"_1981-2010.dat",sep=""))
  NAvalue(p) <- -999})
p_rast_st <- stack(p_rast)
p_sommer <- calc(p_rast_st,sum)

crs(p_sommer) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"

writeRaster(p_sommer, filename="C:/Users/annelie.saeurich/Documents/DWD/p_sommer_1981-2010.tif", format="GTiff", overwrite=TRUE)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Berechnung potentielle Verdunstung - Mai bis August

year <- c(1991:2018)

etp <- lapply(year, function(y) {
  
  if (y %in% c(1992,1996,2000,2004,2008,2012,2016)){ # Schaltjahr
    day <- c("122","153","183","214")}
  
  else {day <- c("121","152","182","213")}  
  
  rast <- lapply(day, function(x) {
    dbRemoveTable(con, "dwd_verdunstung_test") # Tabelle löschen, da Überschreiben nicht möglich
    query_paste <- paste("CREATE TABLE public.dwd_verdunstung_test AS SELECT rid, filename,doy, ST_Transform(rast, 25832, 'NearestNeighbor') AS rast FROM dwd.verdunstung_p_monatlich WHERE doy = ",y,x,sep="")
    dbSendQuery(con, query_paste) # Abfrage Datenbank -> schreibe Tabelle für raster mit gesuchtem DOY
    print(x)
    pgGetRast(con,"dwd_verdunstung_test","rast") # Raster einladen
    
  })
  
  rast_st <- stack(rast)
  print(y)
  calc(rast_st,sum)  # Summe aller Monate
  
})

etp_st <- stack(etp)
etp_rast_1991_2018 <- calc(etp_st,mean) # Mittelwert aus allen Jahren berechnen

extent(etp_rast_1991_2018) <- extent(P_1989_2018) # Extent an P Raster anpassen
writeRaster(etp_rast_1991_2018, filename="C:/Users/annelie.saeurich/Documents/DWD/etp_veg_mw_1991_2018.tif", format="GTiff", overwrite=TRUE)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Niederschlag Vegetationsperiode Mai-August

month <- c("05","06","07","08")

p_rast <- lapply(month, function(x) {
  p <- raster(paste("Z:/DWD/Zeitreihen/Monatsmittel/P/",x,"/P_SUM_monthly_UTM32_",x,"_1989-2018.dat",sep=""))
  reclassify(p, cbind(-999, -998, NA), right=FALSE)})
p_rast_st <- stack(p_rast)
p_sommer <- calc(p_rast_st,sum)

crs(p_sommer) <- "+proj=utm +zone=32 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"

writeRaster(p_sommer, filename="C:/Users/annelie.saeurich/Documents/DWD/p_veg_1989-2018.tif", format="GTiff", overwrite=TRUE)

# Klimatische Wasserbilanz = P - ETP

klim_wb_st <- stack(etp_rast_1991_2018,p_sommer)
klim_wb <- calc(klim_wb_st, fun=function (x) x[[2]]-(x[[1]]/10)) # klimatische Wasserbilanz berechnen, ETP Werte müssen durch 10 geteilt werden
writeRaster(klim_wb, filename="C:/Users/annelie.saeurich/Documents/DWD/klim_wb_veg.tif", format="GTiff", overwrite=TRUE)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Berechnung Temperatursummen

year <- c(1989:2018)

temp <- lapply(year, function(y) {
  
      day <- sprintf("%03d",c(1:365))
  
  rast <- lapply(day, function(x) {
    dbRemoveTable(con, "dwd_verdunstung_test") # Tabelle löschen, da Überschreiben nicht möglich
    query_paste <- paste("CREATE TABLE public.dwd_verdunstung_test AS SELECT rid, filename,doy, ST_Transform(rast, 25832, 'NearestNeighbor') AS rast FROM dwd.tempmittel WHERE doy = ",y,x,sep="")
    dbSendQuery(con, query_paste) # Abfrage Datenbank -> schreibe Tabelle für raster mit gesuchtem DOY
    print(x)
    pgGetRast(con,"dwd_verdunstung_test","rast") # Raster einladen
    
  })
  
  rast_st <- stack(rast)
  print(y)
  calc(rast_st,sum)  # Summe aller Monate
  
})

temp_st <- stack(temp)
temp_rast <- calc(temp_st,mean) # Mittelwert aus allen Jahren berechnen

extent(temp_rast) <- extent(P_1989_2018) # Extent an P Raster anpassen
writeRaster(temp_rast, filename="C:/Users/annelie.saeurich/Documents/DWD/Temp_summen.tif", format="GTiff", overwrite=TRUE)


