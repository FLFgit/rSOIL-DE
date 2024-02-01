#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#   Zuordnung BÜK200 Bodenprofile mittels 
########## CORINE2006 Landnutzung #########
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# 1.
# Übereinstimmende GEN_ID und CORINE Kultur?
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

#~~~~~~~~~~~~~~~~                                        #~~~~~~~~~~~~~~~~
# JA                                                     # NEIN 
#~~~~~~~~~~~~~~~~                                        #~~~~~~~~~~~~~~~~                  
                      
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%            #%%%%%%%%%%%%%%%%%
# 2.                                                     #Leitbodenprofil
# Zuordnung passendes Bodenprofil zu GL-Nutzung          #%%%%%%%%%%%%%%%%%
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

#~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2.1
# Was ist LEITBODENPROFIL?
#~~~~~~~~~~~~~~~~~~~~~~~~~~~    

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2.2
# Gibt es Profile mit MAX. FLÄCHENANTEIL?
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2.3
# Haben Profile GLEICHE FLÄCHENANTEILE?
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Aussortieren unggeigneter Bodenprofile aus 2.3 z.B. fehlen der GW-Stände oder anderer Parameter

# Zufallsauswahl für die restlichen BF_IDs

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# 3.
# Abschließende Zuordnung BF_ID
# No match Leitböden aus 1. + Leitböden aus 2.1 + Max. Flächenanteile aus 2.2 + ausgewählte Profile bei gleichen Flächenanteilen aus 2.3
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

library(data.table)
setwd("V:/BÜK200/CLC2006")

#~~~~~~~~~~~~~~~~~~~~~
# Grünland Code 231
#~~~~~~~~~~~~~~~~~~~~~

# Zuordnen Grünland Bodenprofil zu GEN_ID

cor <- fread("bük200_clc06_GL.csv") # intersect von BÜK200 und CLC2006 in QGIS, gefiltert nach 231 
length(unique(cor$tblZuordnu))
profil <- fread("./tblProfile_V06.csv",na.strings = c("", "NA"))

# wie viele der 2331 GEN_IDs fallen unter die CORINE Nutzung Code 231 (GL)?
cor[Code_06 == 231 ,Kultur_cor:="G"]
length(unique(profil[GEN_ID %in% cor$tblZuordnu]$GEN_ID)) # 2008 GL
length(unique(profil[!GEN_ID %in% cor$tblZuordnu]$GEN_ID)) # 323 kein GL, nicht weiter berücksichtigt bei der Zuordnung

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# 1.
# Übereinstimmende GEN_ID und CORINE Kultur?
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

#~~~~~~~~~~~~~~~~
# JA
#~~~~~~~~~~~~~~~~
match <- profil[GEN_ID %in% cor$tblZuordnu & KULTUR %in% cor$Kultur_cor] 
length(unique(match$GEN_ID)) # 1085 GEN_ID mit mind. einem Profil unter GL
#~~~~~~~~~~~~~~~~
# weiter zu 2.
#~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~
# NEIN 
#~~~~~~~~~~~~~~~~
# nicht Übereinstimmende GEN_ID und Kultur, laut BÜK200 keine GL-Nutzung
nomatch <- profil[!GEN_ID %in% unique(match$GEN_ID)][GEN_ID %in% unique(cor$tblZuordnu)]  
length(unique(nomatch$GEN_ID)) #923 GEN_IDS ohne passendes Bodenprofil der CORINE Nutzung
write.table(unique(nomatch$GEN_ID),"nomatch_gruen.csv",row.names=F,sep=";") 

#was ist leitbodenprofil??
nomatch_leit <- nomatch[,.SD[BOF_NR == 1], by=GEN_ID] # 923 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#--> Verwendung der entsprechenden Leitbodenprofile
# Leitbodenprofile der no-match GEN_ID
nomatch_leit  <- nomatch_leit[,.(GEN_ID,BF_ID)]
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# 2.
# Zuordnung passendes Bodenprofil zu GL-Nutzung
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# auch laut BÜK200 GL-nutzung
profil <- profil[GEN_ID %in% unique(match$GEN_ID) & KULTUR== "G"] # nur Verwendung von match

#~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2.1
# Was ist LEITBODENPROFIL?
#~~~~~~~~~~~~~~~~~~~~~~~~~~~
leitboden <- profil[,.SD[BOF_NR == 1], by=GEN_ID] #421

# Leitbodenprofile der match GEN_ID
leitboden  <- leitboden[,.(GEN_ID,BF_ID)]
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2.2
# Gibt es Profile mit MAX. FLÄCHENANTEIL?
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# maximale Flächenanteile
profil <- profil[!GEN_ID %in% leitboden$GEN_ID] # GEN_ID die keinen Leitboden GL haben

profil <- profil[profil[,.I[FLANT_MITTELW == max(FLANT_MITTELW)], by = GEN_ID]$V1] # Verwendung der maximalen Flächenanteile je GEN_ID, Dopplungen möglich
# GEN_ID mit genau einem passendem Bodenprofil
profil[,.SD[.N == 1], by=GEN_ID] # 487

# Bodenprofil mit max. Flächenanteil match GEN_ID
profil_max <- profil[,.SD[.N == 1], by=GEN_ID][,.(GEN_ID,BF_ID)]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2.3
# Haben Profile GLEICHE FLÄCHENANTEILE?
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# GEN_ID mit 2 Bodenprofilen BF_IDs (geteilt durch 2)
profil[,.SD[.N == 2], by=GEN_ID] # 124
# GEN_ID mit 3 Bodenprofilen BF_IDs / 3
profil[,.SD[.N == 3], by=GEN_ID] # 43
# GEN_ID mit 4 Bodenprofilen BF_IDs / 4
profil[,.SD[.N == 4], by=GEN_ID] # 5
# GEN_ID mit 5 Bodenprofilen BF_IDs / 5
profil[,.SD[.N == 5], by=GEN_ID] # 4 
# GEN_ID mit 6 Bodenprofilen BF_IDs / 6
profil[,.SD[.N == 6], by=GEN_ID] # 1

############################################
# Aussortieren unggeigneter Bodenprofile aus 2.3 
# z.B. fehlen der GW-Stände oder anderer Parameter
############################################

# GEN_ID mit 2 Bodenprofilen
#~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Ausscheiden der Profile ohne GW-Stand, wenn beide BF_IDs (SD[.N == 2]) betroffen, kein Ausscheiden
profil[,.SD[.N == 2], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID] # 5

raus <- profil[,.SD[.N == 2], by=GEN_ID][BF_ID %in% profil[,.SD[.N == 2], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID]$BF_ID]
profil22 <-profil[,.SD[.N == 2], by=GEN_ID][!GEN_ID %in% profil[,.SD[.N == 2], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID]$GEN_ID][,.(GEN_ID,BF_ID)]

hor <- fread("./tblHorizonte_V06.csv",na.strings = c("", "NA"))

# NAs in wichtigen Bodenparametern?
# pH-Wert
ph <- as.data.table(unique (hor[BF_ID %in% profil22$BF_ID][(is.na(PH)|!nzchar(PH))]$BF_ID))
ph <- profil22[BF_ID %in% ph$V1][,.SD[.N == 1], by=GEN_ID] # 11

raus <- rbind(raus, profil[,.SD[.N == 2], by=GEN_ID][BF_ID %in% ph$BF_ID])
profil22 <- profil22[!GEN_ID %in% ph$GEN_ID]

# LD
ld <- as.data.table(unique (hor[BF_ID %in% profil22$BF_ID][(is.na(LD)|!nzchar(LD))]$BF_ID))
ld <- profil22[BF_ID %in% ld$V1][,.SD[.N == 1], by=GEN_ID] # 9

raus <- rbind(raus, profil[,.SD[.N == 2], by=GEN_ID][BF_ID %in% ld$BF_ID])
profil22 <- profil22[!GEN_ID %in% ld$GEN_ID]

#Skelett
grob <- as.data.table(unique (hor[BF_ID %in% profil22$BF_ID][(is.na(GROBBOD_K)|!nzchar(GROBBOD_K))]$BF_ID))
grob <- profil22[BF_ID %in% grob$V1][,.SD[.N == 1], by=GEN_ID] # 0

raus <- rbind(raus, profil[,.SD[.N == 2], by=GEN_ID][BF_ID %in% grob$BF_ID])
profil22 <- profil22[!GEN_ID %in% grob$GEN_ID]

# HUmus
humus <- as.data.table(unique (hor[BF_ID %in% profil22$BF_ID][(is.na(HUMUS)|!nzchar(HUMUS))]$BF_ID))
humus <- profil22[BF_ID %in% humus$V1][,.SD[.N == 1], by=GEN_ID] # 0

raus <- rbind(raus, profil[,.SD[.N == 2], by=GEN_ID][BF_ID %in% humus$BF_ID])
profil22 <- profil22[!GEN_ID %in% humus$GEN_ID]

# GEfüge
gef <- as.data.table(unique (hor[BF_ID %in% profil22$BF_ID][(is.na(GEFUEGE)|!nzchar(GEFUEGE))]$BF_ID))
gef <- profil22[BF_ID %in% gef$V1][,.SD[.N == 1], by=GEN_ID] #2

raus <- rbind(raus, profil[,.SD[.N == 2], by=GEN_ID][BF_ID %in% gef$BF_ID])
profil22 <- profil22[!GEN_ID %in% gef$GEN_ID] # 69

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 27 BF_IDS eliminiert
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# wurden GEN_IDs komplett eliminiert?
raus[,.SD[.N == 2], by=GEN_ID] # 0

profil2 <- profil[,.SD[.N == 2], by=GEN_ID][!BF_ID %in% raus$BF_ID]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Zufallsauswahl für die restlichen BF_IDs
profil2 <- profil2[,.SD[sample(.N, 1)], by = GEN_ID]

# Summe ausgewählter BF_IDs
length(unique(profil2$GEN_ID)) #124

########################################################

# GEN_ID mit 3 Bodenprofilen
#~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Auswahl das Profils mit GW-Stand

# 2 von 3 BF_ID sind NA
profil[,.SD[.N == 3], by=GEN_ID][is.na(MGW)][,.SD[.N == 2], by=GEN_ID] # 3 
raus3 <- profil[,.SD[.N == 3], by=GEN_ID][BF_ID %in% profil[,.SD[.N == 3], by=GEN_ID][is.na(MGW)][,.SD[.N == 2], by=GEN_ID]$BF_ID]

# GEN_IDs aus Profil32 entfernen
profil32 <-profil[,.SD[.N == 3], by=GEN_ID][!GEN_ID %in% profil[,.SD[.N == 3], by=GEN_ID][is.na(MGW)][,.SD[.N == 2], by=GEN_ID]$GEN_ID][,.(GEN_ID,BF_ID)]

# 1 von 3 BF_ID sind NA
profil[,.SD[.N == 3], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID] # 1 
raus3 <- rbind(raus3, profil[,.SD[.N == 3], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID])
# BF_IDs aus Profil32 entfernen
profil32 <- profil32[!BF_ID %in% profil[,.SD[.N == 3], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID]$BF_ID]

# NAs in wichtigen BOdenparametern?

# pH-Wert
ph <- as.data.table(unique (hor[BF_ID %in% profil32$BF_ID][(is.na(PH)|!nzchar(PH))]$BF_ID))
# 2 von 3 BF_ID sind NA
ph2 <- profil32[BF_ID %in% ph$V1][,.SD[.N == 2], by=GEN_ID] # 3
# 1 von 3 BF_ID sind NA
ph1 <- profil32[BF_ID %in% ph$V1][,.SD[.N == 1], by=GEN_ID] # 3

raus3 <- rbind(raus3, profil[,.SD[.N == 3], by=GEN_ID][BF_ID %in% ph2$BF_ID])
raus3 <- rbind(raus3, profil[,.SD[.N == 3], by=GEN_ID][BF_ID %in% ph1$BF_ID])

# GEN_IDs aus Profil32 entfernen
profil32 <- profil32[!GEN_ID %in% ph2$GEN_ID]
# BF_IDs aus Profil32 entfernen
profil32 <- profil32[!BF_ID %in% ph1$BF_ID]

# LD
ld <- as.data.table(unique (hor[BF_ID %in% profil32$BF_ID][(is.na(LD)|!nzchar(LD))]$BF_ID))
# 2 von 3 BF_ID sind NA
ld2 <- profil32[BF_ID %in% ld$V1][,.SD[.N == 2], by=GEN_ID] # 2
# 1 von 3 BF_ID sind NA
ld1 <- profil32[BF_ID %in% ld$V1][,.SD[.N == 1], by=GEN_ID] # 3

raus3 <- rbind(raus3, profil[,.SD[.N == 3], by=GEN_ID][BF_ID %in% ld2$BF_ID])
raus3 <- rbind(raus3, profil[,.SD[.N == 3], by=GEN_ID][BF_ID %in% ld1$BF_ID])

# GEN_IDs aus Profil32 entfernen
profil32 <- profil32[!GEN_ID %in% ld2$GEN_ID]
# BF_IDs aus Profil32 entfernen
profil32 <- profil32[!BF_ID %in% ld1$BF_ID]

#Skelett
grob <- as.data.table(unique (hor[BF_ID %in% profil32$BF_ID][(is.na(GROBBOD_K)|!nzchar(GROBBOD_K))]$BF_ID))
# 2 von 3 BF_ID sind NA
grob2 <- profil32[BF_ID %in% grob$V1][,.SD[.N == 2], by=GEN_ID] # 0
# 1 von 3 BF_ID sind NA
grob1 <- profil32[BF_ID %in% grob$V1][,.SD[.N == 1], by=GEN_ID] # 0

#profil32 <- profil32[!GEN_ID %in% grob$GEN_ID]

# HUmus
humus <- as.data.table(unique (hor[BF_ID %in% profil32$BF_ID][(is.na(HUMUS)|!nzchar(HUMUS))]$BF_ID))
# 2 von 3 BF_ID sind NA
humus2 <- profil32[BF_ID %in% humus$V1][,.SD[.N == 2], by=GEN_ID] # 0
# 1 von 3 BF_ID sind NA
humus1 <- profil32[BF_ID %in% humus$V1][,.SD[.N == 1], by=GEN_ID] # 0

#profil32 <- profil32[!GEN_ID %in% humus$GEN_ID]

# GEfüge
gef <- as.data.table(unique (hor[BF_ID %in% profil32$BF_ID][(is.na(GEFUEGE)|!nzchar(GEFUEGE))]$BF_ID))
# 2 von 3 BF_ID sind NA
gef2 <- profil32[BF_ID %in% gef$V1][,.SD[.N == 2], by=GEN_ID] #3
# 1 von 3 BF_ID sind NA
gef1 <- profil32[BF_ID %in% gef$V1][,.SD[.N == 1], by=GEN_ID] #0

raus3 <- rbind(raus3, profil[,.SD[.N == 3], by=GEN_ID][BF_ID %in% gef2$BF_ID])
raus3 <- rbind(raus3, profil[,.SD[.N == 3], by=GEN_ID][BF_ID %in% gef1$BF_ID])

# GEN_IDs aus Profil32 entfernen
profil32 <- profil32[!GEN_ID %in% gef2$GEN_ID]
# BF_IDs aus Profil32 entfernen
profil32 <- profil32[!BF_ID %in% gef1$BF_ID]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 29 BF_IDs eliminiert
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# wurden GEN_IDs komplett eliminiert?
raus3[,.SD[.N == 3], by=GEN_ID] # 3: 744, 1496, 125

#744: 3052 raus (kein PH) 
raus3 <- raus3[!BF_ID %in% c("3054","3055")]
#1496: 6299 raus (kein PH) 
raus3 <- raus3[!BF_ID %in% c("6297","6298")]
#125: alle gleichwertig
raus3 <- raus3[!GEN_ID =="125"]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 22 BF_IDs eliminiert
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

profil3 <- profil[,.SD[.N == 3], by=GEN_ID][!BF_ID %in% raus3$BF_ID]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Zufallsauswahl für die restlichen BF_IDs

profil3 <- profil3[,.SD[sample(.N, 1)], by = GEN_ID]

# Summe ausgewählter BF_IDs
length(unique(profil3$GEN_ID)) #43

#########################################################


# GEN_ID mit 4 Bodenprofilen
#~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Auswahl das Profils mit GW-Stand
profil[,.SD[.N == 4], by=GEN_ID][is.na(MGW)][,.SD[.N == 2], by=GEN_ID] # 0
profil[,.SD[.N == 4], by=GEN_ID][is.na(MGW)][,.SD[.N == 3], by=GEN_ID] # 0
profil[,.SD[.N == 4], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID] # 0

raus4 <- profil[,.SD[.N == 4], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID]
# BF_IDs aus Profil32 entfernen
profil42 <- profil[,.SD[.N == 4], by=GEN_ID][!BF_ID %in% profil[,.SD[.N == 4], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID]$BF_ID][,.(GEN_ID,BF_ID)]

# NAs in wichtigen Bodenparametern?

# pH-Wert
ph <- as.data.table(unique (hor[BF_ID %in% profil42$BF_ID][(is.na(PH)|!nzchar(PH))]$BF_ID))
# 3 von 4 BF_ID sind NA
ph3 <- profil42[BF_ID %in% ph$V1][,.SD[.N == 3], by=GEN_ID] # 0
# 2 von 4 BF_ID sind NA
ph2 <- profil42[BF_ID %in% ph$V1][,.SD[.N == 2], by=GEN_ID] # 0
# 1 von 4 BF_ID sind NA
ph1 <- profil42[BF_ID %in% ph$V1][,.SD[.N == 1], by=GEN_ID] # 0

raus4 <- rbind(raus4, profil[,.SD[.N == 4], by=GEN_ID][BF_ID %in% ph3$BF_ID])

# BF_IDs aus profil42 entfernen
profil42 <- profil42[!BF_ID %in% ph3$BF_ID]

# LD
ld <- as.data.table(unique (hor[BF_ID %in% profil42$BF_ID][(is.na(LD)|!nzchar(LD))]$BF_ID)) #0
# 3 von 4 BF_ID sind NA
ld3 <- profil42[BF_ID %in% ld$V1][,.SD[.N == 3], by=GEN_ID] # 0
# 2 von 4 BF_ID sind NA
ld2 <- profil42[BF_ID %in% ld$V1][,.SD[.N == 2], by=GEN_ID] # 0
# 1 von 4 BF_ID sind NA
ld1 <- profil42[BF_ID %in% ld$V1][,.SD[.N == 1], by=GEN_ID] # 1

# BF_IDs aus Profil42 entfernen
raus4 <- rbind(raus4, profil[,.SD[.N == 4], by=GEN_ID][BF_ID %in% ld1$BF_ID])
profil42 <- profil42[!BF_ID %in% ld1$BF_ID]

#Skelett
grob <- as.data.table(unique (hor[BF_ID %in% profil42$BF_ID][(is.na(GROBBOD_K)|!nzchar(GROBBOD_K))]$BF_ID)) #0

# Humus
humus <- as.data.table(unique (hor[BF_ID %in% profil42$BF_ID][(is.na(HUMUS)|!nzchar(HUMUS))]$BF_ID)) #0

# GEfüge
gef <- as.data.table(unique (hor[BF_ID %in% profil42$BF_ID][(is.na(GEFUEGE)|!nzchar(GEFUEGE))]$BF_ID))  #0
# 3 von 4 BF_ID sind NA
gef3 <- profil42[BF_ID %in% gef$V1][,.SD[.N == 3], by=GEN_ID] # 0
# 2 von 4 BF_ID sind NA
gef2 <- profil42[BF_ID %in% gef$V1][,.SD[.N == 2], by=GEN_ID] # 0
# 1 von 4 BF_ID sind NA
gef1 <- profil42[BF_ID %in% gef$V1][,.SD[.N == 1], by=GEN_ID] # 0

# BF_IDs aus Profil42 entfernen
raus4 <- rbind(raus4, profil[,.SD[.N == 4], by=GEN_ID][BF_ID %in% gef1$BF_ID])
profil42 <- profil42[!BF_ID %in% gef1$BF_ID]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 4 BF_IDs eliminiert
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# wurden GEN_IDs komplett eliminiert?
raus4[,.SD[.N == 4], by=GEN_ID] # 0

profil4 <- profil[,.SD[.N == 4], by=GEN_ID][!BF_ID %in% raus4$BF_ID]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Zufallsauswahl für die restlichen BF_IDs
profil4 <- profil4[,.SD[sample(.N, 1)], by = GEN_ID]

# Summe ausgewählter BF_IDs
length(unique(profil4$GEN_ID)) #5 

########################################
#########################################################

# GEN_ID mit 5 Bodenprofilen
#~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Auswahl das Profils mit GW-Stand
profil[,.SD[.N == 5], by=GEN_ID][is.na(MGW)][,.SD[.N == 2], by=GEN_ID] # 0
profil[,.SD[.N == 5], by=GEN_ID][is.na(MGW)][,.SD[.N == 3], by=GEN_ID] # 0
profil[,.SD[.N == 5], by=GEN_ID][is.na(MGW)][,.SD[.N == 4], by=GEN_ID] # 1
profil[,.SD[.N == 5], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID] # 0

raus5 <- profil[,.SD[.N == 5], by=GEN_ID][is.na(MGW)][,.SD[.N == 4], by=GEN_ID]
# GEN_ID aus Profil52 entfernen
profil52 <- profil[,.SD[.N == 5], by=GEN_ID][!BF_ID %in% profil[,.SD[.N == 5], by=GEN_ID][is.na(MGW)][,.SD[.N == 4], by=GEN_ID]$BF_ID][,.(GEN_ID,BF_ID)]

# NAs in wichtigen Bodenparametern?
# pH-Wert
ph <- as.data.table(unique (hor[BF_ID %in% profil52$BF_ID][(is.na(PH)|!nzchar(PH))]$BF_ID)) #0

# LD
ld <- as.data.table(unique (hor[BF_ID %in% profil52$BF_ID][(is.na(LD)|!nzchar(LD))]$BF_ID)) #4
# 2 von 5 BF_ID sind NA
ld2 <- profil52[BF_ID %in% ld$V1][,.SD[.N == 2], by=GEN_ID] # 4

# BF_IDs aus Profil42 entfernen
raus5 <- rbind(raus5, profil[,.SD[.N == 5], by=GEN_ID][BF_ID %in% ld2$BF_ID])
profil52 <- profil52[!BF_ID %in% ld2$BF_ID]

#Skelett
grob <- as.data.table(unique (hor[BF_ID %in% profil52$BF_ID][(is.na(GROBBOD_K)|!nzchar(GROBBOD_K))]$BF_ID)) #0

# HUmus
humus <- as.data.table(unique (hor[BF_ID %in% profil52$BF_ID][(is.na(HUMUS)|!nzchar(HUMUS))]$BF_ID)) #0

# GEfüge
gef <- as.data.table(unique (hor[BF_ID %in% profil52$BF_ID][(is.na(GEFUEGE)|!nzchar(GEFUEGE))]$BF_ID))  #0

#~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 4 BF_IDs eliminiert
#~~~~~~~~~~~~~~~~~~~~~~~~~~~

# wurden GEN_IDs komplett eliminiert?
raus5[,.SD[.N == 5], by=GEN_ID] # 0

profil5 <- profil[,.SD[.N == 5], by=GEN_ID][!BF_ID %in% raus5$BF_ID]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Zufallsauswahl für die restlichen BF_IDs
profil5 <- profil5[,.SD[sample(.N, 1)], by = GEN_ID]

# Summe ausgewählter BF_IDs
length(unique(profil5$GEN_ID)) #4

################################

# GEN_ID mit 6 Bodenprofilen
#~~~~~~~~~~~~~~~~~~~~~~~~~~~

profil6 <- profil[,.SD[.N == 6], by=GEN_ID]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Zufallsauswahl für die restlichen BF_IDs

profil6 <- profil6[,.SD[sample(.N, 1)], by = GEN_ID]

#################################################
# Gesamtbetrachtung gleicher Flächenanteil BF_IDs
#################################################

profil_sim <- rbind(profil2,profil3,profil4,profil5,profil6)[,.(GEN_ID,BF_ID)]
write.table(profil_sim,"gleicher_Anteil_GL.csv",row.names=F,sep=";")

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# Abschließende Zuordnung BF_ID

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

gesamt <- rbind(nomatch_leit,leitboden,profil_max, profil_sim)

gesamt_hor_GL <- hor[BF_ID %in% gesamt$BF_ID]
gesamt_hor_GL[,CORINE:="GL"]
write.table(gesamt_hor_GL,"Horizonte_GL.csv",row.names=F,sep=";")

profil <- fread("../tblProfile_V06.csv")
gesamt_profil_GL <- profil[BF_ID %in% gesamt$BF_ID]
gesamt_profil_GL[,CORINE:="GL"]
write.table(gesamt_profil_GL,"Profile_GL.csv",row.names=F,sep=";") # anschließender Join mit GL_shape


##############################################################################################################################################
##############################################################################################################################################
##############################################################################################################################################

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Ackerland Code 211, 221, 222, 242, 243
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Zuordnen Acker Bodenprofil zu GEN_ID

library(data.table)

cor <- fread("bük200_clc06_A.csv")
profil <- fread("./tblProfile_V06.csv",na.strings = c("", "NA"))

# gibts es zu jedem CORINE Code ein passendes Bodenprofil?
cor[Code_06 %in% c(211,243,242),Kultur_cor:="A"][Code_06 %in% c(221,222),Kultur_cor:="S"]
length(unique(profil[GEN_ID %in% cor$tblZuordnu]$GEN_ID)) # 2102 Acker
length(unique(profil[!GEN_ID %in% cor$tblZuordnu]$GEN_ID)) # 229 kein Acker, nicht weiter berücksichigt bei der Zuordnung

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# 1.
# Übereinstimmende GEN_ID und CORINE Kultur?
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

#~~~~~~~~~~~~~~~~
# JA
#~~~~~~~~~~~~~~~~
match <- profil[GEN_ID %in% cor$tblZuordnu & KULTUR %in% cor$Kultur_cor] # Übereinstimmende GEN_ID und Kultur
length(unique(match$GEN_ID)) # 1479
#~~~~~~~~~~~~~~~~
# weiter zu 2.
#~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~
# NEIN 
#~~~~~~~~~~~~~~~~
# nicht Übereinstimmende GEN_ID und Kultur, laut BÜK200 keine Ackernutzung
nomatch <- profil[!GEN_ID %in% unique(match$GEN_ID)][GEN_ID %in% unique(cor$tblZuordnu)] # NICHT Übereinstimmende GEN_ID und Kultur
length(unique(nomatch$GEN_ID)) #623 GEN_IDS ohne passendes Bodenprofil der CORINE Nutzung
write.table(unique(nomatch$GEN_ID),"nomatch.csv",row.names=F,sep=";") 

#was ist leitbodenprofil??
nomatch_leit <- nomatch[,.SD[BOF_NR == 1], by=GEN_ID] # 623 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# --> Verwendung der entsprechenden Leitbodenprofile
# Leitbodenprofile der no-match GEN_ID
nomatch_leit  <- nomatch_leit[,.(GEN_ID,BF_ID)]
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# 2.
# Zuordnung passendes Bodenprofil zu Acker-Nutzung
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# auch laut BÜK200 Ackernutzung
profil <- profil[GEN_ID %in% unique(match$GEN_ID) & KULTUR %in% c("A", "BA", "S")] # nur Verwendung von match

#~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2.1
# Was ist LEITBODENPROFIL?
#~~~~~~~~~~~~~~~~~~~~~~~~~~~
leitboden <- profil[,.SD[BOF_NR == 1], by=GEN_ID] #1054

# Leitbodenprofile der match GEN_ID
leitboden  <- leitboden[,.(GEN_ID,BF_ID)]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2.2
# Gibt es Profile mit MAX. FLÄCHENANTEIL?
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# maximale Flächenanteile
profil <- profil[!GEN_ID %in% leitboden$GEN_ID] # GEN_ID die keinen Leitboden A oder S haben

profil <- profil[profil[,.I[FLANT_MITTELW == max(FLANT_MITTELW)], by = GEN_ID]$V1] # Verwendung der maximalen Flächenanteile je GEN_ID, Dopplungen möglich

# GEN_ID mit genau einem passendem Bodenprofil
profil[,.SD[.N == 1], by=GEN_ID] # 302

# Bodenprofil mit max. Flächenanteil match GEN_ID
profil_max <- profil[,.SD[.N == 1], by=GEN_ID][,.(GEN_ID,BF_ID)]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2.3
# Haben Profile GLEICHE FLÄCHENANTEILE?
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# GEN_ID mit 2 Bodenprofilen BF_IDs (geteilt durch 2)
profil[,.SD[.N == 2], by=GEN_ID] # 91
# GEN_ID mit 3 Bodenprofilen
profil[,.SD[.N == 3], by=GEN_ID] # 29
# GEN_ID mit 4 Bodenprofilen
profil[,.SD[.N == 4], by=GEN_ID] # 2
# GEN_ID mit 5 Bodenprofilen
profil[,.SD[.N == 5], by=GEN_ID] # 1 --> BF_ID: 9695

############################################
# Aussortieren unggeigneter Bodenprofile aus 2.3 
# z.B. fehlen der GW-Stände oder anderer Parameter
############################################

# GEN_ID mit 2 Bodenprofilen
#~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Ausscheiden der Profile ohne GW-Stand, wenn beide BF_IDs (SD[.N == 2]) betroffen, kein Ausscheiden
profil[,.SD[.N == 2], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID] # 6

raus <- profil[,.SD[.N == 2], by=GEN_ID][BF_ID %in% profil[,.SD[.N == 2], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID]$BF_ID]
profil2 <- profil[,.SD[.N == 2], by=GEN_ID][!GEN_ID %in% profil[,.SD[.N == 2], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID]$GEN_ID]

profil22 <-profil[,.SD[.N == 2], by=GEN_ID][!GEN_ID %in% profil[,.SD[.N == 2], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID]$GEN_ID][,.(GEN_ID,BF_ID)]
hor <- fread("./tblHorizonte_V06.csv",na.strings = c("", "NA"))

# NAs in wichtigen Bodenparametern?
# pH-Wert
ph <- as.data.table(unique (hor[BF_ID %in% profil22$BF_ID][(is.na(PH)|!nzchar(PH))]$BF_ID))
ph <- profil22[BF_ID %in% ph$V1][,.SD[.N == 1], by=GEN_ID] # 9

raus <- rbind(raus, profil[,.SD[.N == 2], by=GEN_ID][BF_ID %in% ph$BF_ID])
profil22 <- profil22[!GEN_ID %in% ph$GEN_ID]

# LD
ld <- as.data.table(unique (hor[BF_ID %in% profil22$BF_ID][(is.na(LD)|!nzchar(LD))]$BF_ID))
ld <- profil22[BF_ID %in% ld$V1][,.SD[.N == 1], by=GEN_ID] # 5

raus <- rbind(raus, profil[,.SD[.N == 2], by=GEN_ID][BF_ID %in% ld$BF_ID])
profil22 <- profil22[!GEN_ID %in% ld$GEN_ID]

#Skelett
grob <- as.data.table(unique (hor[BF_ID %in% profil22$BF_ID][(is.na(GROBBOD_K)|!nzchar(GROBBOD_K))]$BF_ID))
grob <- profil22[BF_ID %in% grob$V1][,.SD[.N == 1], by=GEN_ID] # 0

raus <- rbind(raus, profil[,.SD[.N == 2], by=GEN_ID][BF_ID %in% grob$BF_ID])
profil22 <- profil22[!GEN_ID %in% grob$GEN_ID]

# Humus
humus <- as.data.table(unique (hor[BF_ID %in% profil22$BF_ID][(is.na(HUMUS)|!nzchar(HUMUS))]$BF_ID))
humus <- profil22[BF_ID %in% humus$V1][,.SD[.N == 1], by=GEN_ID] # 0

raus <- rbind(raus, profil[,.SD[.N == 2], by=GEN_ID][BF_ID %in% humus$BF_ID])
profil22 <- profil22[!GEN_ID %in% humus$GEN_ID]

# Gefüge
gef <- as.data.table(unique (hor[BF_ID %in% profil22$BF_ID][(is.na(GEFUEGE)|!nzchar(GEFUEGE))]$BF_ID))
gef <- profil22[BF_ID %in% gef$V1][,.SD[.N == 1], by=GEN_ID] #1

raus <- rbind(raus, profil[,.SD[.N == 2], by=GEN_ID][BF_ID %in% gef$BF_ID])
profil22 <- profil22[!GEN_ID %in% gef$GEN_ID] # 69

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 21 BF_IDS eliminiert
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# wurden GEN_IDs komplett eliminiert?
raus[,.SD[.N == 2], by=GEN_ID] # 0

profil2 <- profil[,.SD[.N == 2], by=GEN_ID][!BF_ID %in% raus$BF_ID]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Zufallsauswahl für die restlichen BF_IDs
profil2 <- profil2[,.SD[sample(.N, 1)], by = GEN_ID]

# Summe ausgewählter BF_IDs
length(unique(profil2$GEN_ID)) #91

########################################################

# GEN_ID mit 3 Bodenprofilen
#~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Auswahl das Profils mit GW-Stand

# 2 von 3 BF_ID sind NA
profil[,.SD[.N == 3], by=GEN_ID][is.na(MGW)][,.SD[.N == 2], by=GEN_ID] # 1 
raus3 <- profil[,.SD[.N == 3], by=GEN_ID][BF_ID %in% profil[,.SD[.N == 3], by=GEN_ID][is.na(MGW)][,.SD[.N == 2], by=GEN_ID]$BF_ID]

# GEN_IDs aus Profil32 entfernen
profil32 <-profil[,.SD[.N == 3], by=GEN_ID][!GEN_ID %in% profil[,.SD[.N == 3], by=GEN_ID][is.na(MGW)][,.SD[.N == 2], by=GEN_ID]$GEN_ID][,.(GEN_ID,BF_ID)]

# 1 von 3 BF_ID sind NA
profil[,.SD[.N == 3], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID] # 3 
raus3 <- rbind(raus3, profil[,.SD[.N == 3], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID])
# BF_IDs aus Profil32 entfernen
profil32 <- profil32[!BF_ID %in% profil[,.SD[.N == 3], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID]$BF_ID]

# NAs in wichtigen Bodenparametern?

# pH-Wert
ph <- as.data.table(unique (hor[BF_ID %in% profil32$BF_ID][(is.na(PH)|!nzchar(PH))]$BF_ID))
# 2 von 3 BF_ID sind NA
ph2 <- profil32[BF_ID %in% ph$V1][,.SD[.N == 2], by=GEN_ID] # 3
# 1 von 3 BF_ID sind NA
ph1 <- profil32[BF_ID %in% ph$V1][,.SD[.N == 1], by=GEN_ID] # 2

raus3 <- rbind(raus3, profil[,.SD[.N == 3], by=GEN_ID][BF_ID %in% ph2$BF_ID])
raus3 <- rbind(raus3, profil[,.SD[.N == 3], by=GEN_ID][BF_ID %in% ph1$BF_ID])

# GEN_IDs aus Profil32 entfernen
profil32 <- profil32[!GEN_ID %in% ph2$GEN_ID]
# BF_IDs aus Profil32 entfernen
profil32 <- profil32[!BF_ID %in% ph1$BF_ID]

# LD
ld <- as.data.table(unique (hor[BF_ID %in% profil32$BF_ID][(is.na(LD)|!nzchar(LD))]$BF_ID))
# 2 von 3 BF_ID sind NA
ld2 <- profil32[BF_ID %in% ld$V1][,.SD[.N == 2], by=GEN_ID] # 1
# 1 von 3 BF_ID sind NA
ld1 <- profil32[BF_ID %in% ld$V1][,.SD[.N == 1], by=GEN_ID] # 1

raus3 <- rbind(raus3, profil[,.SD[.N == 3], by=GEN_ID][BF_ID %in% ld2$BF_ID])
raus3 <- rbind(raus3, profil[,.SD[.N == 3], by=GEN_ID][BF_ID %in% ld1$BF_ID])

# GEN_IDs aus Profil32 entfernen
profil32 <- profil32[!GEN_ID %in% ld2$GEN_ID]
# BF_IDs aus Profil32 entfernen
profil32 <- profil32[!BF_ID %in% ld1$BF_ID]

#Skelett
grob <- as.data.table(unique (hor[BF_ID %in% profil32$BF_ID][(is.na(GROBBOD_K)|!nzchar(GROBBOD_K))]$BF_ID))
# 2 von 3 BF_ID sind NA
grob2 <- profil32[BF_ID %in% grob$V1][,.SD[.N == 2], by=GEN_ID] # 0
# 1 von 3 BF_ID sind NA
grob1 <- profil32[BF_ID %in% grob$V1][,.SD[.N == 1], by=GEN_ID] # 0

#profil32 <- profil32[!GEN_ID %in% grob$GEN_ID]

# Humus
humus <- as.data.table(unique (hor[BF_ID %in% profil32$BF_ID][(is.na(HUMUS)|!nzchar(HUMUS))]$BF_ID))
# 2 von 3 BF_ID sind NA
humus2 <- profil32[BF_ID %in% humus$V1][,.SD[.N == 2], by=GEN_ID] # 0
# 1 von 3 BF_ID sind NA
humus1 <- profil32[BF_ID %in% humus$V1][,.SD[.N == 1], by=GEN_ID] # 0

#profil32 <- profil32[!GEN_ID %in% humus$GEN_ID]

# Gefüge
gef <- as.data.table(unique (hor[BF_ID %in% profil32$BF_ID][(is.na(GEFUEGE)|!nzchar(GEFUEGE))]$BF_ID))
# 2 von 3 BF_ID sind NA
gef2 <- profil32[BF_ID %in% gef$V1][,.SD[.N == 2], by=GEN_ID] #2
# 1 von 3 BF_ID sind NA
gef1 <- profil32[BF_ID %in% gef$V1][,.SD[.N == 1], by=GEN_ID] #0

raus3 <- rbind(raus3, profil[,.SD[.N == 3], by=GEN_ID][BF_ID %in% gef2$BF_ID])
raus3 <- rbind(raus3, profil[,.SD[.N == 3], by=GEN_ID][BF_ID %in% gef1$BF_ID])

# GEN_IDs aus Profil32 entfernen
profil32 <- profil32[!GEN_ID %in% gef2$GEN_ID]
# BF_IDs aus Profil32 entfernen
profil32 <- profil32[!BF_ID %in% gef1$BF_ID]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 20 BF_IDs eliminiert
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# wurden GEN_IDs komplett eliminiert?
raus3[,.SD[.N == 3], by=GEN_ID] # 4: 549, 1163, 2130, 243

#549: 2695 raus (kein GW) 
raus3 <- raus3[!BF_ID %in% c("2696","2697")]
#1163: 5363 raus (kein GW) 
raus3 <- raus3[!BF_ID %in% c("5361","5362")]
#2130: alle gleichwertig
raus3 <- raus3[!GEN_ID =="2130"]
#243 alle gleichwertig
raus3 <- raus3[!GEN_ID =="243"]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 10 BF_IDs eliminiert
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

profil3 <- profil[,.SD[.N == 3], by=GEN_ID][!BF_ID %in% raus3$BF_ID]

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Zufallsauswahl für die restlichen BF_IDs

profil3 <- profil3[,.SD[sample(.N, 1)], by = GEN_ID]

# Summe ausgewählter BF_IDs
length(unique(profil3$GEN_ID)) #29
#########################################################

# GEN_ID mit 4 Bodenprofilen
#~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Auswahl das Profils mit GW-Stand
profil[,.SD[.N == 4], by=GEN_ID][is.na(MGW)][,.SD[.N == 2], by=GEN_ID] # 0
profil[,.SD[.N == 4], by=GEN_ID][is.na(MGW)][,.SD[.N == 3], by=GEN_ID] # 0
profil[,.SD[.N == 4], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID] # 1

raus4 <- profil[,.SD[.N == 4], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID]
# BF_IDs aus Profil32 entfernen
profil42 <- profil[,.SD[.N == 4], by=GEN_ID][!BF_ID %in% profil[,.SD[.N == 4], by=GEN_ID][is.na(MGW)][,.SD[.N == 1], by=GEN_ID]$BF_ID][,.(GEN_ID,BF_ID)]

# NAs in wichtigen Bodenparametern?

# pH-Wert
ph <- as.data.table(unique (hor[BF_ID %in% profil42$BF_ID][(is.na(PH)|!nzchar(PH))]$BF_ID))
# 3 von 4 BF_ID sind NA
ph3 <- profil42[BF_ID %in% ph$V1][,.SD[.N == 3], by=GEN_ID] # 1
# 2 von 4 BF_ID sind NA
ph2 <- profil42[BF_ID %in% ph$V1][,.SD[.N == 2], by=GEN_ID] # 0
# 1 von 4 BF_ID sind NA
ph1 <- profil42[BF_ID %in% ph$V1][,.SD[.N == 1], by=GEN_ID] # 0

raus4 <- rbind(raus4, profil[,.SD[.N == 4], by=GEN_ID][BF_ID %in% ph3$BF_ID])

# BF_IDs aus profil42 entfernen
profil42 <- profil42[!BF_ID %in% ph3$BF_ID]

# LD
ld <- as.data.table(unique (hor[BF_ID %in% profil42$BF_ID][(is.na(LD)|!nzchar(LD))]$BF_ID)) #0

#Skelett
grob <- as.data.table(unique (hor[BF_ID %in% profil42$BF_ID][(is.na(GROBBOD_K)|!nzchar(GROBBOD_K))]$BF_ID)) #0

# Humus
humus <- as.data.table(unique (hor[BF_ID %in% profil42$BF_ID][(is.na(HUMUS)|!nzchar(HUMUS))]$BF_ID)) #0

# Gefüge
gef <- as.data.table(unique (hor[BF_ID %in% profil42$BF_ID][(is.na(GEFUEGE)|!nzchar(GEFUEGE))]$BF_ID))  #0

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 4 BF_IDs eliminiert
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# wurden GEN_IDs komplett eliminiert?
raus4[,.SD[.N == 4], by=GEN_ID] # 1: 1146

#1146: 2729 raus (kein GW)
raus4 <- raus4[BF_ID=="4729"]

profil4 <- profil[,.SD[.N == 4], by=GEN_ID][!BF_ID %in% raus4$BF_ID]

# Zufallsauswahl für die restlichen BF_IDs
profil4 <- profil4[,.SD[sample(.N, 1)], by = GEN_ID]

# Summe ausgewählter BF_IDs
length(unique(profil4$GEN_ID)) #2

#################################################
# Gesamtbetrachtung gleicher Flächenanteil BF_IDs
#################################################

profil_sim <- rbind(profil2,profil3,profil4,profil[,.SD[.N == 5], by=GEN_ID][BF_ID=="9695"])[,.(GEN_ID,BF_ID)] # GEN_ID mit 5 Profilen
write.table(profil_sim,"gleicher_Anteil_A.csv",row.names=F,sep=";")

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

# Abschließende Zuordnung BF_ID

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

gesamt <- rbind(nomatch_leit,leitboden,profil_max, profil_sim)

gesamt_hor_A <- hor[BF_ID %in% gesamt$BF_ID]
gesamt_hor_A[,CORINE:="A"]
write.table(gesamt_hor_A,"Horizonte_A.csv",row.names=F,sep=";")

profil <- fread("../tblProfile_V06.csv")
gesamt_profil_A <- profil[BF_ID %in% gesamt$BF_ID]
gesamt_profil_A[,CORINE:="A"]
write.table(gesamt_profil_A,"Profile_A.csv",row.names=F,sep=";") # anschließender Join mit Acker_shape

#########################################

# Acker und Grünland

gesamt_bük200 <- rbind(gesamt_profil_A,gesamt_profil_GL)
unique(gesamt_bük200$BF_ID)
