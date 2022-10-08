library(tidyr)
library(yaml)
library(magrittr)
library(stringr)
library(dplyr)
library(lubridate)
library(fs)
library(readr)
library(futile.logger)
library(jsonlite)

gidsweek <- fromJSON("C:/cz_salsa/gidsweek_uploaden/gidsteksten_20221006.json") 

gidsweek.2 <- gidsweek[[2]] %>% as_tibble() %>% mutate(gids_date = names(gidsweek[1]))


gids_uploads <- dir_ls("C:/cz_salsa/gidsweek_uploaden", type = "file", regexp = "gidsteksten_2022\\d{4}.json$")
