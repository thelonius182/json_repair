# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
# README ----
# Om te voorkomen dat hh_fixR wordt gebruikt om systeemherhalingen te wijzigen,
# is een bron nodig die zegt of een post een systeemherhaling is. Het modelrooster
# wijzigt steeds, dus niet geschikt daarvoor. De gidsuploads wel.
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
suppressWarnings(suppressPackageStartupMessages(library(tidyr)))
suppressWarnings(suppressPackageStartupMessages(library(yaml)))
suppressWarnings(suppressPackageStartupMessages(library(magrittr)))
suppressWarnings(suppressPackageStartupMessages(library(stringr)))
suppressWarnings(suppressPackageStartupMessages(library(dplyr)))
suppressWarnings(suppressPackageStartupMessages(library(lubridate)))
suppressWarnings(suppressPackageStartupMessages(library(fs)))
suppressWarnings(suppressPackageStartupMessages(library(readr)))
suppressWarnings(suppressPackageStartupMessages(library(jsonlite)))
suppressWarnings(suppressPackageStartupMessages(library(DBI)))

source("src/shared_functions.R", encoding = "UTF-8")

err_msg <- NULL
db_conn_exists <- F

for (ctrl_grp.1 in 1:1) {
  
  slot_to_fix <- commandArgs(trailingOnly = TRUE) %>% str_flatten(collapse = " ")
  
  # validatie - syntax
  if (str_length(slot_to_fix) == 0) {
    err_msg <<- "niets ingevuld"
    break
  }
  
  # validatie - syntax
  if (!str_detect(slot_to_fix, "^\\d{4}-\\d{2}-\\d{2} \\d{2}$")) {
    err_msg <<- sprintf("%s is ongeldig. Geldig is bv 2022-03-02 01", slot_to_fix)
    break
  }
  
  # validatie - datum
  valid_slot <- try(suppressWarnings(ymd_h(slot_to_fix)))

  if (is.na(valid_slot)) {
    err_msg <<- sprintf("%s is geen geldige datum/tijd", slot_to_fix)
    break
  }
  
  gids_uploads <- dir_ls("C:/cz_salsa/gidsweek_uploaden", 
                         type = "file", 
                         regexp = "gidsteksten_\\d{8}.json$")
  
  # de json's die al bekeken zijn
  hh_fnam_his_qfn <- "C:/Users/nipper/cz_rds_store/branches/hh_fnam_his.RDS"
  
  if (file_exists(hh_fnam_his_qfn)) {
    hh_fnam_his <- read_rds(hh_fnam_his_qfn)
  
    # tbv check nieuwe files 
    hh_fnam_his_rows_sav <- nrow(hh_fnam_his)
  
  } else {
    hh_fnam_his <- NULL
    hh_fnam_his_rows_sav <- 0
  }
  
  # de posts die wel gerepareerd mogen worden
  hh_fixable_posts_qfn <- "C:/Users/nipper/cz_rds_store/branches/hh_fixable_posts.RDS"
  ini_fipo_qfn <- "C:/Users/nipper/cz_rds_store/branches/initial_fixable_post.RDS"
  
  if (file_exists(hh_fixable_posts_qfn)) {
    hh_fixable_posts <- read_rds(hh_fixable_posts_qfn)
  } else {
    # init op 15 velden (incl genre-2)
    hh_fixable_posts <- read_rds(ini_fipo_qfn)
  }
  
  # de posts die niet gerepareerd mogen worden
  hh_unfixable_posts_qfn <- "C:/Users/nipper/cz_rds_store/branches/hh_unfixable_posts.RDS"
  
  if (file_exists(hh_unfixable_posts_qfn)) {
    hh_unfixable_posts <- read_rds(hh_unfixable_posts_qfn)
  } else {
    hh_unfixable_posts <- NULL
  }
  
  # verzameling posts die gerepareerd mogen worden aanvullen
  for (fnam in gids_uploads) {
    
    # print(fnam)
    
    # json al bekeken > skip
    if (fnam %in% hh_fnam_his$qfn) {
      next
    }
    
    # json afvlaggen
    if (is.null(hh_fnam_his)) {
      hh_fnam_his <- tibble(qfn = fnam)
    } else {
      hh_fnam_his %<>% add_row(qfn = fnam)
    }
    
    gw_json <- fromJSON(fnam) 
    
    for (j1 in 1:length(gw_json)) {
      
      cur_post <- gw_json[[j1]] %>% as_tibble() %>% mutate(gids_item_key = names(gw_json[1]),
                                                           gids_upload = path_file(fnam))
      
      # vaste herhalingen 
      if (ncol(cur_post) < 10) {
        
        if (is.null(hh_unfixable_posts)) {
          hh_unfixable_posts <- cur_post
        } else {
          hh_unfixable_posts %<>% add_row(cur_post)
        }
        
        next
      }
      
      # vervangende herhalingen 
      hh_fixable_posts %<>% add_row(cur_post)
    }
  }
  
  # nieuwe files, dan update his
  if (nrow(hh_fnam_his) > hh_fnam_his_rows_sav) {
    write_rds(hh_fnam_his, file = hh_fnam_his_qfn)
    write_rds(hh_fixable_posts, file = hh_fixable_posts_qfn)
    write_rds(hh_unfixable_posts, file = hh_unfixable_posts_qfn)
  }

  # herstel alleen vervangende herhalingen
  start_to_find <- slot_to_fix %>% paste0(":00")
  
  if (start_to_find %in% hh_unfixable_posts$start) {
    src_json <- hh_unfixable_posts %>% filter(start == start_to_find) %>% select(gids_upload)
    err_msg <<- sprintf("%s is gelockt: dit is geen vervangende herhaling (zie %s)", 
                        slot_to_fix, src_json)
    break
  }
  
  if (!start_to_find %in% hh_fixable_posts$start) {
    err_msg <<- sprintf("%s is geen bestaande post", slot_to_fix)
    break
  }
  
  # NL-post-id ophalen, om de bestaande posts te verwijderen
  # . Connect to database 
  wp_conn <- get_wp_conn()
  
  # . connection type S4 indicates a valid connection; other types indicate failure
  if (typeof(wp_conn) != "S4") { 
    err_msg <<- "database is niet beschikbaar"
    break
  }
  
  db_conn_exists <<- T
  
  sql_stmt <- "
     SELECT 
         po1.id AS nl_post_id
     FROM
         wp_posts po1
             JOIN
         wp_term_relationships tr1 ON tr1.object_id = po1.id
     WHERE
                 po1.post_date = '@FIX_DATE'
             AND tr1.term_taxonomy_id = 5
             AND po1.post_type = 'programma'" %>% 
    str_replace("@FIX_DATE", paste0(slot_to_fix, ":00:00"))
  
  sql_result <<- dbGetQuery(wp_conn, sql_stmt)
  
  # verwijder NL- en EN-post
  sql_where <- sprintf("in (%s, %s)", sql_result$nl_post_id, 1 + sql_result$nl_post_id)
  
  sql_stmt_del.1 <- sprintf("delete from wp_postmeta where post_id %s", sql_where)
  dbExecute(wp_conn, sql_stmt_del.1)
  
  sql_stmt_del.2 <- sprintf("delete from wp_posts where id %s", sql_where)
  dbExecute(wp_conn, sql_stmt_del.2)
  
  # maak de repair-json
  hh_fipo_all <- hh_fixable_posts %>% filter(start == start_to_find)
  hh_fipo.1 <- hh_fipo_all %>% select(-starts_with("gids_"))
  hh_fipo.2 <- 
    if (!is.na(hh_fipo.1$`genre-2-nl`)) {  
      hh_fipo.1 
    } else {
      hh_fipo.1 %>% select(-starts_with("genre-2"))
    }
  
  hh_fipo_json.1 <- toJSON(hh_fipo.2)
  
  hh_fipo_json.2 <- hh_fipo_json.1 %>% 
    str_replace("\\]", "}") %>% 
    str_replace("\\[", sprintf('{\\"%s":', hh_fipo_all$gids_item_key))
  
  repair_json_qfn <- sprintf("C:/cz_salsa/gidsweek_uploaden/hh_fix_%s.json", slot_to_fix) %>% 
    str_replace_all("[ -]", "_")
  
  write_file(hh_fipo_json.2, repair_json_qfn)
}

if (db_conn_exists) {
  dbDisconnect(wp_conn)
}

if (!is.null(err_msg)) {
  print(err_msg)
} else {
  sprintf("herstel-json staat klaar: %s", path_file(repair_json_qfn))
}
