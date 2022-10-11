get_wp_conn <- function() {
  
  db_env <- "wpprd_mariadb"
  
  ### TEST
  # db_env <- "wpdev_mariadb"
  ### TEST
  
  result <- tryCatch( {
    grh_conn <- dbConnect(odbc::odbc(), db_env, timeout = 10)
  },
  error = function(cond) {
    return("connection-error")
  }
  )
  return(result)
}