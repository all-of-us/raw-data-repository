#!/usr/local/bin Rscript

########################################## install required packages if you haven't before in your current working directory, uncomment the below two lines.
#install.packages("httr", repos = "http://cran.us.r-project.org")
#install.packages("jsonlite", repos = "http://cran.us.r-project.org")

require("httr")
require("jsonlite")

WD <- getwd()
keyname <- "/gcloud_key.json"
keyfile <- paste(WD,keyname,sep = "", collapse = NULL)
service_account <- "configurator@all-of-us-rdr-sandbox.iam.gserviceaccount.com"

############################### uncomment below if you're not logged in to gcloud
#system("gcloud auth login")

############################### uncomment below if you need to create a key for the service account. The key [gcloud_key.json] will be in your current working directory 
#system(sprintf("gcloud iam service-accounts keys create --account michael.mead@pmi-ops.org --project all-of-us-rdr-sandbox --iam-account %s %s", service_account, keyfile))
 
token <- httr::oauth_service_token(
              endpoint=httr::oauth_endpoints("google"),
              secrets=jsonlite::fromJSON(paste(keyfile)),
              scope="https://www.googleapis.com/auth/userinfo.email")
                                 
################################# Change awardee string to your Awardee ID
awardee <- "ILLINOIS"

################### The ops_data_url can be modified to fit your needs. Docs on the api are at https://github.com/all-of-us/raw-data-repository/blob/master/opsdataAPI.md
################### Limited to 2 participant summaries for example purposes.
ops_data_url <- sprintf("https://all-of-us-rdr-sandbox.appspot.com/rdr/v1/ParticipantSummary?awardee=%s&_count=2", awardee)

arg_list <- list(url = paste(ops_data_url), 
                 config = httr::config(token = token), 
                 encode = "json")

callback <- do.call("GET", args = arg_list, envir = asNamespace("httr"))

get_text <- httr::content(callback, as = "text")
json <- fromJSON(get_text, flatten = TRUE)
paste(json)


###############################   TESTING DIFFERENT OUTPUT METHODS #############################
#test <- fromJSON(get_text)
#list_view <- test$entry$resource
#paste(list_view)

