library(readr)
library(dplyr)
library(stringr)
library(stringdist)

new <- read_csv("data/kusheshwar/kusheshwar_booth_mapping.csv")
old <- read_csv("mar_2015_boothlist.csv")


to_remove_words <- c("west","part","north","south","room"," no ",
                     "kishanganj","middle","east","office",
                     "building","hat","new","madarsa","taluka",
                     "dakshin","bhawan"," nagar",
                     "purab","purana","bhag","uttar","madhya",
                     "pashchim","uttari","naya","dakshini","parth",
                     "purvi","paschim")


get_clean_name <- function(ps_name){
  #ps_name <- gsub("\\."," ", ps_name)
  #ps_name <- gsub("\\,"," ", ps_name)
  ps_name <- gsub("[^a-zA-Z ]","", ps_name)
  
  for(i in 1:length(to_remove_words)){
    ps_name <- str_remove_all(ps_name,
                                     paste0("\\b",to_remove_words[i],"\\b"))
  }
  ps_name <-  word(trimws(ps_name),-1)
  ps_name
}

new$booth_number <- as.numeric(sapply(str_split(new$ps_number_name," - "),"[[",1))
new$booth_name <- sapply(str_split(new$ps_number_name," - "),"[[",2)

new$locality <- get_clean_name(tolower(new$booth_name))
old$locality <- get_clean_name(tolower(old$Name))
View(table(new$locality))
write_csv(new,"data/kusheshwar/kusheshwar_booth_2019.csv")
write_csv(old,"old.csv")


new_unique <- unique(new$locality)
old_unique <- unique(old$locality)

length(new_unique)
length(old_unique)

dist_mat <- stringdistmatrix(new_unique,old_unique)
dim(dist_mat)

new_self_mat <- stringdistmatrix(new_unique,new_unique)
for (i in 1:nrow(new_self_mat)){
  new_self_mat[i,i] <- NA
}


new_dedup <- data.frame(new=new_unique,
                        new_dedup=new_unique[apply(new_self_mat, 1, which.min)])

new_dedup$dist <- stringdist(new_dedup$new,new_dedup$new_dedup)
new_dedup$soundex <- stringdist(new_dedup$new,new_dedup$new_dedup,
                                method="soundex")

View(new_dedup)
table(new_dedup$dist)
table(new_dedup$soundex)

new_matches <- data.frame(new=new_unique,
                          old_match=old_unique[apply(dist_mat,1,which.min)],
                          stringsAsFactors = FALSE)
View(new_matches)
