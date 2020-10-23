
library(dplyr)
library(readr)
library(stringr)
voter_files <- list.files("data/rr_nagar/parsed/assembly=154/",
                          full.names = TRUE,recursive = TRUE)

# file_matches <- str_match(voter_files,"data/kaliaganj/parsed/ACNo_34_PartNo_(\\d+)_Page(\\d+)_(\\d+).csv")

voter_file_df <- data.frame(
  filename=voter_files,
  # part_no=as.numeric(file_matches[,2]),
  # page_no=as.numeric(file_matches[,3]),
  # dpi=as.numeric(file_matches[,4]),
  stringsAsFactors = FALSE
)

df_list <- list()
for(i in 1:nrow(voter_file_df)){
  print(i)
  tmp <- read_csv(voter_file_df$filename[i])
  tmp$voter_id <- as.character(tmp$voter_id) 
  
  tmp$house_number <- as.character(tmp$house_number) 
  tmp$age <- as.character(tmp$age) 
  
  # tmp$part_no <- voter_file_df$part_no[i] 
  # tmp$page_no <- voter_file_df$page_no[i] 
  # tmp$dpi <- voter_file_df$dpi[i] 
  df_list[[i]] <- tmp
  
}

df <- bind_rows(df_list)
write_excel_csv(df,'data/rr_nagar/rr_nagar_31booths.csv',na="")

booth_summary <- df %>% group_by(part) %>% 
  summarize(voters=n())
d200 <- df %>% filter(dpi==200)
d500 <- df %>% filter(dpi==500)

dcoal <- d200 %>% 
  left_join(d500 %>% filter(!is.na(voter_id)) %>% 
              group_by(voter_id) %>% 
              mutate(v=n()) %>% 
              filter(v==1),by="voter_id",suffix=c("_200","_500"))
nrow(dcoal)/nrow(d200)
colnames(dcoal)
dcoal$name <- coalesce(dcoal$name_200,dcoal$name_500)
dcoal$rel_name <- coalesce(dcoal$rel_name_200,dcoal$rel_name_500)
dcoal$house <- coalesce(dcoal$house_200,dcoal$house_500)
dcoal$age <- coalesce(dcoal$age_200,dcoal$age_500)
dcoal$gender <- coalesce(dcoal$gender_200,dcoal$gender_500)

dcoal <- dcoal %>% 
  select(voter_id,name,rel_name,age,gender,house,X1_200,
         part_no_200,page_no_200)

sum(complete.cases(d200))/nrow(d200)
sum(complete.cases(dcoal))/nrow(dcoal)

write_excel_csv(dcoal,'data/kaliaganj/analysis/consolidated_df.csv',
                na="")
