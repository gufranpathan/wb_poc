library(dplyr)
library(readr)
library(stringr)
library(tidyr)

df <- read_csv('data/kaliaganj/consolidated.csv')
religion_mapped <- read_csv('data/kaliaganj/analysis/religion_mapped.csv')

religion_mapped$religion <- trimws(religion_mapped$religion)
religion_mapped <- religion_mapped %>% 
  filter(religion%in%c("Hindu","Muslim"))


name_split <- str_split(df$name," ",simplify = TRUE)
df$first_name_ben <- name_split[,1]
df$middle_name_ben <- NA
df$last_name_ben <- NA
two_names <- name_split[,3]==""
df$middle_name_ben[!two_names] <- name_split[,2][!two_names]
df$last_name_ben[two_names] <- name_split[,2][two_names]
df$last_name_ben[!two_names] <- name_split[,3][!two_names]

colnames(religion_mapped)
print(nrow(df))
df <- df %>% left_join(religion_mapped %>% select(last_name_ben,religion),
                       by="last_name_ben")
print(nrow(df))

colnames(df)
religion_booth <- df %>%
  mutate(religion=if_else(is.na(religion),"Unknown_Religion",religion)) %>% 
  group_by(part,religion) %>%
  summarize(voters=n()) %>% 
  spread(religion,voters,fill=0) %>% 
  rename(booth_number=part)



df$age_numeric <- as.numeric(df$age)
df$age_bucket <- NA

df$age_bucket[which(df$age_numeric>=18,df$age_numeric<=25)] <- "18 - 25"
df$age_bucket[which(df$age_numeric>=26,df$age_numeric<=40)] <- "26 - 40"
df$age_bucket[which(df$age_numeric>=41,df$age_numeric<=60)] <- "41 - 60"
df$age_bucket[which(df$age_numeric>=61)] <- "61+"

age_booth <- df %>%
  mutate(age_bucket=if_else(is.na(age_bucket),"Unknown_Age",age_bucket)) %>% 
  group_by(part,age_bucket) %>%
  summarize(voters=n()) %>% 
  spread(age_bucket,voters,fill=0) %>% 
  rename(booth_number=part)
View(age_booth)


booth_mapping <- read.csv("data/kaliaganj/analysis/booth_mapping.csv",
                          stringsAsFactors = FALSE)
all_booth <- booth_mapping %>% select(booth_number,booth_name) %>% 
  full_join(religion_booth,by="booth_number") %>% 
  full_join(age_booth,by="booth_number")

write.csv(all_booth,"data/kaliaganj/analysis/kaliaganj_demographics.csv",
          row.names = FALSE,na="")
