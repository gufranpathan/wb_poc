library(stringr)
library(dplyr)
library(readr)
df <- read.csv("data/kaliaganj/analysis/consolidated_df_transliterated.csv",stringsAsFactors = FALSE,
               encoding = 'UTF-8',na.strings = "")
df <- df[complete.cases(df),]
# Split name
name_split <- str_split(df$name," ",simplify = TRUE)
df$first_name_ben <- name_split[,1]
df$middle_name_ben <- NA
df$last_name_ben <- NA
two_names <- name_split[,3]==""
df$middle_name_ben[!two_names] <- name_split[,2][!two_names]
df$last_name_ben[two_names] <- name_split[,2][two_names]
df$last_name_ben[!two_names] <- name_split[,3][!two_names]


name_split <- str_split(df$name_english," ",simplify = TRUE)
df$first_name <- name_split[,1]
df$middle_name <- NA
df$last_name <- NA
two_names <- name_split[,3]==""
df$middle_name[!two_names] <- name_split[,2][!two_names]
df$last_name[two_names] <- name_split[,2][two_names]
df$last_name[!two_names] <- name_split[,3][!two_names]



# Split rel_name
rel_name_split <- str_split(df$rel_name_english," ",simplify = TRUE)
df$first_name_rel <- rel_name_split[,1]
df$middle_name_rel <- NA
df$last_name_rel <- NA
two_names_rel <- rel_name_split[,3]==""
df$middle_name_rel[!two_names_rel] <- rel_name_split[,2][!two_names_rel]
df$last_name_rel[two_names_rel] <- rel_name_split[,2][two_names_rel]
df$last_name_rel[!two_names_rel] <- rel_name_split[,3][!two_names_rel]
View(df)


# name analysis
last_names <- df %>% group_by(last_name) %>% 
  summarize(people=n()) %>% 
  arrange(desc(people)) %>% 
  mutate(cumulative_people=cumsum(people))
last_names$cum_per <- last_names$cumulative_people/sum(last_names$people)
View(last_names)
sum(last_names$people)
# write.csv(last_names,"data/kaliaganj/analysis/last_names.csv",row.names = FALSE)
# write_excel_csv(last_names,"last_names.csv")

#Map religion back

religion <- read_csv("data/kaliaganj/analysis/last_names_religion_mapped.csv")


df <- df %>% left_join(
  religion %>% group_by(last_name) %>% summarize(Religion=max(Religion)),
  by=c("last_name")
)
booth <- read.csv("data/kaliaganj//analysis/booth_mapping.csv",
                  stringsAsFactors = FALSE)

male = "পুং"
female = "স্ত্রী"
df <- df %>% left_join(booth,by=c("part_no_200"="booth_number"))

df$sex <- if_else(df$gender==male,"male",if_else(df$gender==female,"female",""))

df$city <- "Kaliaganj"
df$state <- "WestBengal"
df$gender <- df$sex
df$date_of_birth <- as.Date(paste(as.character(2020-as.numeric(df$age)),"01","01",sep="-"))
write_excel_csv(df,'data/kaliaganj/analysis/kaliaganj_ecanv.csv',na = "")


write_excel_csv(df[1:5000,],'data/kaliaganj/analysis/kaliaganj_ecanv_v1.csv',na = "")
write_excel_csv(df[1:500,],'data/kaliaganj/analysis/kaliaganj_ecanv_v0.csv',na = "")
write_excel_csv(df[501:15000,],'data/kaliaganj/analysis/kaliaganj_ecanv_v02.csv',na = "")

write_excel_csv(df[5000:10000,],'data/kaliaganj/analysis/kaliaganj_ecanv_v2.csv',na = "")
write_excel_csv(df[10000:15000,],'data/kaliaganj/analysis/kaliaganj_ecanv_v3.csv',na = "")
write_excel_csv(df[1:5000,],'data/kaliaganj/analysis/kaliaganj_ecanv_v1.csv',na = "")
