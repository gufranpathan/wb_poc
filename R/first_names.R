library(dplyr)
library(readr)
library(stringr)

df <- read_csv('data/kaliaganj/consolidated.csv')
religion_mapped <- read_csv('data/kaliaganj/analysis/religion_mapped.csv')

religion_mapped$religion <- trimws(religion_mapped$religion)
View(table(religion_mapped$religion))

missing_religion <- religion_mapped %>% 
  filter(religion%in%c("Hindu","Muslim")) %>% 
  select(last_name_ben) %>% 
  pull(.)

name_split <- str_split(df$name," ",simplify = TRUE)
df$first_name_ben <- name_split[,1]
df$middle_name_ben <- NA
df$last_name_ben <- NA
two_names <- name_split[,3]==""
df$middle_name_ben[!two_names] <- name_split[,2][!two_names]
df$last_name_ben[two_names] <- name_split[,2][two_names]
df$last_name_ben[!two_names] <- name_split[,3][!two_names]

sum(df$last_name_ben%in%missing_religion)
missing_df <- df[!df$last_name_ben%in%missing_religion,]
head(missing_religion)



first_names <- missing_df %>% group_by(first_name_ben) %>% 
  summarize(people=n()) %>% 
  arrange(desc(people)) %>% 
  mutate(cumulative_people=cumsum(people)+sum(df$last_name_ben%in%missing_religion)) %>% 
  ungroup() %>% 
  filter(!is.na(first_name_ben))
first_names$cum_per <- first_names$cumulative_people/nrow(df)
View(first_names)

write_excel_csv(first_names,"data/kaliaganj/first_names.csv")
