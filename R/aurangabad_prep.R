social_eng <- read_csv('data/aurangabad/Social_Eng_Clean.csv')
booth <- read.csv('data/aurangabad/booth_mapping.csv',
                       stringsAsFactors = FALSE)
colnames(social_eng)[1] <- c("last_name")
colnames(social_eng)[4] <- c("community")
colnames(social_eng)[5] <- c("party_allegiance")

df$booth_number <- as.numeric(df$part)
df_joined <- df %>% 
  left_join(social_eng %>% select(last_name,Religion,community,party_allegiance)
            ,by=c("last_name_ben"="last_name")) 
df_joined <- df_joined %>% left_join(.,booth,by="booth_number") %>% select(-filename)

df_joined$sex <- NA
df_joined$gender <- trimws(df_joined$gender)
df_joined$sex[which(df_joined$gender=="पुरूष")] <- "Male"
df_joined$sex[which(df_joined$gender=="परूष")] <- "Male"
df_joined$sex[which(df_joined$gender=="महिला")]<-"Female"
df_joined$sex[is.na(df_joined$sex)] <- "Others"
table(df_joined$sex,useNA = "always")

df_joined$age <- as.numeric(df_joined$age)
df_joined$city <- "Aurangabad"
df_joined$state <- "Bihar"
df_joined$date_of_birth <- as.Date(paste(2020 - df_joined$age,"01","01",sep="-"))
df_joined <- df_joined[,colnames(df_joined)[2:ncol(df_joined)]]
write_excel_csv(df_joined,'data/aurangabad/aurangabad_clean.csv',na="")
View(table(df_joined$booth_name))

write_excel_csv(df_joined %>% filter(booth_category=="Battle Ground"),'data/aurangabad/aurangabad_battleground.csv',na="")

write_excel_csv(df_joined %>% filter(booth_category=="Battle Ground"),'data/aurangabad/aurangabad_battleground.csv',na="")

write_excel_csv(df_joined %>% filter(booth_number%in%c(10,11)),'data/aurangabad/sample_battleground.csv',na="")
