library(stringr)
library(dplyr)
library(readr)

city_booths <- c(
  137,	138,	139,	140,	141,	144,	145,	146,	147,	148,	153,	154,	155,	
  156,	157,	158,	159,	160,	161,	162,	163,	164,	165,	166,	167,	169,	
  170,	171,	172,	173,	174,	175,	176,	177,	178,	179,	180,	182,	183,	
  184,	185,	186,	187,	188,	191,	192,	193,	194,	195,	198,	199,	200,	
  201,	202,	203,	206,	207,	208,	209,	210,	211,	212,	213,	214,	215,	
  216,	217,	218
)


df <- read_csv('data/aurangabad/aurangabad_clean.csv')
df <- df[df$booth_number%in%city_booths,]
df$rel_name_brackets <- paste0("(",df$rel_name,")")
df$house_number <- as.numeric(df$house_number)
write_excel_csv(df,'data/aurangabad/aurangabad_city_booths.csv',na="")
write_excel_csv(df,'data/aurangabad/aurangabad_clean.csv',na="")
