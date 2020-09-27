import pandas as pd
from indic_transliteration import sanscript
from indic_transliteration.sanscript import SchemeMap, SCHEMES, transliterate

df = pd.read_csv('data/kaliaganj/analysis/consolidated_df.csv')
df.columns

data = df.name.iloc[0]




df['name_english'] = df.name.apply(lambda x: transliterate(x,sanscript.BENGALI,sanscript.ITRANS).title() \
    if not isinstance(x,float) else "")

df['rel_name_english'] = df.rel_name.apply(lambda x: transliterate(x,sanscript.BENGALI,sanscript.ITRANS).title() \
    if not isinstance(x,float) else "")

df['gender_english'] = df.gender.apply(lambda x: transliterate(x,sanscript.BENGALI,sanscript.ITRANS).title() \
    if not isinstance(x,float) else "")



df.to_csv('data/kaliaganj/analysis/consolidated_df_transliterated.csv',encoding='utf-8-sig',index=False)

male = "পুং"
female = "স্ত্রী"


