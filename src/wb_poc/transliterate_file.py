import pandas as pd
from indic_transliteration import sanscript
from indic_transliteration.sanscript import SchemeMap, SCHEMES, transliterate

df = pd.read_csv('data/aurangabad/aurangabad_clean.csv')

prabhag = pd.read_csv('data/aurangabad/booth_prabhag_ward.csv')


df['name_english'] = df.name.apply(lambda x: transliterate(x,sanscript.DEVANAGARI,sanscript.ITRANS).title() \
    if not isinstance(x,float) else "")

df['rel_name_english'] = df.rel_name.apply(lambda x: "("+transliterate(x,sanscript.DEVANAGARI,sanscript.ITRANS).title()+")" \
    if not isinstance(x,float) else "")

# df['gender_english'] = df.gender.apply(lambda x: transliterate(x,sanscript.BENGALI,sanscript.ITRANS).title() \
#     if not isinstance(x,float) else "")

df['surname'] = df.rel_name_brackets +"-"+ df.name_english
df.columns
df = df.join(prabhag.set_index('booth_no'),on='booth_number')
df.to_csv('data/aurangabad/aurangabad_prabhag.csv',encoding='utf-8-sig',index=False)

male = "পুং"
female = "স্ত্রী"


