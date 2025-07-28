import pandas as pd
from french_cities import find_city

data = {
    "city": ["Lille", "Ajaccio","L ISLE ADAM"],
    "postcode": ["59000", "20000","78313"]  # codes postaux réels ou plausibles
}
df = pd.DataFrame(data)

df_corrige = find_city(df)
#print(df_corrige[["city", "postcode", "target_code_insee", "target_city"]])

print(df_corrige.columns)
print(df_corrige.head())