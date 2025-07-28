import pandas as pd
from fuzzyjoin import fuzzy_left_join

df1 = pd.DataFrame({"nom": ["DU,MOSE,HOUEL", "DU MOSE HOUEL"]})
df2 = pd.DataFrame({"nom": ["DU MOSE HOUEL DU", "DU,MOSE,HOUEL"]})

result = fuzzy_left_join(df1, df2, by="nom", method="levenshtein", threshold=3)
print(result)

