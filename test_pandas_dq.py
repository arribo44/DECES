# 
import pandas as pd
from ydata_profiling import ProfileReport

df = pd.read_csv('../DATAS/1970/OUT/PERSONNES/CONCAT_PERSONNES_1970.csv', sep=';', header=0, low_memory=False)
profile = ProfileReport(df)
profile.to_file("test_pandas_dq.html")