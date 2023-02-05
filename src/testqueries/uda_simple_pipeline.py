import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from database import create_connection
import pandas as pd


conn = create_connection()

def my_func(prices : pd.Series) -> float:
    from statistics import mean
    if len(prices) == 0:
        return 0
    prices.sort_values(inplace=True)
    if(len(prices) < 3):
        return prices.mean()
    else: 
        return mean([prices.iloc[0], prices.iloc[1], prices.iloc[0], prices.iloc[-1], prices.iloc[-2], prices.iloc[-3]])

df = pd.read_sql("SELECT * FROM orders", conn)
df3 = df.groupby("o_custkey")["o_totalprice"].apply(my_func)

df3.head()