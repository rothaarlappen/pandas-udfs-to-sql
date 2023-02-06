import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from database import create_connection
import pandas as pd
from pandas import Timestamp

conn = create_connection()


def maxPrimeFactors (n):
    import math 
    n = n % 15013
    maxPrime = -1
     
    while n % 2 == 0:
        maxPrime = 2
        n >>= 1     
    for i in range(3, int(math.sqrt(n)) + 1, 2):
        while n % i == 0:
            maxPrime = i
            n = n / i
    if n > 2:
        maxPrime = n
     
    return int(maxPrime)


df = pd.read_sql("SELECT o_orderkey FROM orders", conn)

df["max_prime"] = df.apply(lambda row: maxPrimeFactors(row["o_orderkey"]), axis=1)

df.to_sql("table_new", conn)


