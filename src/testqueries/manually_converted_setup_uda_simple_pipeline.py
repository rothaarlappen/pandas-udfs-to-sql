import sys

from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from database import create_connection

import pandas as pd

from pandas import Timestamp

conn = create_connection()

conn.execute(
    """
CREATE OR REPLACE FUNCTION uda_price (prices float Array)
RETURNS float
AS $$
from statistics import mean
if len(prices) == 0:
	return 0
prices.sort()
if(len(prices) < 3):
	return mean(prices)
else:
	return mean(prices[:3]+ prices[-3:])
$$ LANGUAGE plpython3u
PARALLEL SAFE;
"""
)
