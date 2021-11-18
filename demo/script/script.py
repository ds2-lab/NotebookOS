import os
import pandas as pd
import numpy as np

base = os.path.dirname(os.path.realpath(__file__))

def load():
  global data
  data = pd.read_csv(base + "/data1.csv")

def sum(df):
  return np.sum(df.loc[:,"x"])

for i in range (0, 10):
  a = i

load()