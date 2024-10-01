import os
import pandas as pd
import numpy as np
from enum import Enum

from typing import Iterator
# from foo import Foo

# enum test
class Fruit(Enum):
  Apple = 4
  Orange = 5
  Pear = 6
apple = Fruit.Apple

class Foo:
  def __init__(self):
    self.name = "foo"
    self.dict = {}
    self.copy = self.dict

  def iterator(self) -> Iterator[int]:
    for i in range(5):
      yield i

base = os.path.dirname(os.path.realpath(__file__))

foo = Foo()

def load():
  global data
  data = pd.read_csv(base + "/data1.csv")
  # Change the attribute of the global variable.
  foo.next = "goo"

def sum(df):
  return np.sum(df.loc[:,"x"])

for i in range (0, 10):
  a = i

load()

# Alias test
food = foo.dict
goo = foo
hoo = Foo()

class Point:
  def __init__(self):
    self.nearest_point = None

p1 = Point()
p2 = Point()
p1.nearest_point = p2   # cycling
p2.nearest_point = p1

# # Generator test
# # goe = (num**2 for num in range(5))
# # gof = foo.iterator()
# # print(gof)