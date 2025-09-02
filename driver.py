import mysql.connector
import math
from DBEDAssign2 import DBEDAssign2

d1 = DBEDAssign2("a1924315")
print("Setting up")
d1.setUp()
print("Reading data")
d1.readData('aust_trim_code.csv')
print("Data read complete")
for i in d1.select_by_pcode("5063"):
    print(i)
locstor = d1.entropyCalc()
print('{:2.3f}'.format(locstor))
print("Test show all:")
for row in d1.show_all():
    print(row)

d1.tearDown()