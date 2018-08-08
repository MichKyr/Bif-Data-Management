# Databricks notebook source
import math

osmRDD=sc.textFile('/FileStore/tables/osm_pois_greece_v1_1_2-fce9c.csv').map(lambda x: x.encode("ascii", "replace")).map(lambda x: x.split(";"))

wordsRDD = sc.textFile("/FileStore/tables/GR.txt").map(lambda x: x.split('\t'))

osmRDD1=osmRDD.filter(lambda x: len(x) == 8)
wordsRDD1= wordsRDD.filter(lambda x: len(x) == 19 )


print osmRDD.take(2)
print wordsRDD1.take(1)
print wordsRDD1.count()


#filtering header
osmRDD2=osmRDD1.filter(lambda x: not ("osm_id" in x[0])  )
print osmRDD2.take(1)
#print wordsRDD2.take(1)



# filter all the files with name:en
osmRDD3=osmRDD2.filter(lambda x: "name:en" in x[6])
print osmRDD3.take(5)

osmRDD3.count()
print osmRDD3.count()
print osmRDD3.take(5)


osmRDD4=osmRDD3.filter(lambda x: float(x[3])>23.41073 and float(x[3])<24.005365 and float(x[4])>37.781702 and float(x[4])<38.265273 )
wordsRDD2=wordsRDD1.filter(lambda x: float(x[4])>37.781702 and float(x[4])<38.265273 and float(x[5])>23.41073 and float(x[5])<24.005365)
print osmRDD4.take(1)
print wordsRDD2.take(1)


print osmRDD4.count()
print wordsRDD2.count()



def extractnames(x):
    firstsplit = x[6].split(",")
    qwe="zcvz"
    for i in firstsplit:
      asdfa=i.find('name:en')
      if asdfa !=-1:
        qwe=i
        qwe1=qwe.replace('""name:en""=>""' , '')
        qwe2=qwe1.replace('""', '')
        qwe3=qwe2.lstrip()
        x.append(qwe3);
        break
      
    return x
        
osmRDD5=osmRDD4.map(lambda x: extractnames(x))
print osmRDD5.take(1)



#Levenshtein function

import math
import numpy as np

class Levenshtein:
  def _computelevenshtein(self,s1,s2):
    slen = len(s1)
    tlen = len(s2)

   
    matrix = np.zeros((slen+1,tlen+1))

    #initalize
    for i in range(slen+1):
      matrix[i][0] = i
    for j in range(tlen+1):
      matrix[0][j] = j
    #print(matrix)

    for i in range(1,slen+1):
      for j in range(1,tlen+1):
        matrix[i][j] = min(matrix[i-1][j]+1,matrix[i][j-1]+1, matrix[i-1][j-1]+ (0 if s1[i-1] == s2[j-1] else 1))

    #print("levendistance",matrix[slen][tlen])
    #print(matrix)
    return matrix[slen][tlen]

  def score(self,s1,s2):
    return 1 - float(self._computelevenshtein(s1,s2))/max(len(s1),len(s2))

def distance(origin, destination):
  lat1, lon1 = origin
  lat2, lon2 = destination
  radius = 6371 # Km
  dlat = math.radians(lat2-lat1)
  dlon = math.radians(lon2-lon1)
  a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
    * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
  c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
  d = radius * c
  # return value is in Km , so 0.5 is 500m
  return d



print osmRDD5.take(1)
print wordsRDD1.take(1)

#cartesian RDD creation
cartRDD=osmRDD5.cartesian(wordsRDD2)
print cartRDD.take(1)
print cartRDD.count()


#task 1
#comparing distances

cartRDD1=cartRDD.filter(lambda x: distance([float(x[0][4]),float(x[0][3])],[float(x[1][4]), float(x[1][5])] ) <0.1)
print cartRDD1.take(1)
#print cartRDD1.count()


print cartRDD1.count()


#from pyspark.sql.functions import levenshtein 
l= Levenshtein()
print(l.score('industry','asciiname'))
print(l.score('cafe','le cafet'))
cartRDD2=cartRDD1.filter(lambda x: l.score(x[0][8], x[1][2]) >0.5)
print cartRDD2.take(1)
print cartRDD2.count()


#task 2 
# function for alternames
 
def namecompare(name_en,alternatenames):
  lev=Levenshtein()
  for alternatename in alternatenames:
    if lev.score(name_en, alternatename)>0.5:
      return True
  return False
        
#cartRDD3=cartRDD1.map(lambda x: namecompare(alternatename,name_en))
#print cartRDD3.take(1)
#print cartRDD3.count()


#task 2
l=Levenshtein()
cartRDD3=cartRDD1.filter(lambda x: (l.score(x[0][8], x[1][2]) >0.5) or  (namecompare(x[0][8],x[1][3].split()) >0.5))

print cartRDD3.take(1)
print cartRDD3.count()


#task 3
#type==RESTAURANT
cartRDD4=cartRDD.filter(lambda x: x[0][2] =='RESTAURANT')
print cartRDD4.take(2)
print cartRDD4.count()


#distance lower to 500 meters
cartRDD5=cartRDD4.filter(lambda x: distance([float(x[0][4]),float(x[0][3])],[float(x[1][4]), float(x[1][5])] ) <0.5)
print cartRDD5.take(1)
print cartRDD5.count()


#reducebykey
cartRDD6=cartRDD5.map(lambda x: (x[1][0], 1)).reduceByKey(lambda x,y: x+y)
print cartRDD6.take(1)
print cartRDD6.takeOrdered(15, key=lambda x: -x[1]) 
print cartRDD6.count()
