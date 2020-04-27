def readData(f):
  import csv
  import gzip

  with gzip.open(f, mode='rt') as recrods:
    reader = csv.reader(recrods)


    for row in reader:
      print(row)
      break

if __name__ == "__main__":

  import sys

  file_location = sys.argv[1]
  print(file_location)

  readData(file_location)

tpep_pickup_datetime,tpep_dropoff_datetime,pickup_latitude,pickup_longitude,dropoff_latitude,dropoff_longitude
