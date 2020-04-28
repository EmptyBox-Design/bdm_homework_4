def readData(f):
  import csv
  # import gzip

  # with gzip.open(f, mode='rt') as recrods:
  #   reader = csv.reader(recrods)
  with open('eggs.csv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=' ', quotechar='|')

    i = 0
    for row in reader:
      print(row)
      i += 1
      if(i == 2):
        break

if __name__ == "__main__":

  import sys

  file_location = sys.argv[1]

  readData(file_location)