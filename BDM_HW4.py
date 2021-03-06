def createIndex(geojson):
    '''
    This function takes in a shapefile path, and return:
    (1) index: an R-Tree based on the geometry data in the file
    (2) zones: the original data of the shapefile
    
    Note that the ID used in the R-tree 'index' is the same as
    the order of the object in zones.
    '''
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(geojson).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    '''
    findZone returned the ID of the shape (stored in 'zones' with
    'index') that contains the given point 'p'. If there's no match,
    None will be returned.
    '''
    match = index.intersection((p.x, p.y, p.x, p.y))

    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None

def processTrips(pid, records):
    '''
    Our aggregation function that iterates through records in each
    partition, checking whether we could find a zone that contain
    the pickup location.
    '''
    import csv
    import pyproj
    import shapely.geometry as geom

    # Create an R-tree index
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    # neighborhood index
    index, neighborhoods = createIndex("neighborhoods.geojson")

    # Skip the header
    if pid==0:
        next(records)
    reader = csv.reader(records)
    # create dict
    counts = {}
    
    neighborhoodNames = neighborhoods['neighborhood']
    boroNames = neighborhoods['borough']

    for row in reader:
        # checks for empty rows
        if (len(row) > 0):
            # pickup location
            pickup = geom.Point(proj(float(row[5]), float(row[6])))
            # query the neighborhood index for pickups
            boro_index = findZone(pickup, index, neighborhoods)
            # init neighborhood variable
            nbd = None
            # data checks
            if(row[10] is not None and row[10] != str and row[9] != str):
                try: 
                    dropoff = geom.Point(proj(float(row[9]), float(row[10])))
                    nbd = findZone(dropoff, index, neighborhoods)
                except ValueError:
                    continue
            # returns the borough-neighborhood as a key to the dict with a counter of 1 
            # only returns if neighborhood and boro are not None. We are not returning None values
            if nbd is not None and boro_index is not None:
                key = str(boroNames[boro_index])+"-"+str(neighborhoodNames[nbd])
                counts[key] = counts.get(key, 0) +1

    return counts.items()

# writes data csv 
# unpacks value tuples
def toCSVLine(data):
    string = []
    
    for d in data:
        if(type(d) is list):
            string.append(','.join(str(e) for e in d))
        else:
            string.append(d)
    return ','.join(str(e) for e in string )

# input value as a nested tuple
# returns list of flattened tuples
def unpackTupes(data):
    j = []
    
    def foo(a, b=None):
        j.append(a)
        j.append(b)

    for i in data:
        foo(*i)

    return j

if __name__ == "__main__":

    from pyspark import SparkContext
    import sys

    file_location = sys.argv[1]
    output_location = sys.argv[2]

    sc = SparkContext()
    rdd = sc.textFile(file_location)
    counts = rdd.mapPartitionsWithIndex(processTrips) \
        .reduceByKey(lambda x,y: x+y) \
        .map(lambda x: (x[0].split("-")[0], (x[0].split("-")[1], x[1]))) \
        .groupByKey() \
        .map(lambda x: (x[0], sorted(x[1], key=lambda z: z[1], reverse=True)[:3])) \
        .sortByKey() \
        .mapValues(lambda x: unpackTupes(x)) \
        .map(toCSVLine) \
        .saveAsTextFile(output_location)