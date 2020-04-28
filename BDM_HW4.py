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
    counts = {}
    
    neighborhoodNames = neighborhoods['neighborhood']
    boroNames = neighborhoods['borough']

    for row in reader:

        if(len(row) > 1):
            pickup = geom.Point(proj(float(row[5]), float(row[6])))

            bh = findZone(pickup, index, neighborhoods)
            nbd = None
            
            # checks the destination column for errors
            if(row[9] is not None and row[9] != "0.0" and row[9] != 'NULL' and type(float(row[5])) == float):
                dropoff = geom.Point(proj(float(row[9]), float(row[10])))
                # Look up a matching zone, and update the count accordly if
                # such a match is found
                nbd = findZone(dropoff, index, neighborhoods)
                # look up matching borough for destination

            # checks the neighborhood or borough returns for data
            if nbd is not None and bh is not None:
                key = str(boroNames[bh])+"-"+str(neighborhoodNames[nbd])
                counts[key] = counts.get(key, 0) +1
            
    return counts.items()

def toCSVLine(data):
    # string = []
    
    # for d in data:
    #     if(type(d) is list):
    #         string.append(','.join(str(e) for e in d))
    #     else:
    #         string.append(d)
    # return ','.join(str(e) for e in string )
    return ",".join(str(d) for d in data)

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
        .map(toCSVLine) \
        .saveAsTextFile(output_location)
    
    # .mapValues(lambda x: unpackTupes(x)) \
    # for key in counts:
    #     print(key)
        # .saveAsTextFile(output_location)

    print('task complete')