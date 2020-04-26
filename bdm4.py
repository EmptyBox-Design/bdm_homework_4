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
    index, neighborhoods = createIndex('../Data/neighborhoods.geojson')
    # borough index
    bindex,boroughs = createIndex("../Data/boroughs.geojson")
    
    # Skip the header
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}
    
    boroNames = boroughs['boro_name']
    neighborhoodNames = neighborhoods['neighborhood']

    for row in reader:
        pickup = geom.Point(proj(float(row[3]), float(row[2])))
        
        bh = findZone(pickup, bindex, boroughs)
        nbd = None
        
        # checks the destination column for errors
        if(row[5] is not None and row[5] != "0.0" and row[5] != 'NULL'):
            dropoff = geom.Point(proj(float(row[5]), float(row[4])))
            # Look up a matching zone, and update the count accordly if
            # such a match is found
            nbd = findZone(dropoff, index, neighborhoods)
            # look up matching borough for destination

        # checks the neighborhood or borough returns for data
        if nbd is not None and bh is not None:
            key = str(boroNames[bh])+"-"+str(neighborhoodNames[nbd])
            counts[key] = counts.get(key, 0) +1
            
    return counts.items()

from pyspark import SparkContext
import sys
fn = '../Data/yellow.csv.gz'
sc = SparkContext()
rdd = sc.textFile(fn)
counts = rdd.mapPartitionsWithIndex(processTrips) \
    .reduceByKey(lambda x,y: x+y) \
    .map(lambda x: (x[0].split("-")[0], (x[0].split("-")[1], x[1]))) \
    .groupByKey() \
    .map(lambda x: (x[0], sorted(x[1], key=lambda z: z[1], reverse=True)[:3])) \

for key in counts.collect():
    print(key)