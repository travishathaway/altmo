from osgeo import gdal
from osgeo import ogr

pts = ogr.Open("points.shp", 0)
layer = pts.GetLayer()

for field in layer.schema:
    print(field.name)

dem = gdal.Open("dem.tif")
gt = dem.GetGeoTransform()

ulx = gt[0]
uly = gt[3]
res = gt[1]

xsize = dem.RasterXSize
ysize = dem.RasterYSize

lrx = ulx + xsize * res
lry = uly - ysize * res

dem = None
pts = layer = None

# nearest neighbor interpolation
nn = gdal.Grid("nearest.tif", "points.shp", zfield="elevation",
               algorithm = "nearest", outputBounds = [ulx,uly,lrx,lry],
               width = xsize, height = ysize)
nn = None

# moving average
ma = gdal.Grid("average.tif", "points.shp", zfield="elevation",
               algorithm = "average:radius1=500:radius2=800:angle=20",
               outputBounds = [ulx,uly,lrx,lry],
               width = xsize, height = ysize)
ma = None

# inverse distance to a power
idw = gdal.Grid("invdist.tif", "points.shp", zfield="elevation",
                algorithm="invdist:power=3:radius1=2000:radius2=2000",
                outputBounds=[ulx, uly, lrx, lry],
                width=xsize, height=ysize)
idw = None

# linear interpolation
lin = gdal.Grid("linear.tif", "points.shp", zfield = "elevation",
                algorithm = "linear",
                outputBounds = [ulx,uly,lrx,lry],
                width = xsize, height = ysize)
lin = None