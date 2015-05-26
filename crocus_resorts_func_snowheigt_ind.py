import gdal
import numpy
import psycopg2
from subprocess import call
import os
import conn_param

def snowheight_sta(src_sta_dir, src_loc, snow_dst_dir, ind, day):

	gdalwarp = "C:\Python276\Lib\site-packages\osgeo\gdalwarp.exe"
	
	myconn = psycopg2.connect("host="+conn_param.host+" dbname="+conn_param.dbname+" user="+conn_param.user+" password="+conn_param.password)
	
	src_sta = src_sta_dir + ind + "_loc.tif"
	if os.path.isfile(src_sta) == False:
		src_rast = gdal.Open(src_loc)
		src_trans = src_rast.GetGeoTransform()
		cur = myconn.cursor()
		query = """
		with a as (
		select ind, st_envelope(st_buffer(the_geom,25)) geom
		from stations.geo_dsa
		where ind = %s
		order by 1
		)
		select st_xmin(geom), st_ymin(geom), st_xmax(geom), st_ymax(geom), ind from a
		"""
		
		cur.execute(query,(ind,))
		sta = cur.fetchone()
		print sta
		#compute raster extent
		x0 = src_trans[0] + (round((sta[0] - src_trans[0])/src_trans[1])*src_trans[1])
		x1 = (src_trans[0] + src_rast.RasterXSize*src_trans[1])-(round(((src_trans[0] + src_rast.RasterXSize*src_trans[1])-sta[2])/src_trans[1])*src_trans[1])
		y1 = src_trans[3] - (round((src_trans[3] - sta[3])/src_trans[5])*src_trans[5])
		y0 = (src_trans[3] - src_rast.RasterYSize*src_trans[5]) + (round((sta[1] - (src_trans[3] - src_rast.RasterYSize*src_trans[5]))/src_trans[5])*src_trans[5])
		te_str = str(x0) + " " + str(y0) + " " + str(x1) + " " + str(y1)
		
		
		#vector data from postgis
		connString = "PG: host = "+conn_param.host+" dbname = "+conn_param.dbname+" user="+conn_param.user+" password="+conn_param.password
		sql = "select ind id, the_geom from stations.geo_dsa where ind = '" + sta[4] + "'"
		
		call(gdalwarp + " -co \"COMPRESS=LZW\" -co \"TILED=YES\" -cutline \"" + connString + "\" -csql \"" + sql + "\" -te "+ te_str +" -dstnodata -9999 " + src_loc + " " + src_sta, shell=True)
		src_rast = None
		
	loc_rast = gdal.Open(src_sta)
	loc_band = loc_rast.GetRasterBand(1)

	#Get metadata
	xsize = loc_band.XSize
	ysize = loc_band.YSize
	block_sizes = loc_band.GetBlockSize()
	x_block_size = block_sizes[0]
	y_block_size = block_sizes[1]
	max_value = loc_band.GetMaximum()
	min_value = loc_band.GetMinimum()
	if max_value == None or min_value == None:
		stats = loc_band.GetStatistics(0, 1)
		max_value = stats[1]
		min_value = stats[0]
	trans = loc_rast.GetGeoTransform()
	proj = loc_rast.GetProjection()

	format = "GTiff"
	driver = gdal.GetDriverByName(format)
	snow_dst_file = snow_dst_dir + ind + "_" + day + ".tif"
	if os.path.isdir(snow_dst_dir) == False:
		os.mkdir(snow_dst_dir)
	if os.path.isfile(snow_dst_file):
		os.remove(snow_dst_file)
	snow = driver.Create(snow_dst_file, xsize, ysize, 1, gdal.GDT_Float32, [ 'TILED=YES', 'COMPRESS=LZW' ])
	snow.SetGeoTransform(trans)
	snow.SetProjection(proj)
	
	data_loc = loc_band.ReadAsArray()

	snowheight = myconn.cursor()
	query = ("""
	select ref_loc, hneige from stations.meteo_crocus_lhb2015 
	where time = %s
	order by 1
	""")
	# query = ("""
	# select loc, hauteur_neige from stations.meteo_crocus 
	# where crocus_date = %s
	# order by 1
	# """)
				
	snowheight.execute(query,(day,))
		
	l = 0
	snow_height = numpy.zeros((snowheight.rowcount, 2), numpy.float32)

	for height in snowheight:
		for m in range(0,2):
			snow_height[l,m] = height[m]
		l=l+1
		
	height_rast = numpy.zeros((ysize, xsize), numpy.float32)
	
	for k in range(0,snow_height.shape[0]):
		height_rast = height_rast + snow_height[k,1]*(data_loc == snow_height[k,0])
	
	height_rast[data_loc == -9999] = -9999
	
	snow.GetRasterBand(1).WriteArray(height_rast)
	snow_band = snow.GetRasterBand(1)
	snow_band.SetNoDataValue(-9999)
	overviews = [2,4,8,16,32,64]
	snow.BuildOverviews("NEAREST", overviews)
	snow = None	
	
snowheight_sta("C:\\ds_test_data\\snow\\sta\\", "C:\\ds_test_data\\snow\\crocus_location.tif", "C:\\ds_test_data\\snow\\test\\test1512\\", "3811", "2006-12-15")
snowheight_sta("C:\\ds_test_data\\snow\\sta\\", "C:\\ds_test_data\\snow\\crocus_location.tif", "C:\\ds_test_data\\snow\\test\\test1512\\", "3811", "2008-12-15")
#snowheight_sta("C:\\ds_test_data\\snow\\sta\\", "C:\\ds_test_data\\snow\\crocus_location.tif", "C:\\ds_test_data\\snow\\test\\test1512\\ref\\", "3811", "2006-12-15")
#snowheight_sta("C:\\ds_test_data\\snow\\sta\\", "C:\\ds_test_data\\snow\\crocus_location.tif", "C:\\ds_test_data\\snow\\test\\test1512\\ref\\", "3811", "2008-12-15")

print "done"