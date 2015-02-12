import gdal
import numpy
import psycopg2
from subprocess import call
import os
import conn_param


def viability_index(snow_dir, sta_dir, dst_dir, ind, year):
	
	gdalwarp = "C:\Python276\Lib\site-packages\osgeo\gdalwarp.exe"
	
	season = str(year) + "-" + str(year + 1)
	

	newdir = dst_dir + season
	if os.path.isdir(newdir) == False:
		os.mkdir(newdir)
	
	#snow_data
	snow_file = snow_dir + "snow_" + season + ".tif"
	snowdays_rast = gdal.Open(snow_file)
	snowdays_trans = snowdays_rast.GetGeoTransform()
	snowdays = snowdays_rast.GetRasterBand(1)
	
	#prepare sql
	myconn = psycopg2.connect("host="+conn_param.host+" dbname="+conn_param.dbname+" user="+conn_param.user+" password="+conn_param.password)
	viability=myconn.cursor()
	query = """
	create table if not exists stations.viability_index_lhb2105(
		ind varchar(4), season integer, index float8)
	"""
	viability.execute(query)
	myconn.commit
	
	query = "delete from stations.viability_index_lhb2105 where season= %s and ind= %s"
	viability.execute(query,(year,ind,))
	myconn.commit
	
	#source data and raster properties
	src_ind = sta_dir + ind + ".tif"
	src_rast = gdal.Open(src_ind)
	src_trans = src_rast.GetGeoTransform()
	src_proj = src_rast.GetProjection()
	src_band = src_rast.GetRasterBand(1)
	mp_data = src_band.ReadAsArray()
	xsize = src_band.XSize
	ysize = src_band.YSize
	
	#Create raster ind_snowdays_season
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
	
	#compute raster extent
	x0 = src_trans[0] + (round((sta[0] - src_trans[0])/src_trans[1])*src_trans[1])
	x1 = (src_trans[0] + src_rast.RasterXSize*src_trans[1])-(round(((src_trans[0] + src_rast.RasterXSize*src_trans[1])-sta[2])/src_trans[1])*src_trans[1])
	y1 = src_trans[3] - (round((src_trans[3] - sta[3])/src_trans[5])*src_trans[5])
	y0 = (src_trans[3] - src_rast.RasterYSize*src_trans[5]) + (round((sta[1] - (src_trans[3] - src_rast.RasterYSize*src_trans[5]))/src_trans[5])*src_trans[5])
	te_str = str(x0) + " " + str(y0) + " " + str(x1) + " " + str(y1)
	
	snowdays_ind_dst = dst_dir + season + "\\" + ind + "_snowdays_" + season + ".tif"
	if os.path.isfile(snowdays_ind_dst):
		os.remove(snowdays_ind_dst)
	#vector data from postgis
	connString = "PG: host = "+conn_param.host+" dbname = "+conn_param.dbname+" user="+conn_param.user+" password="+conn_param.password
	sql = "select ind id, the_geom from stations.geo_dsa where ind = '" + sta[4] + "'"

	call(gdalwarp + " -co \"COMPRESS=LZW\" -co \"TILED=YES\" -cutline \"" + connString + "\" -csql \"" + sql + "\" -te "+ te_str +" -dstnodata -9999 " + snow_file + " " + snowdays_ind_dst, shell=True)
		
	#create new file 
	format = "GTiff"
	driver = gdal.GetDriverByName(format)
	dst_file = dst_dir + season + "\\viability_" + ind + ".tif"
	if os.path.isfile(dst_file):
		os.remove(dst_file)
	dst_ds = driver.Create(dst_file, xsize, ysize, 3, gdal.GDT_Byte, [ 'TILED=YES', 'COMPRESS=LZW' ])
	dst_ds.SetGeoTransform(src_trans)
	dst_ds.SetProjection(src_proj)
	viable_index_r = numpy.zeros((ysize, xsize), numpy.uint8)
	viable_index_g = numpy.zeros((ysize, xsize), numpy.uint8)
	viable_index_b = numpy.zeros((ysize, xsize), numpy.uint8)
	viable_index_r[numpy.isnan(mp_data)] = 1
	viable_index_g[numpy.isnan(mp_data)] = 1
	viable_index_b[numpy.isnan(mp_data)] = 1

	# viability index
	snowdays_rast = None
	snowdays_rast = gdal.Open(snowdays_ind_dst)
	snowdays = snowdays_rast.GetRasterBand(1)
	snow = snowdays.ReadAsArray()
	
	#compute validity index
	viable_pix = mp_data[snow >= 100]
	viability_index = numpy.sum(viable_pix[viable_pix > 0.])
	
	#Create 3 band raster based on number of days
		# Less than 25 days red
	viable_index_r = viable_index_r + 222 * numpy.logical_and(numpy.isfinite(mp_data), snow < 25)
	viable_index_g = viable_index_g + 45 * numpy.logical_and(numpy.isfinite(mp_data), snow < 25)
	viable_index_b = viable_index_b + 38 * numpy.logical_and(numpy.isfinite(mp_data), snow < 25)
	dst_ds.GetRasterBand(1).WriteArray(viable_index_r)
	dst_ds.GetRasterBand(2).WriteArray(viable_index_g)
	dst_ds.GetRasterBand(3).WriteArray(viable_index_b)
		# Between 25 and 50 days orange
	viable_index_r[viable_index_r != 1].fill(0)
	viable_index_g[viable_index_g != 1].fill(0)
	viable_index_b[viable_index_b != 1].fill(0)
	viable_index_r = viable_index_r + 254 * numpy.logical_and(numpy.isfinite(mp_data), numpy.logical_and(snow >= 25, snow < 50))
	viable_index_g = viable_index_g + 153 * numpy.logical_and(numpy.isfinite(mp_data), numpy.logical_and(snow >= 25, snow < 50))
	viable_index_b = viable_index_b + 41 * numpy.logical_and(numpy.isfinite(mp_data), numpy.logical_and(snow >= 25, snow < 50))
	dst_ds.GetRasterBand(1).WriteArray(viable_index_r)
	dst_ds.GetRasterBand(2).WriteArray(viable_index_g)
	dst_ds.GetRasterBand(3).WriteArray(viable_index_b)
		# Between 50 and 80 days yellow
	viable_index_r[viable_index_r != 1].fill(0)
	viable_index_g[viable_index_g != 1].fill(0)
	viable_index_b[viable_index_b != 1].fill(0)
	viable_index_r = viable_index_r + 255 * numpy.logical_and(numpy.isfinite(mp_data), numpy.logical_and(snow >= 50, snow < 80))
	viable_index_g = viable_index_g + 247 * numpy.logical_and(numpy.isfinite(mp_data), numpy.logical_and(snow >= 50, snow < 80))
	viable_index_b = viable_index_b + 0 * numpy.logical_and(numpy.isfinite(mp_data), numpy.logical_and(snow >= 50, snow < 80))
	dst_ds.GetRasterBand(1).WriteArray(viable_index_r)
	dst_ds.GetRasterBand(2).WriteArray(viable_index_g)
	dst_ds.GetRasterBand(3).WriteArray(viable_index_b)
		# Between 80 and 100 days light green
	viable_index_r[viable_index_r != 1].fill(0)
	viable_index_g[viable_index_g != 1].fill(0)
	viable_index_b[viable_index_b != 1].fill(0)
	viable_index_r = viable_index_r + 186 * numpy.logical_and(numpy.isfinite(mp_data), numpy.logical_and(snow >= 80, snow < 100))
	viable_index_g = viable_index_g + 228 * numpy.logical_and(numpy.isfinite(mp_data), numpy.logical_and(snow >= 80, snow < 100))
	viable_index_b = viable_index_b + 179 * numpy.logical_and(numpy.isfinite(mp_data), numpy.logical_and(snow >= 80, snow < 100))
	dst_ds.GetRasterBand(1).WriteArray(viable_index_r)
	dst_ds.GetRasterBand(2).WriteArray(viable_index_g)
	dst_ds.GetRasterBand(3).WriteArray(viable_index_b)
		# Between 100 and 120 days dark green
	viable_index_r[viable_index_r != 1].fill(0)
	viable_index_g[viable_index_g != 1].fill(0)
	viable_index_b[viable_index_b != 1].fill(0)
	viable_index_r = viable_index_r + 49 * numpy.logical_and(numpy.isfinite(mp_data), numpy.logical_and(snow >= 100, snow < 120))
	viable_index_g = viable_index_g + 163 * numpy.logical_and(numpy.isfinite(mp_data), numpy.logical_and(snow >= 100, snow < 120))
	viable_index_b = viable_index_b + 84 * numpy.logical_and(numpy.isfinite(mp_data), numpy.logical_and(snow >= 100, snow < 120))
	dst_ds.GetRasterBand(1).WriteArray(viable_index_r)
	dst_ds.GetRasterBand(2).WriteArray(viable_index_g)
	dst_ds.GetRasterBand(3).WriteArray(viable_index_b)
		# Between 120 and 150 days light blue
	viable_index_r[viable_index_r != 1].fill(0)
	viable_index_g[viable_index_g != 1].fill(0)
	viable_index_b[viable_index_b != 1].fill(0)
	viable_index_r = viable_index_r + 107 * numpy.logical_and(numpy.isfinite(mp_data), numpy.logical_and(snow >= 120, snow < 150))
	viable_index_g = viable_index_g + 174 * numpy.logical_and(numpy.isfinite(mp_data), numpy.logical_and(snow >= 120, snow < 150))
	viable_index_b = viable_index_b + 214 * numpy.logical_and(numpy.isfinite(mp_data), numpy.logical_and(snow >= 120, snow < 150))
	dst_ds.GetRasterBand(1).WriteArray(viable_index_r)
	dst_ds.GetRasterBand(2).WriteArray(viable_index_g)
	dst_ds.GetRasterBand(3).WriteArray(viable_index_b)
		# More than 150 days
	viable_index_r[viable_index_r != 1].fill(0)
	viable_index_g[viable_index_g != 1].fill(0)
	viable_index_b[viable_index_b != 1].fill(0)
	viable_index_r = viable_index_r + 8 * numpy.logical_and(numpy.isfinite(mp_data), snow >= 150)
	viable_index_g = viable_index_g + 81 * numpy.logical_and(numpy.isfinite(mp_data), snow >= 150)
	viable_index_b = viable_index_b + 156 * numpy.logical_and(numpy.isfinite(mp_data), snow >= 150)
	dst_ds.GetRasterBand(1).WriteArray(viable_index_r)
	dst_ds.GetRasterBand(2).WriteArray(viable_index_g)
	dst_ds.GetRasterBand(3).WriteArray(viable_index_b)
	
	dst_band = dst_ds.GetRasterBand(1)
	dst_band.SetNoDataValue(1)
	dst_band = dst_ds.GetRasterBand(2)
	dst_band.SetNoDataValue(1)
	dst_band = dst_ds.GetRasterBand(3)
	dst_band.SetNoDataValue(1)
	dst_ds = None
	snowdays_rast = None
		
	query = "insert into stations.viability_index_lhb2105 values(%s, %s, %s);"
	viability.execute(query,(ind, year, viability_index.tolist(),))
	myconn.commit()
	
viability_index("C:\\ds_test_data\\snow\\test\\", "C:\\ds_test_data\\snow\\sta\\", "C:\\ds_test_data\\snow\\test\\", "3811", 2006)
viability_index("C:\\ds_test_data\\snow\\", "C:\\ds_test_data\\snow\\sta\\", "C:\\ds_test_data\\snow\\test\\test_ref\\", "3811", 2006)