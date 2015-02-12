import gdal
import numpy
import psycopg2
from subprocess import call
import os
import conn_param

def snow_season(src_loc, snow_dst_dir, year):
	
	myconn = psycopg2.connect("host="+conn_param.host+" dbname="+conn_param.dbname+" user="+conn_param.user+" password="+conn_param.password)
	
	season = str(year) + "-" + str(year + 1)

	loc_rast = gdal.Open(src_loc)
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
	snow_dst_file = snow_dst_dir + "snow_" + season + ".tif"
	if os.path.isdir(snow_dst_dir) == False:
		os.mkdir(snow_dst_dir)
	if os.path.isfile(snow_dst_file):
		os.remove(snow_dst_file)
	snow = driver.Create(snow_dst_file, xsize, ysize, 1, gdal.GDT_Int16, [ 'TILED=YES', 'COMPRESS=LZW' ])
	snow.SetGeoTransform(trans)
	snow.SetProjection(proj)
	

	for i in range(0, ysize, y_block_size):
		if i + y_block_size < ysize:
			rows = y_block_size
		else:
			rows = ysize - i
		for j in range(0, xsize, x_block_size):
			if j + x_block_size < xsize:
				cols = x_block_size
			else:
				cols = xsize - j

			data_loc = loc_band.ReadAsArray(j, i, cols, rows)
			
			unique_loc = numpy.unique(data_loc)
			loc_list = ', '.join(map(str, unique_loc))
			
			begin = str(year) + "-11-01"
			end = str(year+1) + "-04-30"
			
			snowdays = myconn.cursor()
			query = ("""
			select ref_loc, count(time) nb_days from stations.meteo_crocus_lhb2015 
			where ref_loc in (select unnest(string_to_array(%s,', '))::integer)
			and time between %s and %s
			and hneige >= 0.3
			group by 1 
			order by 1 
			""")
						
			snowdays.execute(query,(loc_list,begin,end,))
			
			if snowdays.rowcount == 0:
				nb_days = numpy.zeros((rows, cols), numpy.int16)
				nb_days[data_loc==0] = -9999
				snow.GetRasterBand(1).WriteArray(nb_days,j,i)
				print i,j, "no test"
			else:		
				l = 0
				snow_days = numpy.zeros((snowdays.rowcount, 2), numpy.int16)

				for days in snowdays:
					for m in range(0,2):
						snow_days[l,m] = days[m]
					l=l+1
					
				nb_days = numpy.zeros((rows, cols), numpy.int16)
				
				for k in range(0,snow_days.shape[0]):
					nb_days = nb_days + snow_days[k,1]*(data_loc == snow_days[k,0])
				
				nb_days[data_loc == 0] = -9999
				
				print i,j
				snow.GetRasterBand(1).WriteArray(nb_days,j,i)
				
	snow_band = snow.GetRasterBand(1)
	snow_band.SetNoDataValue(-9999)
	overviews = [2,4,8,16,32,64]
	snow.BuildOverviews("NEAREST", overviews)
	snow = None	
	
snow_season("C:\ds_test_data\snow\crocus_location.tif", "C:\\ds_test_data\\snow\\test\\", 2006)

print "done"