import gdal
import numpy
import psycopg2
from subprocess import call
import os
import conn_param
			
def snow_days_gif(path, date):
	gdaldem = "C:\Python276\Lib\site-packages\osgeo\gdaldem.exe"	
	
	myconn = psycopg2.connect("host="+conn_param.host+" dbname="+conn_param.dbname+" user="+conn_param.user+" password="+conn_param.password)

	src_loc = "C:\ds_test_data\snow\crocus_location.tif"
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
	snow_dst_file_tmp = path + "snow_tmp.tif"
	if os.path.isfile(snow_dst_file_tmp):
		os.remove(snow_dst_file_tmp)
	snow = driver.Create(snow_dst_file_tmp, xsize, ysize, 1, gdal.GDT_Float32 )
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
						
			snowh = myconn.cursor()
			query = ("""
			select loc, round(hauteur_neige,2)*100 hn from stations.meteo_crocus 
			where loc in (select unnest(string_to_array(%s,', '))::integer)
			and crocus_date = %s 
			order by 1 
			""")
						
			snowh.execute(query,(loc_list,date,))
			
			if snowh.rowcount == 0:
				height = numpy.zeros((rows, cols), numpy.float32)
				height[data_loc==0] = -9999
				snow.GetRasterBand(1).WriteArray(height,j,i)
				print i,j, "no test"
			else:		
				l = 0
				height = numpy.zeros((snowh.rowcount, 2), numpy.float32)

				for h in snowh:
					for m in range(0,2):
						height[l,m] = h[m]
					l=l+1
					
				snowheight = numpy.zeros((rows, cols), numpy.float32)
				
				for k in range(0,height.shape[0]):
					snowheight = snowheight + height[k,1]*(data_loc == height[k,0])
				
				snowheight[data_loc == 0] = -9999
				
				print i,j
				snow.GetRasterBand(1).WriteArray(snowheight,j,i)

	snow_band = snow.GetRasterBand(1)
	snow_band.SetNoDataValue(-9999)
	snow = None	
	snow_dst_file = path + "snow_test_" + str(date) + ".png"
	if os.path.isfile(snow_dst_file):
		os.remove(snow_dst_file)
	color_file = path + "color_test.txt"
	call(gdaldem + " color-relief " + snow_dst_file_tmp + " " + color_file + " " + snow_dst_file + " -of PNG", shell=True)

path = "C:\ds_test_data\\test_snow_gif\\"
myconn = psycopg2.connect("host="+conn_param.host+" dbname="+conn_param.dbname+" user="+conn_param.user+" password="+conn_param.password)
days = myconn.cursor()
days.execute("""
select distinct crocus_date from stations.meteo_crocus
where crocus_date between '2009-01-01' and '2009-12-31'
order by 1
""")
for date in days:
	print date[0]
	snow_days_gif(path,date[0])
	
# snow_days_gif(path,"2009-01-01")