import gdal
import numpy
import psycopg2
from subprocess import call
import os
import conn_param

#####################Functions

def reclass_rast(classification_values, classification_output_values, src_file, dst_file):

	rast = gdal.Open(src_file)
	band = rast.GetRasterBand(1)

	xsize = band.XSize
	ysize = band.YSize
	block_sizes = band.GetBlockSize()
	x_block_size = block_sizes[0]
	y_block_size = block_sizes[1]

	max_value = band.GetMaximum()
	min_value = band.GetMinimum()
	if max_value == None or min_value == None:
		stats = band.GetStatistics(0, 1)
		max_value = stats[1]
		min_value = stats[0]
		
	format = "GTiff"
	driver = gdal.GetDriverByName(format)

	dst_ds = driver.Create(dst_file, xsize, ysize, 1, gdal.GDT_Int16 )
	dst_ds.SetGeoTransform(rast.GetGeoTransform())
	dst_ds.SetProjection(rast.GetProjection())

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

			data = band.ReadAsArray(j, i, cols, rows)
			
			r = numpy.zeros((rows, cols), numpy.int16)

			for k in range(len(classification_values) - 1):
				if classification_values[k] < max_value and (classification_values[k + 1] > min_value ):
					r = r + classification_output_values[k] * numpy.logical_and(data >= classification_values[k], data < classification_values[k + 1])
			if classification_values[k + 1] < max_value:
				r = r + classification_output_values[k + 1] * (data >= classification_values[k + 1])

			
			dst_ds.GetRasterBand(1).WriteArray(r,j,i)

	dst_ds = None
	
def get_location(path):

	dst_crocus_alt = path + "crocus_altitude.tif"
	dst_crocus_slope = path + "crocus_slope.tif"
	dst_crocus_aspect = path + "crocus_aspect.tif"
	dst_massif = path + "crocus_massifs_safran.tif"

	#Opening mandatory rasters
	alt_rast = gdal.Open(dst_crocus_alt)
	alt_band = alt_rast.GetRasterBand(1)

	slope_rast = gdal.Open(dst_crocus_slope)
	slope_band = slope_rast.GetRasterBand(1)

	aspect_rast = gdal.Open(dst_crocus_aspect)
	aspect_band = aspect_rast.GetRasterBand(1)

	massif_rast = gdal.Open(dst_massif)
	massif_band = massif_rast.GetRasterBand(1)

	#Get metadata
	xsize = alt_band.XSize
	ysize = alt_band.YSize
	block_sizes = alt_band.GetBlockSize()
	x_block_size = block_sizes[0]
	y_block_size = block_sizes[1]
	max_value = alt_band.GetMaximum()
	min_value = alt_band.GetMinimum()
	if max_value == None or min_value == None:
		stats = alt_band.GetStatistics(0, 1)
		max_value = stats[1]
		min_value = stats[0]
	trans = alt_rast.GetGeoTransform()
	proj = alt_rast.GetProjection()

	format = "GTiff"
	driver = gdal.GetDriverByName(format)
	dst_file = path + "crocus_location_tmp.tif"
	if os.path.isfile(dst_file):
		os.remove(dst_file)	
	dst_ds = driver.Create(dst_file, xsize, ysize, 1, gdal.GDT_Int16 )
	dst_ds.SetGeoTransform(trans)
	dst_ds.SetProjection(proj)

	###################BLOCK VERSION
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

			data_alt = alt_band.ReadAsArray(j, i, cols, rows)
			data_slope = slope_band.ReadAsArray(j, i, cols, rows)
			data_aspect = aspect_band.ReadAsArray(j, i, cols, rows)
			data_massif = massif_band.ReadAsArray(j, i, cols, rows)
			
			unique_alt = numpy.unique(data_alt)
			alt_list = ', '.join(map(str, unique_alt))
			unique_slope = numpy.unique(data_slope)
			slope_list = ', '.join(map(str, unique_slope))
			unique_aspect = numpy.unique(data_aspect)
			aspect_list = ', '.join(map(str, unique_aspect))
			unique_massif = numpy.unique(data_massif)
			massif_list = ', '.join(map(str, unique_massif))
						
			#get loc query
			location = myconn.cursor()
			query = ("""
			with
			ref as (
			select distinct loc::int, alti::int, slope::int, aspect::int, ref_massif_meteo::int from stations.meteo_crocus_location a
			join stations.passage_meteo_massif_ind b on a.ref_point_crocus = b.ref_point_crocus
			),
			a as (select * from ref where alti in (select unnest(string_to_array(%s,', '))::integer)),
			b as (select * from a where slope in (select unnest(string_to_array(%s,', '))::integer) and slope != 0),
			c as (select * from b where aspect in (select unnest(string_to_array(%s,', '))::integer))
			
			select * from c where ref_massif_meteo in (select unnest(string_to_array(%s,', '))::integer)
			""")
			
			location.execute(query,(alt_list, slope_list, aspect_list, massif_list,))
			
			if location.rowcount == 0:
				print i,j, "no test"
			else:			
				l = 0
				crocus_loc = numpy.zeros((location.rowcount, 5), numpy.int16)

				for loc in location:
					for m in range(0,5):
						crocus_loc[l,m] = loc[m]
					l=l+1

				loc_output = numpy.zeros((rows, cols), numpy.int16)
						
				for k in range(0,crocus_loc.shape[0]):
					#print i,j,k
					test1 = numpy.logical_and(data_alt == crocus_loc[k,1], data_slope == crocus_loc[k,2])
					test2 = numpy.logical_and(data_aspect == crocus_loc[k,3], data_massif == crocus_loc[k,4])
					loc_output = loc_output + crocus_loc[k,0]*numpy.logical_and(test1,test2)
					
				#slope = 0 and no aspect
				
				query = ("""
				with
				ref as (
				select distinct loc::int, alti::int, slope::int, aspect::int, ref_massif_meteo::int from stations.meteo_crocus_location a
				join stations.passage_meteo_massif_ind b on a.ref_point_crocus = b.ref_point_crocus
				),
				a as (select * from ref where alti in (select unnest(string_to_array(%s,', '))::integer)),
				b as (select * from a where slope = 0)
							
				select * from b where ref_massif_meteo in (select unnest(string_to_array(%s,', '))::integer)
				""")
				
				location.execute(query,(alt_list, massif_list,))
				
				if location.rowcount == 0:
					print "no null aspect test"
				else:			
					l = 0
					crocus_loc = numpy.zeros((location.rowcount, 5), numpy.int16)

					for loc in location:
						for m in range(0,5):
							crocus_loc[l,m] = loc[m]
						l=l+1
							
					for k in range(0,crocus_loc.shape[0]):
						#print i,j,k
						test1 = numpy.logical_and(data_alt == crocus_loc[k,1], data_slope == 0)
						loc_output = loc_output + crocus_loc[k,0]*numpy.logical_and(test1,data_massif == crocus_loc[k,4])

				
				dst_ds.GetRasterBand(1).WriteArray(loc_output,j,i)
					
	dst_ds = None
	dst_file_tiled = path + "crocus_location.tif"
	if os.path.isfile(dst_file_tiled):
		os.remove(dst_file_tiled)
	call(gdalwarp + " -co \"COMPRESS=LZW\" -co \"TILED=YES\" " + dst_file + " " + dst_file_tiled, shell=True)

			
def snow_season(src_loc, snow_dst_file, year):
	
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
	if os.path.isfile(snow_dst_file):
		os.remove(snow_dst_file)
	snow = driver.Create(snow_dst_file, xsize, ysize, 1, gdal.GDT_Int16, [ 'TILED=YES', 'COMPRESS=LZW' ])
	snow.SetGeoTransform(trans)
	snow.SetProjection(proj)
	snow.SetNoDataValue(-9999)

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
			select loc, count(crocus_date) nb_days from stations.meteo_crocus 
			where loc in (select unnest(string_to_array(%s,', '))::integer)
			and crocus_date between %s and %s
			and hauteur_neige >= 0.3
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
	snow.BuildOverviews("NEAREST", 6, {2,4,8,16,32,64})
	snow = None	

def mp_resort_rast(src_dir, dst_dir, ind):

	gdalwarp = "C:\Python276\Lib\site-packages\osgeo\gdalwarp.exe"
	
	#psycopg2 connection to DB
	myconn = psycopg2.connect("host="+conn_param.host+" dbname="+conn_param.dbname+" user="+conn_param.user+" password="+conn_param.password)
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
	
		#new rasters extent
	src_crocus_alt = src_dir + "crocus_altitude.tif"
	src_rast = gdal.Open(src_crocus_alt)
	src_trans = src_rast.GetGeoTransform()
	
	src_crocus_slope = src_dir + "crocus_slope.tif"
	src_crocus_aspect = src_dir + "crocus_aspect.tif"

	sta = cur.fetchone()
	print sta[4]
	x0 = src_trans[0] + (round((sta[0] - src_trans[0])/src_trans[1])*src_trans[1])
	x1 = (src_trans[0] + src_rast.RasterXSize*src_trans[1])-(round(((src_trans[0] + src_rast.RasterXSize*src_trans[1])-sta[2])/src_trans[1])*src_trans[1])
	y1 = src_trans[3] - (round((src_trans[3] - sta[3])/src_trans[5])*src_trans[5])
	y0 = (src_trans[3] - src_rast.RasterYSize*src_trans[5]) + (round((sta[1] - (src_trans[3] - src_rast.RasterYSize*src_trans[5]))/src_trans[5])*src_trans[5])
	te_str = str(x0) + " " + str(y0) + " " + str(x1) + " " + str(y1)
	dst_file = dst_dir + "crocus_alt_" + sta[4] + ".tif"
	dst_file_slope = dst_dir + "crocus_slope_" + sta[4] + ".tif"
	dst_file_aspect = dst_dir + "crocus_aspect_" + sta[4] + ".tif"
	if os.path.isfile(dst_file):
		os.remove(dst_file)
	if os.path.isdir(dst_dir) == False:
		os.mkdir(dst_dir)
	#vector data from postgis
	connString = "PG: host = "+conn_param.host+" dbname = "+conn_param.dbname+" user="+conn_param.user+" password="+conn_param.password
	sql = "select ind id, the_geom from stations.geo_dsa where ind = '" + sta[4] + "'"

	#cutting and prepare data to build resort mp raster
	call(gdalwarp + " -co \"COMPRESS=LZW\" -co \"TILED=YES\" -cutline \"" + connString + "\" -csql \"" + sql + "\" -te "+ te_str +" -dstnodata -9999 " + src_crocus_alt + " " + dst_file, shell=True)
	call(gdalwarp + " -co \"COMPRESS=LZW\" -co \"TILED=YES\" -cutline \"" + connString + "\" -csql \"" + sql + "\" -te "+ te_str +" -dstnodata -9999 " + src_crocus_slope + " " + dst_file_slope, shell=True)
	call(gdalwarp + " -co \"COMPRESS=LZW\" -co \"TILED=YES\" -cutline \"" + connString + "\" -csql \"" + sql + "\" -te "+ te_str +" -dstnodata -9999 " + src_crocus_aspect+ " " + dst_file_aspect, shell=True)
	
	alt_rast = gdal.Open(dst_file)
	trans = alt_rast.GetGeoTransform()
	proj = alt_rast.GetProjection()
	alt_band = alt_rast.GetRasterBand(1)
	alt_arr = alt_band.ReadAsArray()
	xsize = alt_band.XSize
	ysize = alt_band.YSize
	
	format = "GTiff"
	driver = gdal.GetDriverByName(format)
	dst_file = dst_dir + "mp_" + sta[4] + ".tif"
	if os.path.isfile(dst_file):
		os.remove(dst_file)
	dst_ds = driver.Create(dst_file, xsize, ysize, 1, gdal.GDT_Float32)
	dst_ds.SetGeoTransform(trans)
	dst_ds.SetProjection(proj)
	mp_data = numpy.zeros((ysize, xsize), numpy.float32)
	
	#get alt and mp_part for sta
	ski_data = myconn.cursor()
	query = """
	select alti_crocus, mp, mp_tot from stations.geo_enveloppes_rm_alpes_alti_crocus
	where indicatif_station = %s;"""
	ski_data.execute(query,(sta[4],))
	for ski in ski_data:
		 nb_pix = (alt_arr == ski[0]).sum()
		 if nb_pix != 0:
			 pix_val = (float(ski[1]) / nb_pix / ski[2]) * 100
			 mp_data[alt_arr == ski[0]] = (pix_val)
				
	mp_data[mp_data == 0] = numpy.nan
	dst_ds.GetRasterBand(1).WriteArray(mp_data)
	dst_ds = None	


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
	src_ind = sta_dir + "mp_" + ind + ".tif"
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
		# Less than 100 days red
	viable_index_r = viable_index_r + 222 * numpy.logical_and(numpy.isfinite(mp_data), snow < 100)
	viable_index_g = viable_index_g + 45 * numpy.logical_and(numpy.isfinite(mp_data), snow < 100)
	viable_index_b = viable_index_b + 38 * numpy.logical_and(numpy.isfinite(mp_data), snow < 100)
	dst_ds.GetRasterBand(1).WriteArray(viable_index_r)
	dst_ds.GetRasterBand(2).WriteArray(viable_index_g)
	dst_ds.GetRasterBand(3).WriteArray(viable_index_b)
		# More than 100 days dark green
	viable_index_r[viable_index_r != 1].fill(0)
	viable_index_g[viable_index_g != 1].fill(0)
	viable_index_b[viable_index_b != 1].fill(0)
	viable_index_r = viable_index_r + 49 * numpy.logical_and(numpy.isfinite(mp_data), snow >= 100 )
	viable_index_g = viable_index_g + 163 * numpy.logical_and(numpy.isfinite(mp_data), snow >= 100)
	viable_index_b = viable_index_b + 84 * numpy.logical_and(numpy.isfinite(mp_data), snow >= 100)
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
		
	query = "insert into stations.viability_index values(%s, %s, %s);"
	viability.execute(query,(ind, year, viability_index.tolist(),))
	myconn.commit()

# #####################Use gdal utilities to compute slopes, aspects and retrieve MF mountain ranges

	# #gdal utilities
# gdalwarp = "C:\Python276\Lib\site-packages\osgeo\gdalwarp.exe"	
# gdaldem = "C:\Python276\Lib\site-packages\osgeo\gdaldem.exe"
# gdaltranslate = "C:\Python276\Lib\site-packages\osgeo\gdal_translate.exe"
# gdal_rasterize = "C:\Python276\Lib\site-packages\osgeo\gdal_rasterize.exe"
	# #files paths
# src_mnt = "C:\ds_test_data\ign_mnt25_alpes_2154.tif"
# src = "C:\ds_test_data\snow\ign_mnt25_alpes_2154_crop.tif"
# dst_slope = "C:\ds_test_data\snow\ign_mnt25_slopes_alpes.tif"
# dst_aspect = "C:\ds_test_data\snow\ign_mnt25_aspects_alpes.tif"
# dst_massif = "C:\ds_test_data\snow\crocus_massifs_safran.tif"
	# #vector data from postgis
# connString = "PG: host = "+conn_param.host+" dbname = "+conn_param.dbname+" user="+conn_param.user+" password="+conn_param.password
# sql = "select massifs_id id, the_geom from spatial.geo_massifs_meteo_france where massifs_id <= 22 or massifs_id = 40"
	# #psycopg2 connection to DB
# myconn = psycopg2.connect("host="+conn_param.host+" dbname="+conn_param.dbname+" user="+conn_param.user+" password="+conn_param.password)
# cur = myconn.cursor()
# cur.execute("""
# with a as (
# select st_envelope(st_buffer(st_union(the_geom),25)) geom
# from spatial.geo_massifs_meteo_france where massifs_id <= 22 or massifs_id = 40
# )
# select st_xmin(geom), st_ymin(geom), st_xmax(geom), st_ymax(geom) from a
# """)
	# #new rasters extent
# src_rast = gdal.Open(src_mnt)
# src_trans = src_rast.GetGeoTransform()
# extent = cur.fetchone()
# print extent
# x0 = src_trans[0] + (round((extent[0] - src_trans[0])/src_trans[1])*src_trans[1])
# x1 = (src_trans[0] + src_rast.RasterXSize*src_trans[1])-(round(((src_trans[0] + src_rast.RasterXSize*src_trans[1])-extent[2])/src_trans[1])*src_trans[1])
# y1 = src_trans[3] - (round((src_trans[3] - extent[3])/src_trans[5])*src_trans[5])
# y0 = (src_trans[3] - src_rast.RasterYSize*src_trans[5]) + (round((extent[1] - (src_trans[3] - src_rast.RasterYSize*src_trans[5]))/src_trans[5])*src_trans[5])
# te_str = str(x0) + " " + str(y0) + " " + str(x1) + " " + str(y1)

# #cutting and prepare data to build new rasters (location and snow below)
# call(gdalwarp + " -co \"COMPRESS=LZW\" -co \"TILED=YES\" -cutline \"" + connString + "\" -csql \"" + sql + "\" -te "+ te_str +" " + src_mnt + " " + src, shell=True)

# #slope and aspect
# call(gdaldem + " slope -compute_edges -co \"COMPRESS=LZW\" -co \"TILED=YES\" " + src + " " + dst_slope, shell=True)
# call(gdaldem + " aspect -compute_edges -co \"COMPRESS=LZW\" -co \"TILED=YES\" " + src + " " + dst_aspect, shell=True)
# #rasterization
# call(gdalwarp + " -co \"COMPRESS=LZW\" -co \"TILED=YES\" " + src + " " + dst_massif, shell=True)
# call(gdal_rasterize + " -a id \"" + connString + "\" -sql \"" + sql + "\" " + dst_massif, shell=True)

# print "new rasters created"

# #####################Reclass rasters to meet crocus output

# #reclass elevations
# old_values = [0, 150,450,750,1050,1350,1650,1950,2250,2550,2850,3150,3450,3750,4050,4350,4650]
# new_values = [0,300,600,900,1200,1500,1800,2100,2400,2700,3000,3300,3600,3900,4200,4500,4800]
# dst_crocus_alt_tmp = "C:\ds_test_data\snow\crocus_altitude_tmp.tif"
# dst_crocus_alt = "C:\ds_test_data\snow\crocus_altitude.tif"
# reclass_rast(old_values, new_values, src, dst_crocus_alt_tmp)
# call(gdalwarp + " -co \"COMPRESS=LZW\" -co \"TILED=YES\" " + dst_crocus_alt_tmp + " " + dst_crocus_alt, shell=True)
# print "elevation reclassed"
# #reclass slopes	
# old_values = [0,5,15,25,35,45]
# new_values = [0,10,20,30,40,50]
# dst_crocus_slope_tmp = "C:\ds_test_data\snow\crocus_slope_tmp.tif"
# dst_crocus_slope = "C:\ds_test_data\snow\crocus_slope.tif"
# reclass_rast(old_values, new_values, dst_slope, dst_crocus_slope_tmp)
# call(gdalwarp + " -co \"COMPRESS=LZW\" -co \"TILED=YES\" " + dst_crocus_slope_tmp + " " + dst_crocus_slope, shell=True)
# print "slopes reclassed"
# #reclass aspects
# old_values = [0, 22.5,67.5,112.5,157.5,202.5,247.5,292.5,337.5]
# new_values = [0,45,90,135,180,225,270,315,0]
# dst_crocus_aspect_tmp = "C:\ds_test_data\snow\crocus_aspect_tmp.tif"
# dst_crocus_aspect = "C:\ds_test_data\snow\crocus_aspect.tif"
# reclass_rast(old_values, new_values, dst_aspect, dst_crocus_aspect_tmp)
# call(gdalwarp + " -co \"COMPRESS=LZW\" -co \"TILED=YES\" " + dst_crocus_aspect_tmp + " " + dst_crocus_aspect, shell=True)
# print "aspects reclassed"

#######################Location raster and snowdays number

path = "C:\ds_test_data\snow\\"
	
# get_location(path)	
for season in range(2000,2012,1):
	#print season
	#snow_season(path, season)
		
	###################COMPUTE DSA
		
	myconn = psycopg2.connect("host="+conn_param.host+" dbname="+conn_param.dbname+" user="+conn_param.user+" password="+conn_param.password)
	cur = myconn.cursor()
	cur.execute("""
	with foo as 
	(select * from spatial.geo_inpn_parcs_naturels_regionaux_2010
	where gid = 1)

	select distinct bar.indicatif_station, nom_indicatif from stations.geo_rm_sta_alpes_ind bar
	join stations.geo_dsf a on bar.indicatif_station = a.indicatif_station
	join foo on st_intersects(foo.the_geom, bar.the_geom)
	order by 1
	""")
	#"select distinct ind from stations.geo_dsa a, spatial.geo_massifs_meteo_france b where st_intersects(a.the_geom, b.the_geom) order by 1"
	
	src_dir = "C:\\ds_test_data\\snow\\"
	dst_dir = "C:\\ds_test_data\\snow\\sta_adamont\\"
	
	
	for ind in cur:
		print ind[0]
		mp_resort_rast(src_dir, dst_dir, ind[0])
		viability_index(src_dir, dst_dir, dst_dir, ind[0], season)
	
print  "done"