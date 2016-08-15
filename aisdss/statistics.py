# -*- coding: utf-8 -*-
"""
Created on Sun Jun 19 13:37:46 2016

@author: dimze
"""

from osgeo import ogr
import pandas as pd
import datetime
import matplotlib
import matplotlib.pyplot as plt
import os
matplotlib.style.use('ggplot')


def FeatureToDict(feature):
	"""Transform single osgeo.org.Feature to python dict"""
	feature_dict = {}
	attributes = feature.items()
	point = feature.geometry()
	if point:
		geometry = {'x': point.GetX(), 'y': point.GetY()}
		feature_dict = {'attributes': attributes, 'geometry': geometry}
	else:
		return attributes
	return feature_dict
	
def LayerToDict(layer):
	"""Transform osgeo.org.Layer to python dict"""
	features = []
	for feature in layer:
		features.append(feature)
	features_dict = map(FeatureToDict, features)
	return features_dict
	
def CreateDataFrame(data, input_fields, output_fields, sort_fields, duplicate_fields):
	"""Creates DataFrame based on input JSON-features file."""
	df = pd.DataFrame(data)
	dfs = pd.DataFrame(pd.DataFrame(df[input_fields[0]].tolist()))
	for field in input_fields[1:]:
		dfs = dfs.join(pd.DataFrame(df[field].tolist()))
	rows = pd.DataFrame(columns=output_fields)
	rows = dfs[output_fields]
	rows = rows.sort_values(by=sort_fields)
	# remove duplicate coordinates
	rows = rows.drop_duplicates(duplicate_fields)
	return rows
	
def AddData(ais_data, vessel, voyage):
	"""Adds vessel and voyage data to ais_data"""
	if 'MMSI' in voyage.columns:
		voyage = voyage.drop('MMSI', 1)
	output = ais_data.merge(vessel, how='inner', on='MMSI')
	output = output.merge(voyage, how='inner', on='VoyageID')
	return output
	
def Categorize(data, field, bins, labels, fill):
	"""Categorizes vessels"""
	categorized = pd.DataFrame(data)
	categorized[field] = pd.cut(data[field], bins, labels=labels, right=False)
	categorized[field] = categorized[[field]].fillna(fill)
	return categorized
	
def ExtractDate(input_date):
	"""Extracts date from datetime"""
	dt = datetime.datetime.strptime(input_date, "%Y/%m/%d %H:%M:%S")
	date = datetime.date(dt.year, dt.month, dt.day)
	return date
	
def Plot(data, date_field, categorical_field, title):
	"""Plots quantity of each vessel type in time"""
	fields = [date_field, categorical_field]
	for_plot = pd.DataFrame(data[fields])
	for_plot[date_field] = pd.to_datetime(for_plot[date_field].apply(ExtractDate))
	size = for_plot.groupby(fields).size()
	grouped = pd.DataFrame(size, columns=['Count'])
	grouped = grouped.reset_index()
	new_fields = for_plot[categorical_field].unique()
	for field in new_fields:
		grouped[field] = grouped.apply(lambda x: x['Count'] if x[categorical_field] == field else 0, axis=1)
	grouped = grouped.groupby(date_field).apply(lambda tdf: pd.Series(  dict([[vv,tdf[vv].tolist()] for vv in tdf])  ))
	grouped = grouped.drop(fields + ['Count'], axis=1)
	for field in new_fields:
		grouped[field] = grouped.apply(lambda x: max(x[field]) if x[field] else 0, axis = 1)
	grouped.index.name = None
	fig = grouped.plot(figsize = (16, 6), title=title)
	return fig

def ExtractData(filename, layer_names):
	"""Extracting data from FileGDB as pandas.DataFrame"""
	driver = ogr.GetDriverByName("OpenFileGDB")
	ds = driver.Open(filename, 0)

	layer_dicts = []

	for layer_name in layer_names:
		layer = ds.GetLayerByName(layer_name)
		layer_dict = LayerToDict(layer)
		layer_dicts.append(layer_dict)

	layer_dfs = []

	input_fields = ['geometry', 'attributes']
	output_fields = ["MMSI", "BaseDateTime", "VoyageID", 'x', 'y']
	sort_fields = ['BaseDateTime']
	duplicate_fields = ['x', 'y', 'MMSI', 'VoyageID']
	layer_df = CreateDataFrame(layer_dicts[0], input_fields, output_fields, sort_fields, duplicate_fields)
	dropped = layer_df.drop_duplicates('VoyageID')
	layer_dfs.append(dropped)

	for layer_dict in layer_dicts[1:]:
		layer_df = pd.DataFrame(layer_dict)
		layer_dfs.append(layer_df)

	#(dropped, vessel_df, voyage_df) = layer_dfs
	full_data = AddData(dropped, layer_dfs[1], layer_dfs[2])
	return full_data

def CategorizeData(data):
	"""Categorizes data by type, size and hazard with predefined parameters"""
	type_field = 'VesselType'
	type_bins = [0, 70, 80, 90]
	type_labels = ['Other', 'Cargo', 'Tanker']
	categorized_by_type = Categorize(data, type_field, type_bins, type_labels, type_labels[0])
	# drop rows with VesselType == 'Other'
	categorized_by_type = categorized_by_type[categorized_by_type['VesselType'] != 'Other']

	size_field = 'Length'
	size_bins = [0, 100, 150, 205, 226, 245, 285, 330, 415, 500]
	size_labels  = ['Other', 'Handysize', 'Handymax', 'Coastal Tanker', 'Seawaymax', 'Aframax', 'Suez-Max', 'VLCC', 'ULCC']
	categorized_by_size = Categorize(categorized_by_type, size_field, size_bins, size_labels, size_labels[0])

	hazard_field = 'Cargo'	
	hazard_bins = [0, 1, 2, 3, 4, 5]
	hazard_labels = ['No Hazard', 'Haz A', 'Haz B', 'Haz C', 'Haz D']
	categorized_by_hazard = pd.DataFrame(categorized_by_size)
	#categorized_by_hazard[hazard_field] = categorized_by_size[hazard_field].apply(lambda x: x % 10 if x > 10 else x)
	categorized_by_hazard[hazard_field] = categorized_by_size[hazard_field].apply(lambda x: x % 10 if x % 10 < 5 else 0)
	categorized_by_hazard = Categorize(categorized_by_size, hazard_field, hazard_bins, hazard_labels, hazard_labels[-1])
	return categorized_by_hazard

def SaveData(data, folder, name):
	"""Saves DataFrame into csv"""
	filename = os.path.join(folder, name + '.csv')
	data.to_csv(filename, sep=';')
	return filename

def VisualizeData(data, folder):
	"""Builds 3 predefined plots on categorized data"""
	date_field = 'BaseDateTime'
	type_field = 'VesselType'
	ax = Plot(data, date_field, type_field, 'Vessel Type')
	fig = ax.get_figure()
	fig.savefig(os.path.join(folder, 'VesselType.png'))
	
	size_field = 'Length'
	ax = Plot(data, date_field, size_field, 'Vessel Size')
	fig = ax.get_figure()
	fig.savefig(os.path.join(folder, 'VesselSize.png'))
	
	hazard_field = 'Cargo'
	ax = Plot(data, date_field, hazard_field, 'Hazard Cargo')
	fig = ax.get_figure()
	fig.savefig(os.path.join(folder, 'HazardCargo.png'))

	plt.show()

def Main():
	filename = "data/zone2/Zone2_2014_01.gdb.zip"
	layer_names = ['Zone2_2014_01_Broadcast', 'Zone2_2014_01_Vessel', 'Zone2_2014_01_Voyage']
	full_data = ExtractData(filename, layer_names)

	categorized = CategorizeData(full_data)

	date_field = 'BaseDateTime'
	type_field = 'VesselType'
	Plot(categorized, date_field, type_field, 'Vessel Type')
	
	size_field = 'Length'
	Plot(categorized, date_field, size_field, 'Vessel Size')
	
	hazard_field = 'Cargo'
	Plot(categorized, date_field, hazard_field, 'Hazard Cargo')

	plt.show()

if __name__ == '__main__':
	Main()