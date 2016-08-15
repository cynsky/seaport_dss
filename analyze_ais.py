from aisdss import *
from string import Template
import argparse
import os
import pandas as pd


def download_and_categorize(zone, year, months, folder):
	temp_url = Template('https://coast.noaa.gov/htdata/CMSP/AISDataHandler/' + \
					'${year}/${month}/Zone${zone}_${year}_${month}${gdb}.zip')
	filenames = []
	gdb = '.gdb' if int(year) < 2014 else '';
	for month in months:
		url = temp_url.substitute(year=year, month=month, zone=zone, gdb=gdb)
		path = download_data.download_file(url, folder)
		layer_name = url.split('/')[-1].split('.')[0]
		layer_names = [layer_name + '_Broadcast', layer_name + '_Vessel', layer_name + '_Voyage']
		full_data = statistics.ExtractData(path, layer_names)
		categorized = statistics.CategorizeData(full_data)
		filename = statistics.SaveData(categorized, folder, layer_name)
		filenames.append(filename)
	return filenames

def join(filenames):
	df = pd.DataFrame()
	for filename in filenames:
		df_tmp = pd.read_csv(filename, sep=';')
		df = df.append(df_tmp)
	df = df.drop_duplicates('VoyageID')
	folder = filenames[0][:filenames[0].rfind(os.sep)]
	filename = statistics.SaveData(df, folder, 'Joined')
	return filename

def visualize(filename):
	plots_folder = os.path.join(os.getcwd(), 'plots')
	download_data.CreateFolder(plots_folder)
	df = pd.read_csv(filename, sep=';')
	statistics.VisualizeData(df, plots_folder)

def Main():
	parser = argparse.ArgumentParser()
	parser.add_argument("zone", type=int, help="UTC zone")
	parser.add_argument("year", type=int, help="year of historical AIS data to analyze")
	parser.add_argument("-m", "--months", type=int, nargs='+',
		help="list of monthes, if not specified will use all 12 months")
	parser.add_argument("-f", "--folder", type=str, help="local data store folder")

	args = parser.parse_args()

	months = []
	if args.months:
		for args_month in args.months:
			month = '0' + str(args_month) if args_month < 10 else str(args_month)
			months.append(month)
	else:
		months = ['0' + str(i) if i < 10 else str(i) for i in range(1, 13)]
	
	folder = os.path.join(os.getcwd(), 'data', 'zone' + str(args.zone))
	if args.folder:
		folder = args.folder
	download_data.CreateFolder(folder)

	filenames = download_and_categorize(args.zone, args.year, months, folder)
	filename = join(filenames)
	visualize(filename)

if __name__ == '__main__':
	Main()