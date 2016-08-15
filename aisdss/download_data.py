from string import Template
import requests
import zipfile
import os


def CreateFolder(folder):
	folders = folder.split(os.sep)
	subfolder = ''
	for folder in folders:
		subfolder += folder + os.sep
		if not os.path.exists(subfolder):
			os.mkdir(subfolder)

def download_file(url, folder):
	local_filename = url.split('/')[-1].split('.')[0] + '.gdb.zip'
	local_path = os.path.join(folder, local_filename)
	if os.path.exists(local_path):
		return local_path
	# NOTE the stream=True parameter
	r = requests.get(url, stream=True)
	with open(local_path, 'wb') as f:
		for chunk in r.iter_content(chunk_size=1024):
			if chunk: # filter out keep-alive new chunks
				f.write(chunk)
	return local_path

def download_vesel_traffic_file(year, month, zone):
	temp_url = Template('https://coast.noaa.gov/htdata/CMSP/AISDataHandler/' + \
				'${year}/${month}/Zone${zone}_${year}_${month}.zip')
	url = temp_url.substitute(year=year, month=month, zone=zone)
	# dir = 'data\\' + year + '\\' + month
	dir = 'data/zone' + zone
	folders = dir.split('/')
	subfolder = ''
	for folder in folders:
		subfolder += folder + '/'
		if not os.path.exists(subfolder):
			os.mkdir(subfolder)

	path = download_file(url, dir)
	print (path)
	return path

def download_vesel_traffic_data():
	years = [str(i) for i in range(2014, 2015)]
	months = ['0' + str(i) if i < 10 else str(i) for i in range(1, 13)]
	# zones = [str(i) for i in range(1, 21) if (i != 12 and i != 13)]
	zones = ['17']
	for year in years:
		for month in months:
			for zone in zones:
				path = download_vesel_traffic_file(year, month, zone)
				if path:
					with zipfile.ZipFile(path) as myzip:
						myzip.extractall(path[:path.rfind('/')])
					os.remove(path)

def main():
	download_vesel_traffic_data()

if __name__ == '__main__':
	main()