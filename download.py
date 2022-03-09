from urllib.request import urlretrieve
import urllib 
import threading
import time
from make_db import make_query
import multiprocessing
import config


#URL for specific price data is made
def make_url(sym):
	start = 1488844800
	end = int(time.time())
	sym = sym.replace('.','-')
	url = f'''https://query1.finance.yahoo.com/v7/finance/download/{sym}?period1={start}&period2={end}&interval=1d&events=history'''
	return str(url)


#A csv file is downloaded to a corresponding directory
def download_csv(url,sym):
	path = f'prices/{sym}.csv'
	urlretrieve(url, path)
	
def download_csv_mp(urls):
	
	sym = urls[1]
	url = urls[0]
	path = f'prices/{sym}.csv'
	try:
		urlretrieve(url, path)
	except:
		print(f'{sym}: FAIL: {url}')

#All S&P 500 companies and sectors are retrieved 
def get_companies():
	stmt = "SELECT symbol FROM companies"
	res = make_query(stmt)
	return res

#Given a list of companies, a csv is retrieved from a specific url
def download_all(comps):
	fails = 1
	for comp in comps:
		sym = comp[0]
		url = make_url(sym)
		
		try:
			download_csv(url, sym)
			print(f'{sym}: PASS')
		except:
			print(f'{sym}: FAIL #{fails} : {url}')
			fails += 1
		time.sleep(.5)


# S&P 500 data is downloaded from past 5 years
def multithread_download_snp():

	comps = get_companies()
	urls = []
	for comp in comps:
		url = make_url(comp[0])
		urls.append([url,comp[0]])

	pool = multiprocessing.Pool(processes=10)
	pool.map(download_csv_mp, urls)

#RUN DOWNLOAD BELOW

# if __name__ == '__main__':

# 	multithread_download_snp()
	