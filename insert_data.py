import os
import shutil
import sqlite3
from datetime import datetime 
import csv
from download import get_companies
from make_db import make_query, insert_query
import multiprocessing
import numpy as np
import sys
import config

def insert_prices(syms):
	
	sym = syms[0]
	sym_file = sym.replace('-','.')
	sym = sym.replace('-','')

	f = open('log.txt','a')
	f.write(f'{sym}')

	file = open(f'prices/{sym_file}.csv')
	csvreader = csv.reader(file)
	next(csvreader)
	data = []
	for row in csvreader:
		date,price = row[0],row[4]
		date2 = date
		date = datetime.strptime(date, '%Y-%m-%d').timestamp()
		price = round(float(price),2)
		data.append((date,price))

	stmt = f'''REPLACE INTO {sym}(day, price)
			   VALUES(?,?);
			'''
	try:
		conn = sqlite3.connect(config.DB_NAME)
		c = conn.cursor()
		c.executemany(stmt,data)
		conn.commit()
		conn.close()
		f.write('\n')
	except:
		f.write(f':FAIL \n')
	
	f.close()

def multiprocess_insert():
	comps = get_companies()
	pool = multiprocessing.Pool(processes=10)
	pool.map(insert_prices, comps)



def returns(p1, p2):
	dp = round(float(p2/p1 - 1),4)
	return dp


def one_day(prices, days):
	ret = []
	x = 1
	for p1 in prices[:-1]:
		p2 = prices[x]
		day = days[x]
		dp = returns(p1,p2)
		ret.append((dp,day))
		x+=1
	return ret

def one_week(prices, days):
	ret = []
	x = 7
	for p1 in prices[:-7]:
		p2 = prices[x]
		day = days[x]
		dp = returns(p1,p2)
		ret.append((dp,day))
		x+=1
	return ret

def one_month(prices, days):
	ret = []
	x = 30
	for p1 in prices[:-30]:
		p2 = prices[x]
		day = days[x]
		dp = returns(p1,p2)
		ret.append((dp,day))
		x+=1
	return ret

def three_months(prices, days):
	ret = []
	x = 90
	for p1 in prices[:-90]:
		p2 = prices[x]
		day = days[x]
		dp = returns(p1,p2)
		ret.append((dp,day))
		x+=1
	return ret

def six_months(prices, days):
	ret = []
	x = 180
	for p1 in prices[:-180]:
		p2 = prices[x]
		day = days[x]
		dp = returns(p1,p2)
		ret.append((dp,day))
		x+=1
	return ret

def get_all_returns(prices, days):
	one = one_day(prices,days)
	week = one_week(prices,days)
	month = one_month(prices,days)
	three = three_months(prices,days)
	six = six_months(prices, days)

	return one, week, month, three, six


def insert_returns(syms):
	sym = syms[0]
	sym = sym.replace('-','')

	f = open('log.txt','a')
	f.write(f'{sym}')

	stmt = f'SELECT day, price FROM {sym};'
	pairs = make_query(stmt)

	res = np.array(pairs)
	prices = res[:,1]
	days = res[:,0]

	one, week, month, three, six = get_all_returns(prices,days)

	stmt1 = f'UPDATE {sym} SET one = ? WHERE day = ?;'
	stmt7 = f'UPDATE {sym} SET seven = ? WHERE day = ?;'
	stmt30 = f'UPDATE {sym} SET month = ? WHERE day = ?;'
	stmt90 = f'UPDATE {sym} SET three = ? WHERE day = ?;'
	stmt180 = f'UPDATE {sym} SET six = ? WHERE day = ?;'

	
	try:
		conn = sqlite3.connect(config.DB_NAME)
		c = conn.cursor()
		c.executemany(stmt1, one)
		c.executemany(stmt7, week)
		c.executemany(stmt30, month)
		c.executemany(stmt90, three)
		c.executemany(stmt180, six)
		conn.commit()
		conn.close()
		f.write('\n')
	except:
		f.write(f': FAIL {stmt1}\n')

def multiprocess_returns():
	comps = get_companies()
	pool = multiprocessing.Pool(processes=10)
	pool.map(insert_returns, comps)


def insert_all_data():
	multiprocess_insert()
	multiprocess_returns()



