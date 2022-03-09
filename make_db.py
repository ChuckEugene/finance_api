import os
import shutil
import sqlite3
from datetime import datetime 
import csv
import config


def make_db():
	conn = sqlite3.connect(config.DB_NAME)
	c = conn.cursor()
	c.execute('''DROP TABLE IF EXISTS companies''')
	conn.commit()
	c.execute('''
			 CREATE TABLE IF NOT EXISTS companies
			 (symbol TEXT NOT NULL,
			  name TEXT NOT NULL,
			  PRIMARY KEY(symbol))
		''')
	conn.commit()
	conn.close()


def insert_company(company):
	sym = company[0]
	sym = sym.replace('.','-')
	conn = sqlite3.connect(config.DB_NAME)
	c = conn.cursor()
	conn.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')

	c.execute('''
				 INSERT INTO companies(symbol, name)
				 VALUES(?, ?)
			  ''', (sym,company[1]))
	conn.commit()
	conn.close()

def make_company_table(sym):
	sym = sym.replace('.','')
	conn = sqlite3.connect(config.DB_NAME)
	c = conn.cursor()
	stmt = f'''
			CREATE TABLE IF NOT EXISTS {sym}
			(day REAL NOT NULL,
			 price REAL NOT NULL,
			 one REAL,
			 seven REAL,
			 month REAL,
			 three REAL,
			 six REAL,
			 PRIMARY KEY(day))
		  '''
	c.execute(stmt)
	conn.commit()
	conn.close()


def make_all_tables():
	file = open('lists/comps.csv')
	csvreader = csv.reader(file)
	next(csvreader)
	for row in csvreader:
		insert_company(row)
		try:
			make_company_table(row[0])
		except:
			print(f'{row[0]}: FAIL')

def make_query(stmt):
	conn = sqlite3.connect(config.DB_NAME)
	c = conn.cursor()
	c.execute(stmt)
	res = c.fetchall()
	conn.close()
	return res

def insert_query(stmt):
	conn = sqlite3.connect(config.DB_NAME)
	c = conn.cursor()
	c.execute(stmt)
	conn.commit()
	conn.close()







