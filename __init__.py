import os
import sqlite3
import csv
import config
from make_db import make_db, make_all_tables
from insert_data import insert_all_data


if __name__ == '__main__':
	
	make_db()
	print("made database")

	make_all_tables()
	print("made companies tables")

	insert_all_data()
	print("all data in database")






