import sys, csv, itertools, time
from creds import login_info

from sqlalchemy import create_engine, Table, MetaData, schema
from sqlalchemy.sql import table, column, select, update, insert

fPath = sys.argv[1]

login_info = login_info
engine = create_engine(login_info) 
connection = engine.connect()
meta = MetaData()
meta.reflect(bind=engine)
dbTable = schema.Table('WM_TOP_ITEMS_WITH_OSA', meta, autoload=True)
print ('***SERVER Connection Established***\n')

def chunk_gen(iterable, size=5000):
    iterator = iter(iterable)
    for first in iterator:
        chunk = list(itertools.chain([first], itertools.islice(iterator, size - 1)))
        for x in chunk:
        	table_data = [{
    			  'Prime Item Nbr':x[0],
    			  'Prime Item Desc':x[1],
    			  'UPC':x[2],
    			  'ERP LV5-Kraft Sub Segment':x[3],
    			  'WM Week':x[4],
    			  'AVP':x[5],
    			  'RVP':x[6],
    			  'RDR':x[7],
    			  'DM':x[8],
    			  'Store Nbr':x[9],
    			  'POS Qty':x[10],
    			  'POS Sales':x[11],
    			  'POS Qty Avg':x[12],
    			  'Lost Sales Qty':x[13],
    			  'Lost Sales':x[14]
    									} for x in chunk]
        yield table_data 

with open(fPath) as f:
	i = 1
	reader = csv.reader(f,delimiter = "|")
	next(reader, None) #Skip header
	for chunk in chunk_gen(reader):
		toc = time.clock()
		connection.execute(dbTable.insert(),chunk)
		tic = time.clock()
		print ('Chunk {} Inserted in {} seconds'.format(i,tic-toc))
		i=i+1

connection.close()
print ('***SERVER Connection Terminated***\n')