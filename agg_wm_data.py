# =-=-=-=-=-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# Author: Phil Harm
# Email: phillip.harm@mdlz.com
# Title: WM Data Automation - OSA
# =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

import os
import openpyxl, csv
import sys, imaplib, email, io, pymysql
import requests, zipfile, re

from sqlalchemy import create_engine, Table, MetaData, schema
from sqlalchemy.sql import table, column, select, update, insert

import tweepy, random

import tinify_files, creds, time

login_info = creds.login_info

class File_Data(object):
	def __init__(self,file_name):
		self.file_name = file_name
		fn, fx = os.path.splitext(self.file_name)
		self.fx = fx

	def create_rows(self):
		if self.fx in ('.csv'):
			csv.register_dialect('pipes', delimiter=',')
			with open(self.file_name,'r') as f:
				reader = csv.reader(f, dialect='pipes')
				rows = list(reader)
			return rows
		else:
			self.loaded_file = openpyxl.load_workbook(filename=self.file_name, read_only=True)
			rows = []
			for row in self.loaded_file.worksheets[0].iter_rows():
				record = [cell.value for cell in row]
				rows.append(record)
			return rows

class Connect_To_Server(object):
    def __init__(self,login_info):
        self.login_info = login_info
        self.engine = create_engine(login_info) 
        self.connection = self.engine.connect()
        self.meta = MetaData()
        self.meta.reflect(bind=self.engine)
        print ('***SERVER Connection Established***\n')

    def upload_record(self, record_type, table_data):
    	self.record_type = record_type
    	print ('\n----------Uploading {}----------\n'.format(self.record_type))
    	correct_table, table_data = self.table_selector(table_data)
    	#bulk insert - must be same structure everytime
    	self.connection.execute(correct_table.insert(), table_data)
    	FM.add_to_loaded_files(ROQ.file_name)
    	print ('\n~~Record Updated~~\n')


    def table_selector(self,table_data):
    	if 'CAO' in self.record_type:
    		dbTable = 'WM_OSA_CAO_PILOT'
    		table_data=table_data[4:]
    		table_data = [{
    			  'Prime Item Nbr':x[0],
    			  'Prime Item Desc':x[1],
    			  'UPC':x[2],
    			  'UPC Desc':x[3],
    			  'Store Nbr':x[4],
    			  'Store Name':x[5],
    			  'WM Week':x[6],
    			  'POS Qty':x[7],
    			  'POS Sales':x[8],
    			  'POS Qty Avg':x[9],
    			  'Lost Sales Qty':x[10],
    			  'Lost Sales':x[11]
    									} for x in table_data] 
    	else:
    		dbTable = 'WM_TOP_ITEMS_WITH_OSA'
    		table_data=table_data[4:]
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
    									} for x in table_data] 
    	
    	correct_table = schema.Table(dbTable, self.meta, autoload=True)
    	return correct_table, table_data


    def close(self): 
        self.connection.close()
        print ('***SERVER Connection Terminated***\n')

class Connect_To_Email(object):
	def __init__(self):
		self.username = creds.username
		self.password = creds.password
		self.M = imaplib.IMAP4_SSL('imap.gmail.com')
		self.M.login(self.username, self.password)
		status, msgs = self.M.select('INBOX')
		status, messages = self.M.search(None, "UnSeen")
		self.inbox_count = messages[0].split()

		print ('***GMAIL Connection Created***')

	def pull_files(self):
		#Pulling Email count from connection
		for message in self.inbox_count:
			status, messageParts = self.M.fetch(message,'(RFC822)')
			self.decoded_message = (email.message_from_string(messageParts[0][1].decode('UTF-8')))
			subject = self.decoded_message['SUBJECT']
			print ('\nMessage {}: {}'.format(message.decode('UTF-8'),subject))

			zip_subjects = ['Newest Week OSA with PPG and SALES HEIRARCHY']

			if subject in zip_subjects:
				self.get_zip()
			elif any(['waste' in subject,'Waste' in subject]):
				pass

			else:
				self.get_attachment()

	def get_zip(self):
		links = re.findall(r'href=3D[\'"]?([^\'" >]+)', str(self.decoded_message))
		correct_link = [x for x in links if 'http://atlas.atlasdsr.com/Do' in x][0]	
		if ('\n' in correct_link):
			a = str(correct_link).replace('=','')
			zip_link = str(a).replace('\n','')

		try:
		 	r = requests.get(zip_link)
		 	z = zipfile.ZipFile(io.BytesIO(r.content))
		 	write_path = (os.path.join(detach_dir, (z.namelist()[0])))

		 	if not os.path.isfile(write_path):
		 		z.extractall(path=detach_dir)
		 		print ('Zip Attachment extracted')
		 	else:
		 		print ('File already unzipped')
		except:
		 	print ('No zip attachment')

	def get_attachment(self):
		for part in self.decoded_message.walk():
			if part.get_content_maintype() == 'multipart':
				continue
			if part.get('Content-Disposition') is None:
				continue

			#If there's an attachment, we'll grab it here
			attach = part.get_filename()
			if ('\r\n' in attach):
				attach = str(attach).replace('\r\n',' ')

			print ('Attachment: {}'.format(attach))

			if bool(attach):
				write_path = (os.path.join(detach_dir, attach))
				if not os.path.isfile(write_path):
					with open(write_path,'wb') as f:
						final_attach = (part.get_payload(decode=True))
						f.write(final_attach)
					print ('SAVED TO SERVER')
				else:
					print ('File Already Pulled')

	def end_connection(self):
		self.M.close()
		self.M.logout()
		print ('\n***GMAIL Connection Terminated***')

class NameManagement(object):
	def __init__(self):
		self.exclusion_list = ['Waste','waste']

	def only_new_files(self):
		for x in os.walk(path_with_files):
			root, dirs, files = x
		
		#Break up files
		tinify_files.create_multiple_files(files,path_with_files)

		for x in os.walk(path_with_files):
			root, dirs, files = x

		with open(os.path.join(detach_dir,'loaded_files.txt')) as f:
			already_uploaded = set([x.strip('\n') for x in f.readlines()])
			files_in_dir = set(files)
			for f in files_in_dir:
				if f in already_uploaded:
					os.remove(os.path.join(root,f))

			self.files_to_upload = (files_in_dir - already_uploaded)
			full_paths = [os.path.join(root,file) for file in self.files_to_upload if file.endswith('.xlsx') or file.endswith('.csv')]

			#Remove using keyword exclusions
			full_paths = [x for x in full_paths if not any(y in x for y in self.exclusion_list)]

			return full_paths

	def add_to_loaded_files(self,file_name):
		head,tail = os.path.split(file_name)
		with open(os.path.join(detach_dir,'loaded_files.txt'), "a") as f:
			f.write('\n')
			f.write(tail)

class Twitter(object):
	def __init__(self):
		CONSUMER_KEY = creds.CONSUMER_KEY
		CONSUMER_SECRET = creds.CONSUMER_SECRET
		ACCESS_KEY = creds.ACCESS_KEY
		ACCESS_SECRET = creds.ACCESS_SECRET
		auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
		auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
		self.api = tweepy.API(auth)

if __name__ == "__main__":
	tic = time.clock()
#=-=-=-=-=-=Set Constants-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	this_path = os.path.dirname(os.path.realpath(__file__))
	path_with_files = os.path.join(this_path, 'attachments')
	detach_dir = os.path.join(this_path, 'attachments')
#=-=-=-=-=-=Construct Connection Instances-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
	GMAIL = Connect_To_Email()
	OSA_Connect = Connect_To_Server(login_info)
	tweet = Twitter()
# #=-=-=-=-=-=Stream Files-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
	GMAIL.pull_files()
#-=-=-=-=-=Loop Files For Upload-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
	FM = NameManagement()
	new_files = FM.only_new_files()
	for data_file in new_files:
		ROQ = File_Data(data_file)
		roq_data = ROQ.create_rows()
		record_type = ROQ.file_name
		OSA_Connect.upload_record(str(data_file),roq_data)
		try:
			os.remove(data_file)
		except:
			print ('Failed to remove file, pls remove manually')
#=-=-=-=-=-=End Connections-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
#=-=-=-=-=-=Tweet Out Success-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
	toc = time.clock()
	try:
		tweet.api.update_status('Process Ran: {} New Files. Duration: {}'.format(len(new_files),toc-tic))
		GMAIL.end_connection()
		OSA_Connect.close()

	except:
		tweet.api.update_status('Error in process: {}'.format(random.randint(0,100)))


