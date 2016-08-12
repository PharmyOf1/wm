import os, csv


def chunker(csv_reader,lines_list):
	lines_set = set(lines_list)
	for line_number, row in enumerate(csv_reader):
		if line_number in lines_list:
			yield row
			lines_set.remove(line_number)
			if not lines_set:
				raise StopIteration

def create_multiple_files(files, path):
	max_size = 1000000
	row_chunks = 100000
	size_dict = {y:os.path.getsize(os.path.join(path_with_files,y)) for y in files}
	large_files = [names for names,size in size_dict.items() if size>max_size]

	for file in large_files:
		with open(os.path.join(path,file),'r') as f:
			for x,y in enumerate(f):
				tot_rows = (x)
		num_files = [chunk_start for chunk_start in range(tot_rows) if chunk_start%row_chunks==0]
		
		if len(num_files)>0:
			with open(os.path.join(path,file),'r') as f:
				reader = csv.reader(f,delimiter='|')
				next(reader) #header
				idx = 0
				for _ in num_files:
					chunk_range = list(range(num_files[idx],num_files[idx+1]))
					for data in chunker(reader,chunk_range):
						pass
					idx+=1


if __name__ == '__main__':
	this_path = os.path.dirname(os.path.realpath(__file__))
	path_with_files = os.path.join(this_path, 'attachments')
	print (path_with_files)

	for x in os.walk(path_with_files):
		root, dirs, files = x

	file_sizes = create_multiple_files(files, path_with_files)

