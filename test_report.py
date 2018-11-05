import urllib, json
import requests
from datetime import datetime, date, timedelta
import dateutil.parser
import csv
import os
import math
import unicodecsv as csv

from requests.auth import HTTPBasicAuth

# Authorization credentials
this_user = 'your_ait_username'
this_pwd = 'your_ait_pwd'

# This the header row of the CSV.
report_header = ["Collection ID","Collection Name","Crawl ID","Crawl Event","Crawl Started","Crawl Completed","Crawl Size ","Crawl Size (Bytes)","Days Remaining to Save","Username","No. Seeds","Sample Seed","Sample Seed Type","Crawl URL"]

# Determine where to save the CSV. It will save to <working directory>/report_output.
working_dir = os.getcwd()
output_dir_name = "report_output"
output_dir_path = working_dir + "/" + output_dir_name

# Create the save folder if it does not exist.
if not os.path.exists(output_dir_path):
	os.makedirs(output_dir_path)

# Set the path to the output file.
outputfile = output_dir_path + "/test_report_" + str(date.today()) + ".csv"

### Authorize and get json
def authorize_and_get_json(url):
	target = requests.get((url), auth=(this_user,this_pwd))
	return json.loads(target.content)

# Get all collection names and ids and save them to a dict.
nameid_dict = {}
def get_collection_names(target_url):
	global nameid_dict
	
	target_json = authorize_and_get_json(target_url)
	
	for collection in target_json:
		name = (collection['name'])
		id = (collection['id'])
	
		nameid_dict[id] = name
		
nameid_dict = {}
				
# Simple write to csv method. Writes a list object to a csv row. 		
def write_row_to_csv(list_row):
	with open (outputfile, mode='ab') as f:
		writer = csv.writer(f)
		writer.writerow(list_row)
 
# Get the collection name based on the collection id. 
def get_this_collname(working_list):
	for crawl in working_list:
		row_id = crawl[0]
		crawl[1] = (nameid_dict[row_id])
	return working_list

# Convert bytes to readable file size for output.
def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])

## Get a working list of crawls for test crawls with status "LIMBO" (meaning slated for deletion). 
## Returns a list formatted as [collection,"",id,scheduled_crawl_event, start_date_str,end_date_str,readable_size,novel_bytes,days_remaining,"","","",""]
## List should match csv header: report_header = ["Collection ID","Collection Name","Crawl ID","Crawl Event","Crawl Started","Crawl Completed","Crawl Size","Craw Size (Bytes)","Days Remaining to Save","Username","No. Seeds","Sample Seed","Crawl URL"]
## Empty values ("") in list will be inserted later.
crawl_id_list = []
def get_limbo_crawl_ids(target_url):
	
	target_json = authorize_and_get_json(target_url)
	
	for crawl in target_json:
    
		collection = (crawl['collection'])
		id = (crawl['id'])
		novel_bytes = (crawl['novel_bytes'])
		readable_size = convert_size(novel_bytes)
		
		scheduled_crawl_event = (crawl['scheduled_crawl_event'])        
		
		# Get the start date and parse it as a date (no minutes, seconds).
		start_date =  (crawl['start_date'])
		start_date = dateutil.parser.parse(start_date).date()
		start_date_str = start_date.strftime('%Y-%m-%d')
		
		# Get the end date and parse it as a date (no minutes, seconds)
		end_date = (crawl['end_date'])
		end_date = dateutil.parser.parse(end_date).date()
		end_date_str = end_date.strftime('%Y-%m-%d')

		days_remaining = get_days_remaining(end_date)		
	
		# id_event is the working list, which will have additional data inserted later.
		id_event = [collection,"",id,scheduled_crawl_event, start_date_str,end_date_str,readable_size,novel_bytes,days_remaining,"","","","",""]
			
		if id_event not in crawl_id_list:
			crawl_id_list.append(id_event)
	print "crawl_id_list: =============================== "
	print crawl_id_list	  
	return crawl_id_list
 
# Get seed information (number of seeds, a sample seed) from the target URL.
def get_seed_info(crawl_list,base_url):
	crawl_plus_seed_info = []
	# Iterate through the list of crawls
	for crawl in crawl_list:
#		print crawl_list
		# Crawl ID is the third list item.
		crawl_id = crawl[2]
		print crawl_id	
		target_url = base_url + str(crawl_id)
		print target_url
		
		target_json = authorize_and_get_json(target_url)
		
		for crawl_obj in target_json:
	
			seed_list = crawl_obj['json']['crawlDefinition']['oneOffSeeds']
			seed_count = 0
			for seed in seed_list:
				sample_seed = seed['canonicalUrl']
				seed_type = seed['seedType']
				seed_count += 1
			print seed_count
			print sample_seed + " = " + seed_type
		crawl[10] = seed_count
		crawl[11] = sample_seed
		crawl[12] = seed_type
		# Insert seed count and a sample seed into the list.
		crawl_plus_seed_info = crawl_plus_seed_info + [crawl]	 
	return crawl_plus_seed_info

# Get user names associated with a crawl. This is accomplished by checking the changelog, which associates "scheduled crawl events" with users and crawl ids.
def get_user_names(crawl_id_list, base_url):
	crawl_plus_user_list = []
	for crawl in crawl_id_list:
		
		scheduled_crawl_event = crawl[3]
		
		target_url = base_url + str(scheduled_crawl_event)
		target_json = authorize_and_get_json(target_url)
		
		# There should only be one item here, so probably don't need the for loop
		i = 0
		for item in target_json:
			i = i + 1
			username = (item['username'])
		# Insert username in working list
		crawl[9] = username
		crawl_plus_user_list = crawl_plus_user_list + [crawl]
	return crawl_plus_user_list


# Calculate the number of days remaining to save a test crawl (crawl end + 59 days).
def get_days_remaining(crawl_end_date):
	today = date.today()
	test_expire_date = crawl_end_date + timedelta(days=59)
	days_remaining = (test_expire_date - today).days
	return days_remaining

# Determine the crawl job URL for easy access. This is formulaic.
def get_crawl_url(crawl_id_list):
	crawl_base = "https://partner.archive-it.org/591/collections/"
	crawl_plus_url_list = []
	for crawl in crawl_id_list:
		crawl_id = crawl[2]
		collection_id = crawl[0]	
		crawl_url = crawl_base + str(collection_id) + "/crawl/" + str(crawl_id)
		#print crawl_url 
		crawl[13] = crawl_url	
		crawl_plus_url_list = crawl_plus_url_list + [crawl]	
	return crawl_plus_url_list

	
get_collection_names("https://partner.archive-it.org/api/collection") # Create a dict of collection ids and names
limbo_list = get_limbo_crawl_ids("https://partner.archive-it.org/api/crawl_job?test=true&test_crawl_state=LIMBO") # Get crawl info for "LIMBO" crawls 
limbo_list = get_limbo_crawl_ids("https://partner.archive-it.org/api/crawl_job?test=true&test_crawl_state=LIMBO_7_DAY_NOTICE")
limbo_list = get_user_names(limbo_list,"https://partner.archive-it.org/api/changelog?row_id=")
limbo_list = get_this_collname(limbo_list) # Grab the collection name and insert them into the limbo list
limbo_list = get_seed_info(limbo_list,"https://partner.archive-it.org/api/crawl_info_json?crawl_job=") # Insert seed info into the list
limbo_list = get_crawl_url(limbo_list) # Derive the URL for the crawl
 
#print limbo_list

write_row_to_csv(report_header)
for item in limbo_list:
	try:
		write_row_to_csv(item)
	except:
		print "Unable to write this row to CSV:      "  + str(item)
