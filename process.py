import datetime
import time

import boto3

from multiprocessing import Process
import os


processes = []

from botocore.client import Config
from urllib.parse import unquote

os.environ['AWS_PROFILE'] = 'myaccount'

# Retrieve the list of existing buckets
session = boto3.Session(profile_name='myaccount')

path_list = ['mtrt.db/']

p = 0
def get_size(bucket, path):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    total_size = 0

    for obj in my_bucket.objects.filter(Prefix=path):
        total_size = total_size + obj.size
    return total_size

def compute():
    startTime = time.time()
    global p
    x = path_list[p]
    path =  "dbc-25924283-13f6/0/user/hive/warehouse/" + str(x)
    size = get_size("databricks-prod-storage-virginia", path)/(1024 * 1024 * 1024) # in GB
    fpath = "databricks-prod-storage-virginia/" + str(path)
    ctime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    endTime = time.time()
    tt = round((endTime - startTime))
    print (ctime,fpath,size,"GB",tt,"Secs")


xxx = 0

for i in range(len(path_list)):
    xxx = xxx + 1;
    print('registering process', xxx)
    processes.append(Process(target=compute))

for process in processes:
    process.start()
    p = p+1

for process in processes:
	process.join()
