# Prepping and dividing the data into smaller chunks of 4M train and 1M test

import pandas as pd
import numpy as np
import random
import datetime
import functions as fn
from math import ceil
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime

coltype = 	{
			'ID' : int, 
			'Country' : str, 
			'Carrier' : int, 
			'TrafficType' : str, 
			'ClickDate' : np.datetime64, 
			'Device' : str,
			'Browser' : str, 
			'OS' : str, 
			'RefererUrl' : str, 
			'UserIp' : str, 
			'ConversionStatus' : bool,
			'ConversionDate' : np.datetime64, 
			'ConversionPayOut' : float, 
			'publisherId' : int,
			'subPublisherId' : int, 
			'advertiserCampaignId' : int,
			'Fraud' : int
			}
all_cols = ['ID', 'Country', 'Carrier', 'TrafficType', 'ClickDate', 'Device', 'Browser', 'OS', 'RefererUrl', 'UserIp', 'ConversionStatus', 'ConversionDate', 'ConversionPayOut', 'publisherId', 'subPublisherId', 'advertiserCampaignId', 'Fraud']
col_lst_mod = ['Label', 'Country', 'Carrier', 'TrafficType', 'Device', 'Browser', 'OS', 'RefererUrl', 'UserIp', 'publisherId', 'advertiserCampaignId', 'Fraud', 'clickdateDay', 'clickdateHour', 'clickdateDayOfWeek', 'ct_ids_hour', 'ct_OS', 'ct_Carrier', 'ct_Country', 'ct_Device', 'ct_Browser', 'ct_RefererUrl', 'ct_publisherId', 'ct_subPublisherId', 'ct_advertiserCampaignId']
countval = 'ID'

random.seed(131)

splitp = 0.8
set_size = 5000000
train_size = 63367217
part_list = range(4)
part_list = range(int(ceil(train_size/int(splitp*set_size))) + 1)

def data_part(i):
	if (i != int(ceil(train_size/int(splitp*set_size)))):
		train = pd.read_csv("../Data/train.csv",nrows = int(splitp*set_size),skiprows = int(i*splitp*set_size) + 1,header = None,names = all_cols)
		test = pd.read_csv("../Data/train.csv",nrows = int((1-splitp)*set_size),skiprows = int(splitp*set_size) + int(i*set_size*splitp) + 1,header = None,names = all_cols)
	else:
		train = pd.read_csv("../Data/train.csv",nrows = int(splitp*(train_size-(set_size*i*splitp))),skiprows = int(i*splitp*set_size) + 1,header = None,names = all_cols)
		test = pd.read_csv("../Data/train.csv",skiprows = int(splitp*(train_size-(set_size*i*splitp))) + (i*set_size) + 1,header = None,names = all_cols)
	train = fn.data_prep(train,coltype)
	test = fn.data_prep(test,coltype)
	train = train.rename(columns={'ConversionStatus': 'Label'})
	test = test.rename(columns={'ConversionStatus': 'Label'})
	train = train.drop('ID',axis = 1)
	test = test.drop('ID',axis = 1)
	train = train[col_lst_mod]
	test = test[col_lst_mod]
	train.to_csv('../Others/train_' + str(i+1) + '.txt', sep='\t', index=False, header=False)
	test.to_csv('../Others/test_' + str(i+1) + '.txt', sep='\t', index=False, header=False)
	return i

with ProcessPoolExecutor() as executor:
	for i, itrno in zip(part_list, executor.map(data_part, part_list)):
		print str(itrno) + " completed."
