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
col_lst_tst = ['Country', 'Carrier', 'TrafficType', 'Device', 'Browser', 'OS', 'RefererUrl', 'UserIp', 'publisherId', 'advertiserCampaignId', 'Fraud', 'clickdateDay', 'clickdateHour', 'clickdateDayOfWeek', 'ct_ids_hour', 'ct_OS', 'ct_Carrier', 'ct_Country', 'ct_Device', 'ct_Browser', 'ct_RefererUrl', 'ct_publisherId', 'ct_subPublisherId', 'ct_advertiserCampaignId']
countval = 'ID'

random.seed(131)

splitp = 0.8
set_size = 5000000
train_size = 63367217

with ProcessPoolExecutor() as executor:
	image_files = glob.glob("*.jpg")
for image_file, thumbnail_file in zip(image_files, executor.map(make_image_thumbnail, image_files)):

for i in range(int(ceil(train_size/set_size)) + 1):
	if (i != int(ceil(train_size/set_size))):
		train = pd.read_csv("../Data/train.csv",nrows = int(splitp*set_size),skiprows = (i*set_size) + 1,header = None,names = all_cols)
		test = pd.read_csv("../Data/train.csv",nrows = int((1-splitp)*set_size),skiprows = int(splitp*set_size) + (i*set_size) + 1,header = None,names = all_cols)
	else:
		train = pd.read_csv("../Data/train.csv",nrows = int(splitp*(train_size-(set_size*int(ceil(train_size/set_size))))),skiprows = (i*set_size) + 1,header = None,names = all_cols)
		test = pd.read_csv("../Data/train.csv",skiprows = int(splitp*(train_size-(set_size*int(ceil(train_size/set_size))))) + (i*set_size) + 1,header = None,names = all_cols)
	train = fn.data_prep(train,coltype)
	test = fn.data_prep(test,coltype)
	train = train.rename(columns={'ConversionStatus': 'Label'})
	test = test.rename(columns={'ConversionStatus': 'Label'})
	train = train.drop('ID',axis = 1)
	test = test.drop('ID',axis = 1)
	train = train[col_lst_mod]
	test = test[col_lst_tst]
	train.to_csv('../Others/train_' + str(i+1) + '.txt', sep='\t', index=False, header=False)
	test.to_csv('../Others/test_' + str(i+1) + '.txt', sep='\t', index=False, header=False)

# data.dtypes
# data['ConversionStatus'].value_counts()
# False    63332693
# True        34524

# 633672, 17

# rows : 63367217
# cols : 17
# carriers : 383
# PublisherID : 10477
# subPublisherId : 4926
# advertiserCampaignId : 1931
# Refeurl : 283264

# data.isnull().sum(axis=0)/data.shape[0]

# def res_dist(df,colname,target,idval):
# 	cfq = pd.crosstab(index=df[colname],columns='count')
# 	freq_cutoff = np.mean(cfq['count'])
# 	res_cutoff_high = np.mean(df[target]) + np.std(df[target])
# 	res_cutoff_low = np.mean(df[target]) - np.std(df[target])
# 	table = pd.pivot_table(df,values=idval,index=[colname],columns=[target],aggfunc='count',margins=True)
# 	table = table.fillna(0)
# 	table.columns = ['negative','positive','all']
# 	table['positive_response_rate'] = table['positive']/table['all']
# 	table = table[table['all'] > freq_cutoff]
# 	try:
# 		hh_rr_list = table[table['positive_response_rate']>=res_cutoff_high][colname]
# 	except:
# 		hh_rr_list = []
# 	try:
# 		lw_rr_list = table[table['positive_response_rate']<=res_cutoff_low][colname]
# 	except:
# 		lw_rr_list = []
# 	return [hh_rr_list,lw_rr_list]

# freegeoip.net/{format}/{IP_or_hostname}

# df = df[['ID','ClickDate','ConversionStatus','ConversionDate']]
# df[df['ConversionStatus'] == True].head()