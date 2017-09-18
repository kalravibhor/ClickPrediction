import pandas as pd
import numpy as np
import random
import datetime
from datetime import datetime
import functions as fn

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
all_cols = ['ID', 'Country', 'Carrier', 'TrafficType', 'ClickDate', 'Device', 'Browser', 'OS', 
			'RefererUrl', 'UserIp', 'ConversionStatus', 'ConversionDate', 'ConversionPayOut', 
			'publisherId', 'subPublisherId', 'advertiserCampaignId', 'Fraud']
countval = 'ID'

random.seed(131)
data = pd.read_csv("../Data/train.csv")
df = data.sample(frac=0.005)

df['ClickDate'] = pd.to_datetime(df['ClickDate'])
train = df.loc[(df['ClickDate'] <= datetime.date(year=2017,month=8,day=27)) & (df['ClickDate'].notnull()),:]
test = df[(df['ClickDate'] > datetime.date(year=2017,month=8,day=27)) & (df['ClickDate'].notnull())]
ignore = df[df['ClickDate'].isnull()]

train = fn.data_prep(train,coltype)
test = fn.data_prep(test,coltype)

train.columns.tolist()
test.columns.tolist()

train = train.rename(columns={'ConversionStatus': 'Label'})
test = test.rename(columns={'ConversionStatus': 'Label'})

train = train.drop('ID',axis = 1)
test = test.drop('ID',axis = 1)

train[['Country', 'Carrier', 'TrafficType', 'Device', 'Browser', 'OS', 'RefererUrl', 'UserIp',
	'Label', 'publisherId', 'subPublisherId', 'advertiserCampaignId', 'Fraud', 'cldotw',
	'cltod', 'ct_ids_hour', 'ct_OS', 'ct_Carrier', 'ct_Country', 'ct_Device', 'ct_Browser', 'ct_RefererUrl',
	'ct_publisherId', 'ct_subPublisherId', 'ct_advertiserCampaignId']] = train[['Label','Country', 'Carrier', 'TrafficType',
	'Device', 'Browser', 'OS', 'RefererUrl', 'UserIp','publisherId', 'subPublisherId', 'advertiserCampaignId',
	'Fraud', 'cldotw','cltod', 'ct_ids_hour', 'ct_OS', 'ct_Carrier', 'ct_Country', 'ct_Device', 'ct_Browser',
	'ct_RefererUrl','ct_publisherId', 'ct_subPublisherId', 'ct_advertiserCampaignId']]

train.to_csv('../Others/train.txt', sep='\t', index=False, header=False)
test.to_csv('../Others/test.txt', sep='\t', index=False, header=False)

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