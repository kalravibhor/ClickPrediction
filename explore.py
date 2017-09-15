import pandas as pd
import numpy as np
import random
from datetime import datetime

data = pd.read_csv("../Data/train.csv")
data.dtypes

random.seed(131)
df = data.sample(frac=0.01)

633672, 17

# rows : 63367217
# cols : 17
# carriers : 383
# PublisherID : 10476
# subPublisherId : 4926
# advertiserCampaignId : 1931

data.isnull().sum(axis=0)/data.shape[0]

np.unique(data['Country'])

def res_dist(df,colname,target,idval):
	cfq = pd.crosstab(index=df[colname],columns='count')
	freq_cutoff = np.mean(cfq['count'])
	res_cutoff_high = np.mean(df[target]) + np.std(df[target])
	res_cutoff_low = np.mean(df[target]) - np.std(df[target])
	table = pd.pivot_table(df,values=idval,index=[colname],columns=[target],aggfunc='count',margins=True)
	table = table.fillna(0)
	table.columns = ['negative','positive','all']
	table['positive_response_rate'] = table['positive']/table['all']
	table = table[table['all'] > freq_cutoff]
	try:
		hh_rr_list = table[table['positive_response_rate']>=res_cutoff_high][colname]
	except:
		hh_rr_list = []
	try:
		lw_rr_list = table[table['positive_response_rate']<=res_cutoff_low][colname]
	except:
		lw_rr_list = []
	return [hh_rr_list,lw_rr_list]

freegeoip.net/{format}/{IP_or_hostname}

df = df[['ID','ClickDate','ConversionStatus','ConversionDate']]
df[df['ConversionStatus'] == True].head()