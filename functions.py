import numpy as np
import pandas as pd
import datetime
from sklearn.preprocessing import MinMaxScaler

def one_hot(df,cols):
	for each in cols:
		dummies = pd.get_dummies(df[each], prefix=each, drop_first=True)
		df = pd.concat([df, dummies], axis=1)
	return df

def univariate(df,uvt_cols):
	for col in uvt_cols:
		table = pd.DataFrame(df[col].value_counts()).reset_index(drop=False)
		table.columns = [col, 'ct_' + col]
		df = df.merge(table,how='left',on=col)
	return df

def bivariate(df,bivt_cols):
	for col1 in bivt_cols:
		for col2 in bivt_cols:
			if ((col1 != col2) & (col1 + '_' + col2 not in df.columns) & (col2 + '_' + col1 not in df.columns)):
				table = pd.pivot_table(df,values=countval,index=[col1,col2],aggfunc='count')
				table = table.reset_index()
				table.columns = [col1,col2,col1 + '_' + col2]
				df = df.merge(table,how='left')
	return df

def impute(df,colnames,coltype):
	for colname in colnames:
		if (coltype[colname] == str):
			df[colname] = df[colname].fillna('')
		elif (coltype[colname] in (int,float)):
			df[colname] = df[colname].fillna(0)
	return df

def data_prep(df,coltype):
	scaler = MinMaxScaler()
	keep_list = ['ID', 'Country', 'Carrier', 'TrafficType', 'ClickDate', 'Device', 'Browser', 'OS', 'RefererUrl', 'UserIp', 'ConversionStatus', 'publisherId', 'subPublisherId','advertiserCampaignId', 'Fraud']
	df = df[keep_list]
	df = impute(df,df.columns.tolist(),coltype)
	df['ClickDate'] =  pd.to_datetime(df['ClickDate'])
	df['clickdateDate'] = df['ClickDate'].dt.date
	df["clickdateDay"] = df.ClickDate.dt.day
	df["clickdateHour"] = df.ClickDate.dt.hour
	df["clickdateDayOfWeek"] = df.ClickDate.dt.weekday
	table = df[['clickdateDate','clickdateHour','ID']].drop_duplicates(subset=['clickdateDate','clickdateHour','ID'])
	table = table.pivot_table(index = ['clickdateDate','clickdateHour'],values = 'ID',aggfunc='count')
	table = table.reset_index()
	table.columns = ['clickdateDate','clickdateHour','ct_ids_hour']
	df = df.merge(table,how='left',on=['clickdateDate','clickdateHour'])
	df['ConversionStatus'] = df['ConversionStatus']*1
	df['Fraud'] = df['Fraud'].astype(int)
	df['Carrier'] = df['Carrier'].astype(int)
	df = univariate(df,['OS','Carrier', 'Country','Device','Browser','RefererUrl','publisherId','subPublisherId','advertiserCampaignId'])
	drop_list = ['ClickDate','clickdateDate','subPublisherId']
	norm_list = ['clickdateDay', 'clickdateHour', 'clickdateDayOfWeek', 'ct_ids_hour', 'ct_OS', 'ct_Carrier', 'ct_Country', 'ct_Device', 'ct_Browser', 'ct_RefererUrl', 'ct_publisherId', 'ct_subPublisherId', 'ct_advertiserCampaignId']
	df = df.drop(drop_list,axis=1)
	df[norm_list] = scaler.fit_transform(df[norm_list])
	return df

def data_prep_pos(df):
	scaler = MinMaxScaler()
	keep_list = ['ID', 'Country', 'Carrier', 'TrafficType', 'ClickDate', 'Device', 'Browser', 'OS', 'RefererUrl', 'UserIp', 'ConversionPayOut', 'ConversionDate', 'publisherId', 'subPublisherId', 'advertiserCampaignId', 'Fraud']
	df = df.loc[df['ConversionStatus']==True, keep_list]
	df = impute(df,df.columns.tolist())
	df['ClickDate'] =  pd.to_datetime(df['ClickDate'])
	df['clickdateDate'] = df['ClickDate'].dt.date
	df["clickdateDay"] = df.ClickDate.dt.day
	df["clickdateHour"] = df.ClickDate.dt.hour
	df["clickdateDayOfWeek"] = df.ClickDate.dt.weekday
	df['ConversionDate'] =  pd.to_datetime(df['ConversionDate'])
	df['conversiondateDate'] = df['ConversionDate'].dt.date
	df["conversiondateDay"] = df.ConversionDate.dt.day
	df["conversiondateHour"] = df.ConversionDate.dt.hour
	df["conversiondateDayofWeek"] = df.ConversionDate.dt.weekday
	table = df[['clickdateDate','clickdateHour','ID']].drop_duplicates(subset=['clickdateDate','clickdateHour','ID'])
	table = table.pivot_table(index = ['clickdateDate','clickdateHour'],values = 'ID',aggfunc='count')
	table = table.reset_index()
	table.columns = ['clickdateDate','clickdateHour','ct_ids_hour']
	df = df.merge(table,how='left',on=['clickdateDate','clickdateHour'])
	
	df = df.merge(table,how='left',on=['cldate','clickhour'])
	df['ConversionStatus'] = df['ConversionStatus']*1
	df['Fraud'] = df['Fraud'].astype(int)
	df['Carrier'] = df['Carrier'].astype(int)
	df = one_hot(df,['TrafficType'])
	df = univariate(df,['OS','Carrier', 'Country','Device','Browser','RefererUrl','publisherId',
						'subPublisherId','advertiserCampaignId'])
	drop_list = ['ClickDate','temp','clickhour','cldate','temp2']
	norm_list = ['cldotw', 'cltod', 'codotw', 'cotod', 'daysclco', 'ct_ids_hour', 'ct_OS',
				 'ct_Carrier', 'ct_Country', 'ct_Device', 'ct_Browser', 'ct_RefererUrl', 'ct_publisherId',
				 'ct_subPublisherId', 'ct_advertiserCampaignId']
	df = df.drop(drop_list,axis=1)
	df[norm_list] = scaler.fit_transform(df[norm_list])
	return df