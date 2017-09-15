import numpy as np
import pandas as pd
import datetime
import xgboost as xgb

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

def one_hot(df,cols):
	for each in cols:
		dummies = pd.get_dummies(df[each], prefix=each, drop_first=True)
		df = pd.concat([df, dummies], axis=1)
	return df

def univariate(df, uvt_cols):
	for col in uvt_cols:
		table = pd.pivot_table(df,values=countval,index=[col],aggfunc='count')
		table = table.reset_index()
		table.columns = [col, 'ct_' + col]
		df = df.merge(table,how='left')

def bivariate(df, bivt_cols, countval):
	for col1 in bivt_cols:
		for col2 in bivt_cols:
			if ((col1 != col2) & (col1 + '_' + col2 not in df.columns) & (col2 + '_' + col1 not in df.columns)):
				table = pd.pivot_table(df,values=countval,index=[col1,col2],aggfunc='count')
				table = table.reset_index()
				table.columns = [col1,col2,col1 + '_' + col2]
				df = df.merge(table,how='left')
	return df

def impute(df,colnames):
	for colname in colnames:
		if (coltype[colname] == str):
			df[colname] = df[colname].fillna('')
		elif (coltype[colname] in (int,float)):
			df[colname] = df[colname].fillna(0)
	return df

def data_prep(df):
	df = impute(df,df.columns.tolist())

	# Datetime variable creation
	df['ClickDate'] = df['ClickDate'].astype(object)
	df['ConversionDate'] = df['ConversionDate'].astype(object)

	df['ConversionDate'] = df['ConversionDate'].apply(lambda x: x if not pd.isnull(x) else '')
	df['ClickDate'] = df['ClickDate'].apply(lambda x: x if not pd.isnull(x) else '')

	df['ClickDate'] =  pd.to_datetime(df['ClickDate'])
	df['ConversionDate'] =  pd.to_datetime(df['ConversionDate'])

	df['cldotw'] = df['ClickDate'].dt.dayofweek
	df['temp'] = df['ClickDate'].dt.time
	df['temp'] = df['temp'].astype('str')

	df['codotw'] = df['ConversionDate'].dt.dayofweek
	df['temp2'] = df['ConversionDate'].dt.time
	df['temp2'] = df['temp2'].astype('str')

	df.loc[df['ClickDate'].notnull(),'cltod'] = df.loc[df['ClickDate'].notnull(),'temp'].apply(lambda x: (int(x.split(':')[0])*3600) + (int(x.split(':')[1])*60) + (int(x.split(':')[2])))
	df.loc[df['ConversionDate'].notnull(),'cotod'] = df.loc[df['ConversionDate'].notnull(),'temp2'].apply(lambda x: (int(x.split(':')[0])*3600) + (int(x.split(':')[1])*60) + (int(x.split(':')[2])))

	df.loc[df['ConversionDate'].notnull(),'daysclco'] = df.loc[df['ConversionDate'].notnull(),'ConversionDate'].dt.dayofyear - df.loc[df['ConversionDate'].notnull(),'ClickDate'].dt.dayofyear

	df = one_hot(df,['TrafficType','OS','Fraud','ConversionStatus'])
	df = univariate(df,['Country','Device','Browser','OS','RefererUrl','publisherId','subPublisherId','advertiserCampaignId'])

	df.drop(['ClickDate','ConversionDate','TrafficType','OS','Fraud','ConversionStatus','Country','Device',
			'Browser','temp','temp2', 'OS','RefererUrl','publisherId','subPublisherId','advertiserCampaignId'],axis=1)
	return df