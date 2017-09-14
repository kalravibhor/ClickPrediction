import numpy as np
import pandas as pd
import datetime
import xgboost as xgb

def one_hot(df,cols):
	for each in cols:
		dummies = pd.get_dummies(df[each], prefix=each, drop_first=True)
		df = pd.concat([df, dummies], axis=1)
	return df

def univariate(df, uvt_cols):
	for col in uvt_cols:
		table = pd.pivot_table(df,values=countval,index=[col],aggfunc='count')
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
		if coltype[colname] == str:
			df[colname] = df[colname].fillna('')
		elif coltype[colname] in (int,float):
			df[colname] = df[colname].fillna(0)
	return df

# Data pre-processing
def data_prep(df):
	df = impute(df,df.columns.tolist())

	# Datetime variable creation
	df['ClickDate'] =  pd.to_datetime(df['ClickDate'])
	df['cldotw'] = df['ClickDate'].dt.dayofweek
	df['cltod'] = df['ClickDate'].dt.time
	df['cltod'] = df['cltod'].astype('str')
	df['cltod'] = df['cltod'].apply(lambda x: (int(x.split(':')[0])*3600) + (int(x.split(':')[1])*60) + (int(x.split(':')[2])))

	df['ConversionDate'] =  pd.to_datetime(df['ConversionDate'])
	df['codotw'] = df['ConversionDate'].dt.dayofweek
	df['cotod'] = df['ConversionDate'].dt.time
	df['cotod'] = df['cotod'].astype('str')
	df['cotod'] = df['cotod'].apply(lambda x: (int(x.split(':')[0])*3600) + (int(x.split(':')[1])*60) + (int(x.split(':')[2])))

	df = one_hot(df,['TrafficType','OS','Fraud','ConversionStatus'])
	df = univariate(df,['Country','Device','Browser','OS','RefererUrl','publisherId','subPublisherId','advertiserCampaignId'])
	df = bivariate(df,['','','',''])
	return df