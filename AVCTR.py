
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# In[199]:


from sklearn import preprocessing
le = preprocessing.LabelEncoder()


# In[243]:


train = pd.read_csv('train.csv', nrows=1000000)


# In[278]:


test = pd.read_csv('test.csv', nrows=1000000)


# In[156]:


def add_datepart(df):
    df.ClickDate = pd.to_datetime(df.ClickDate)
    df["clickdateMonth"] = df.ClickDate.dt.month
    df["clickdateWeek"] = df.ClickDate.dt.week
    df["clickdateDay"] = df.ClickDate.dt.day
    df["clickdateHour"] = df.ClickDate.dt.hour
    df["clickdateWeekOfYear"] = df.ClickDate.dt.weekofyear
    df["clickdateDayOfWeek"] = df.ClickDate.dt.weekday


# In[281]:


def missmatch_col(df_train, df_test):
    extra_train_cols = []
    extra_test_cols = []

    for i in df_train.columns:
        if i in df_test.columns:
            continue
        else:
            extra_train_cols.append(i)

    for i in df_test.columns:
        if i in df_train.columns:
            continue
        else:
            extra_test_cols.append(i)
    print("Extra Columns in Train, ","Extra columns in Test")        
    return extra_train_cols,extra_test_cols


# In[255]:


train["target"] = 0


# In[256]:


test['target'] = 1


# In[257]:


total = train.append(test)


# In[241]:


def Datainfo(df):
    print("Printing count of unique values, type and non-Null in all columns\n")
    for col in df.columns:
        print(col+" :",len(df[col].unique()),"    Type:",df[col].dtype, "    Non-NULL % = ",df[col].count()*100/len(df))


# In[258]:


Datainfo(total)


# ConversionDate and subPublisherId should be dropped

# In[259]:


total.drop(["subPublisherId","ConversionDate"], axis=1, inplace=True)


# Any column having NaN is object type

# In[260]:


total.fillna("NAN", axis=1, inplace=True)


# In[261]:


add_datepart(total)


# Drop ClickDate

# In[262]:


total.drop(["ClickDate"], axis=1, inplace=True)


# In[263]:


# Not looping coz some issue with PublisherId, need to check

country_fit = le
total['Country'] = country_fit.fit_transform(total['Country'])
traffic_fit = le
total['TrafficType'] = traffic_fit.fit_transform(total['TrafficType'])
device_fit = le
total['Device'] = device_fit.fit_transform(total['Device'])
browser_fit = le
total['Browser'] = browser_fit.fit_transform(total['Browser'])
os_fit = le
total['OS'] = os_fit.fit_transform(total['OS'])
url_fit = le
total['RefererUrl'] = url_fit.fit_transform(total['RefererUrl'])
ip_fit = le
total['UserIp'] = ip_fit.fit_transform(total['UserIp'])


# In[264]:


total.ConversionStatus = 1*total.ConversionStatus #Typecasting to int()


# In[265]:


total.drop(['publisherId'], axis=1, inplace=True)


# In[268]:


Xtrain = total[total.loc[:,"ConversionPayOut"] != "NAN"]


# In[272]:


Xtest = total[total.loc[:,"ConversionPayOut"] == "NAN"]


# Lets verify using the target variable

# In[274]:


Xtrain['target'].mean(), Xtest['target'].mean()


# Perfect now we have a label encoded complete data and preserved mapping

# In[276]:


del total, train, test


# In[283]:


Ytrain = Xtrain['ConversionPayOut']
Xtrain.drop(['ConversionPayOut','ConversionStatus','target'], axis=1, inplace=True)


# In[284]:


Xtest.drop(['ConversionPayOut','ConversionStatus','target'], axis=1, inplace=True)


# In[285]:


missmatch_col(Xtrain,Xtest)


# Empty -> Perfect match

# In[212]:


from sklearn.decomposition import PCA


# In[213]:


pca = PCA(5)


# In[286]:


pca_f = pd.DataFrame(pca.fit_transform(Xtrain))


# In[287]:


pca_f.columns = ["pca1","pca2","pca3","pca4","pca5"]


# In[288]:


for col in pca_f.columns:
    Xtrain[col] = pca_f[col]


# In[289]:


pca_f = pd.DataFrame(pca.transform(Xtest))


# In[290]:


pca_f.columns = ["pca1","pca2","pca3","pca4","pca5"]


# In[291]:


for col in pca_f.columns:
    Xtest[col] = pca_f[col]


# In[292]:


del pca_f


# In[ ]:





# # Data ready to into any model.. let's start with baseline xgboost

# In[ ]:





# In[ ]:




