# Modelling using TinrTGU model

import pickle
from pandas import read_csv
from datetime import datetime
from csv import DictReader
from math import exp, log, sqrt
from pymmh3 import hash
from sklearn.metrics import mean_squared_error
from concurrent.futures import ProcessPoolExecutor

# Bounded logloss function
def logloss(p, y):
	p = max(min(p, 1. - 10e-17), 10e-17)        # The bounds
	return -log(p) if y == 1. else -log(1. - p)

# Hashing to generate final features
def get_x(csv_row, D):
	fullind = []
	for key, value in csv_row.items():
		s = key + '=' + value
		fullind.append(hash(s) % D) # weakest hash ever ?? Not anymore :P
	if interaction == True:
		indlist2 = []
		for i in range(len(fullind)):
			for j in range(i+1,len(fullind)):
				indlist2.append(fullind[i] ^ fullind[j]) # Creating interactions using XOR
		fullind = fullind + indlist2
	x = {}
	x[0] = 1  # 0 is the index of the bias term
	for index in fullind:
		if(not x.has_key(index)):
			x[index] = 0
		if signed:
			x[index] += (1 if (hash(str(index))%2)==1 else -1) # Disable for speed
		else:
			x[index] += 1
	return x  # x contains indices of features that have a value as number of occurences

# Predicting using the given weights. Probability of p(y = 1 | x; w)
def get_p(x, w):
	wTx = 0.
	for i, xi in x.items():
		wTx += w[i] * xi  # w[i] * x[i]
	return 1. / (1. + exp(-max(min(wTx, 50.), -50.)))  # bounded sigmoid

# Updating the model using SGD
def update_w(w, g, x, p, y):
	for i, xi in x.items():
		# alpha / (sqrt(g) + 1) is the adaptive learning rate heuristic
		# (p - y) * x[i] is the current gradient
		# note that in our case, if i in x then x[i] = 1
		delreg = (lambda1 * ((-1.) if w[i] < 0. else 1.) + lambda2 * w[i]) if i != 0 else 0.
		delta = (p - y) * xi + delreg
		if adapt > 0:
			g[i] += delta ** 2
		w[i] -= delta * alpha / (sqrt(g[i]) ** adapt)  # Minimising log loss
	return w, g

def create_model(datapart):
	train = '../Others/train_' + str(datapart+1) + '.txt'  # path to training file
	test = '../Others/test_' + str(datapart+1) + '.txt'  # path to testing file
	y_true = list()
	y_pred = list()
	logbatch = 10000
	D = 2 ** 12    # number of weights use for learning
	signed = False    # Use signed hash? Set to False for to reduce number of hash calls
	interaction = True
	lambda1 = 0.
	lambda2 = 0.
	if interaction:
		alpha = .004  # learning rate for sgd optimization
	else:
		alpha = .05   # learning rate for sgd optimization
	adapt = 1.        # Use adagrad, sets it as power of adaptive factor. >1 will amplify adaptive measure and vice versa
	fudge = .5        # Fudge factor
	header = ['Label', 'Country', 'Carrier', 'TrafficType', 'Device', 'Browser', 'OS', 'RefererUrl', 'UserIp', 'publisherId', 'advertiserCampaignId', 'Fraud', 'clickdateDay', 'clickdateHour', 'clickdateDayOfWeek', 'ct_ids_hour', 'ct_OS', 'ct_Carrier', 'ct_Country', 'ct_Device', 'ct_Browser', 'ct_RefererUrl', 'ct_publisherId', 'ct_subPublisherId', 'ct_advertiserCampaignId']
	# Initialization
	w = [0.] * D  # weights
	g = [fudge] * D  # sum of historical gradients
	loss = 0.
	lossb = 0.
	for t, row in enumerate(DictReader(open(train), header, delimiter='\t')):
		y = 1. if row['Label'] == '1' else 0.
		del row['Label']  # can't let the model peek the answer
		# 1. Getting hashed features
		x = get_x(row, D)
		# 2. Getting prediction
		p = get_p(x, w)
		# 3. Progress monitoring and validation
		lossx = logloss(p, y)
		loss += lossx
		lossb += lossx
		if t % logbatch == 0 and t > 1:
			print('%s\tencountered: %d\tcurrent whole logloss: %f\tcurrent batch logloss: %f' % (datetime.now(), t, loss/t, lossb/logbatch))
		lossb = 0.
		# 3. Updating the model
		w, g = update_w(w, g, x, p, y)
		# Predicting on the test set
	for t, row in enumerate(DictReader(open(test), header, delimiter='\t')):
		y_true.append(row[0])
	for t, row in enumerate(DictReader(open(test), header[1:], delimiter='\t')):
		x = get_x(row, D)
		p = get_p(x, w)
		y_pred.append(p)
	rmse = mean_squared_error(y_true, y_pred)
	return [datapart, w, g, rmse]

model_resuls = list()
part_list = range(16)

with ProcessPoolExecutor() as executor:
	for i, model_result in zip(part_list, executor.map(create_model,i)):
		model_resuls.append(model_result)

with open('../Others/Linear_Regression_AllModels.txt', "wb") as fp:
	pickle.dump(model_resuls, fp)
