from sklearn.model_selection import train_test_split
import pandas as pd
from sklearn import tree
from sklearn.metrics import accuracy_score
from sklearn.model_selection import GridSearchCV
from sklearn import ensemble
from xgboost import XGBRegressor
from xgboost import plot_tree
import matplotlib.pyplot as plt
import numpy as np

"""
f = open('m_decision_tree_data.csv', 'r')
new_file = open('m_new_test.csv', 'w')
ready = False
for line in f:
    if ready:
        parts = line.split(',')
        formatted = parts[8].split('.')[0]
        new_line = parts[0] + ',' + parts[1] + ',' + parts[2] + ',' + parts[3] + ',' + parts[4] + ',' + parts[5] + ',' + parts[6] + ',' + parts[7] + ',' + formatted
        new_file.write(new_line + '\n')
    else:
        new_file.write(line)
        ready = True

"""
alg='e'
df = pd.read_csv(alg + '_new_test.csv', header=0, encoding="utf-8-sig")
#df['Throughput'] = df['Throughput'].astype(int)
#df.convert_objects(convert_numeric=True)
df = df.dropna()
df['Throughput'] = df['Throughput'].apply(lambda x: int((abs(x - 70) / 70) * 100))
#df = df.replace({"Algorithm": {'e': 0, 'm': 2.0, 'g': 1.0}})
#df = df.replace({"Thriftiness": {False: 0.0, True: 1.0}})
print(df)
#df = df['Thriftiness'].map({False: 0.0, True: 1.0})
y = df['Throughput']
X = df.drop('Throughput', axis=1)

X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1)
model = tree.DecisionTreeRegressor()
model.fit(X_train, y_train)

parameters = {'max_depth':range(3,4)}
clf = GridSearchCV(tree.DecisionTreeRegressor(), parameters, n_jobs=4, cv=10)
#clf = GridSearchCV(XGBRegressor(), parameters, n_jobs=4, cv=10)

clf.fit(X=X_train, y=y_train)
tree_model = clf.best_estimator_
print (clf.best_score_, clf.best_params_)
print(tree_model.feature_importances_)

from sklearn.linear_model import LassoCV
from sklearn.datasets import make_regression
from sklearn.feature_selection import SelectFromModel
import matplotlib.pyplot as plt

cols = df.columns
reg = LassoCV(cv=5, positive=True)
score = reg.fit(X,y).score(X, y)
print("Coefficients")
print(reg.coef_)

print("Reg score ")
print(score)
sfm = SelectFromModel(reg)
sfm.fit(X, y)
n_features = sfm.transform(X).shape[1]
print(n_features)
feature_idx = sfm.get_support()
#feature_name = df.columns[feature_idx]
print(feature_idx)

from sklearn.model_selection import cross_val_score

score = cross_val_score(tree_model, X_test, y_test)
#score = tree_model.score(X_test, y_test)
print(score)

dotfile = open(alg + "_dtree.dot", 'w')
dot_data = tree.export_graphviz(tree_model, out_file = dotfile, feature_names = X.columns)
dotfile.close()

from subprocess import call
call(['dot', '-Tpng', alg + '_dtree.dot', '-o', alg + '_tree.png', '-Gdpi=600'])

#y_predict = model.predict(X_test)
#accuracy_score(y_test, y_predict)

def plot():
    index = np.arange(len(cols) - 1)
    plt.bar(index, tree_model.feature_importances_)
    plt.xlabel('Parameter', fontsize=5)
    plt.ylabel('Gini Importance', fontsize=5)
    plt.xticks(index, cols, fontsize=5, rotation=30)
    plt.title('Gini Importance for Each Parameter')
    plt.show()
plot()
