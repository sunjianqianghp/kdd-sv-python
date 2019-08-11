# -*- coding: utf-8 -*-
import numpy as np
from sklearn import svm
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt

data = []
labels = []
with open("svm_data.txt") as ifile:
    for line in ifile.readlines():
        tokens = line.strip().split("\t")
        try:
            data.append([float(tk) for tk in tokens[:-1]])
            labels.append(int(tokens[-1]))
        except Exception:
            print("wrong")


x = np.array(data)
labels = np.array(labels)
y = np.zeros(labels.shape)
y[labels == 1] = 1

x_train, x_test, y_train, y_test = train_test_split(x, y, test_size = 0.3)

h = 0.1
x_min, x_max = x_train[:, 0].min() - 0.1, x_train[:,0].max() + 0.1
y_min, y_max = x_train[:, 1].min()  -   1, x_train[:, 1].max() +   1
xx, yy = np.meshgrid( np.arange(x_min, x_max, h),
                               np.arange(y_min, y_max, h))
titles = ['LinearSVC(linear lernel)',
           'SVC with RBF kernel',
           'SVC with Sigmoid kernel']
clf_linear = svm.SVC(kernel = 'linear').fit(x, y)
clf_rbf    = svm.SVC(gamma = 'auto').fit(x, y)
clf_sigmoid = svm.SVC(kernel = 'sigmoid').fit(x, y)

for i, clf in enumerate((clf_linear, clf_rbf, clf_sigmoid)):
    answer = clf.predict( np.c_[xx.ravel(), yy.ravel()] )
    plt.subplot(2,2,i+1)
    z = answer.reshape(xx.shape)
    plt.contourf(xx, yy, z, 2, alpha = 0.8)
    plt.xlabel('X')
    plt.ylabel('Y')
    plt.xticks(())
    plt.yticks(())
#     plt.titile(titles[i])
plt.show()