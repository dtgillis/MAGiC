__author__ = 'dtgillis'

from sklearn.cross_validation import StratifiedKFold
import sklearn.linear_model as lin_model
from scipy.stats import pearsonr
import numpy as np
from sklearn.preprocessing import scale
from sklearn.grid_search import GridSearchCV
from sklearn.grid_search import ParameterGrid
from sklearn.metrics import make_scorer
from sklearn.tree import DecisionTreeClassifier


def clean_pearsonr(y_true, y_predict):

    pearson_r = pearsonr(y_true, y_predict)
    if np.isnan(pearson_r[0]):
        return .0
    else:
        return (pearson_r[0])**2


class LogisticRegressor():

    def __init__(self, params=None):

        # self.folds = folds
        if params is not None:
            self.log_regressor = lin_model.LogisticRegression(C=float(params['C']), penalty=params['penalty'])
        else:
            self.log_regressor = lin_model.LogisticRegression()


    def grid_search(self, X, y):

        X = scale(X)
        cv = StratifiedKFold(y=y, n_folds=5)
        parameters = {'C': 10.0 ** np.arange(-2, 9), 'penalty': ['l1', 'l2']}
        clf = GridSearchCV(estimator=self.log_regressor, param_grid=parameters, n_jobs=5, cv=cv, refit=True,
                           scoring=make_scorer(clean_pearsonr))
        clf.fit(X, y)

        return clf.best_score_, clf.best_params_


    def fit_data(self, y, X,):

        skf = StratifiedKFold(y, n_folds=5)

        pearsonr_list = []
        X = scale(X)

        coeff_list = []

        for train_index, test_index in skf:

            x_train, y_train = X[train_index], y[train_index]
            x_test, y_test = X[test_index], y[test_index]

            self.log_regressor.fit(X=x_train, y=y_train)

            coeff_list.append(self.log_regressor.coef_)

            y_predict = self.log_regressor.predict(X=x_test)

            pearsonr_list.append(clean_pearsonr(y_test, y_predict))

        coeff_dict = dict()

        for i, coeff in enumerate(coeff_list):
            for j, values in enumerate(coeff):
                if j not in coeff_dict:
                    coeff_dict[j] = np.zeros((len(coeff_list), len(values)))
                coeff_dict[j][i, :] = values


        return pearsonr_list, coeff_dict


class DTC():

    def __init__(self, snp_name):
        self.snp_name = snp_name
        self.classify = DecisionTreeClassifier(compute_importances=True)

    def grid_search(self, X, y):

        X = scale(X)
        parameters = {'criterion': ['gini', 'entropy']}
        clf = GridSearchCV(estimator=self.classify, param_grid=parameters, n_jobs=5, cv=5, refit=True,
                           scoring=make_scorer(clean_pearsonr))
        clf.fit(X, y)
        print "Decision Tree"
        print self.snp_name
        print clf.best_estimator_
        print clf.best_score_














