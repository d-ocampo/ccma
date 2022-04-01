#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: d-ocampo jdquinoneze
"""

import pandas as pd
from surprise import Reader
from surprise import Dataset
from surprise.model_selection import cross_validate
from surprise import NormalPredictor
from surprise import KNNBasic
from surprise import KNNWithMeans
from surprise import KNNWithZScore
from surprise import KNNBaseline
from surprise import SVD
from surprise import BaselineOnly
from surprise import SVDpp
from surprise import NMF
from surprise import SlopeOne
from surprise import CoClustering
from surprise.accuracy import rmse
from surprise import accuracy
from surprise.model_selection import train_test_split

import math as m

demanda = cargar_demanda()
demanda, eventos = arreglar_demanda(demanda)


start_time = time.time()
# Para elegir identificadores de clientes y eventos
rating = demanda[['IDENTASISTENTE', 'ID']]

# Para quitar registros sin datos del cliente
rating = rating.loc[~rating['IDENTASISTENTE'].isnull()]


# 144084 clientes
#   5241 eventos
# 441190 registros

# Para corregir los tipos de datos
rating['IDENTASISTENTE'] = rating['IDENTASISTENTE'].apply(lambda x: str(x))
rating['ID'] = rating['ID'].apply(lambda x: str(x))

# Para agrupar por asistente y evento, y contar repeticiones
rating = rating.groupby(['IDENTASISTENTE', 'ID']).size().reset_index(name='reps')

# Obtener el máximo de repeticiones para escalar
maximum = max(rating['reps'])
rating['reps'] = rating['reps'].apply(lambda x: m.ceil(x / abs(maximum) * 5))


reader = Reader(rating_scale=(1, 5))
data = Dataset.load_from_df(rating, reader)

benchmark = []
# Iterate over all algorithms
for algorithm in [NormalPredictor(), SVD(), BaselineOnly()]:
    # Perform cross validation
    results = cross_validate(algorithm, data, measures=['RMSE'], cv=3, verbose=False)
    
    # Get results & append algorithm name
    tmp = pd.DataFrame.from_dict(results).mean(axis=0)
    tmp = tmp.append(pd.Series([str(algorithm).split(' ')[0].split('.')[-1]], index=['Algorithm']))
    benchmark.append(tmp)

surprise_results = pd.DataFrame(benchmark).set_index('Algorithm').sort_values('test_rmse')
surprise_results


print('Using ALS')
bsl_options = {'method': 'als',
               'n_epochs': 5,
               'reg_u': 12,
               'reg_i': 5
               }
algo = BaselineOnly(bsl_options=bsl_options)

algo = SVD()
cross_validate(algo, data, measures=['RMSE'], cv=5, verbose=False)

trainset, testset = train_test_split(data, test_size=0.25)
algo = BaselineOnly(bsl_options=bsl_options)
predictions = algo.fit(trainset).test(testset)
accuracy.rmse(predictions)

trainset = algo.trainset
print(algo.__class__.__name__)

def get_Iu(uid):
    """ return the number of items rated by given user
    args: 
      uid: the id of the user
    returns: 
      the number of items rated by the user
    """
    try:
        return len(trainset.ur[trainset.to_inner_uid(uid)])
    except ValueError: # user was not part of the trainset
        return 0
    
def get_Ui(iid):
    """ return number of users that have rated given item
    args:
      iid: the raw id of the item
    returns:
      the number of users that have rated the item.
    """
    try: 
        return len(trainset.ir[trainset.to_inner_iid(iid)])
    except ValueError:
        return 0
    
df = pd.DataFrame(predictions, columns=['uid', 'iid', 'rui', 'est', 'details'])
df['Iu'] = df.uid.apply(get_Iu)
df['Ui'] = df.iid.apply(get_Ui)
df['err'] = abs(df.est - df.rui)

best_predictions = df.sort_values(by='err')[:10]
worst_predictions = df.sort_values(by='err')[-10:]

best_predictions

###############################################################


