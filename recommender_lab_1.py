#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: d-ocampo jdquinoneze
"""

# !pip install numpy
# !pip install scikit-surprise
# !pip install pandas
# !pip install seaborn

import os
import numpy as np
import pandas as pd
import seaborn as sns
from surprise import Reader
from surprise import Dataset
from surprise.model_selection import train_test_split
from surprise import KNNBasic
from surprise import SVD
from surprise import accuracy
import random
import math as m

# Para garantizar reproducibilidad en resultados
seed = 10
random.seed(seed)
np.random.seed(seed)

demanda = cargar_demanda()
demanda, eventos = arreglar_demanda(demanda)


start_time = time.time()

# porcentaje de registros
n = 0.1

# Para elegir identificadores de clientes y eventos
rating = demanda[['IDENTASISTENTE', 'ID']]
rating = demanda.sample(int(demanda.shape[0]*n))[['IDENTASISTENTE', 'ID']]


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
rating['reps'].sum()


rating.groupby(['IDENTASISTENTE', 'ID']).count()

# Obtener el máximo de repeticiones para escalar
maximum = max(rating['reps'])
rating['reps'] = rating['reps'].apply(lambda x: m.ceil(x / abs(maximum) * 5))


rating.columns = [ 'user_id', 'item_id', 'rating']

############################################################
## Creación del dataset de entrenamiento y prueba
############################################################

reader = Reader( rating_scale = ( 1, 5 ) )
# Se crea el dataset a partir del dataframe
surprise_dataset = Dataset.load_from_df( rating[ [ 'user_id', 'item_id', 'rating' ] ], reader )

# Para dividir la base entre entrenamiento y prueba
train_set, test_set=  train_test_split(surprise_dataset, test_size=.2)

# Ejemplo 
ejemplo = test_set[10]

# Se crea un modelo knnbasic item-item con similitud coseno 
sim_options = {'name': 'cosine',
               'user_based': False  # calcule similitud item-item
               }
algo = KNNBasic(k=20, min_k=2, sim_options=sim_options)
# algo = SVD()


# Se le pasa la matriz de utilidad al algoritmo 
algo.fit(trainset=train_set)

#Verifique la propiedad est de la predicción
algo.predict(ejemplo[0], ejemplo[1])
demanda[demanda['ID']==int(ejemplo[1])][['ID', 'NOMBRE', 'LUGAR', 'TEMA']].drop_duplicates()


# Calcular la predicción para todos los elementos del conjunto de test
test_predictions=algo.test(test_set)

# En promedio, el sistema encuentra ratings que estan una estrella por encima o por debajo del rating del usuario
accuracy.rmse( test_predictions, verbose = True )


############################################################
## Generando listas de predicciones para los usuarios
############################################################

#Se crea el dataset para modelo 
rating_data=surprise_dataset.build_full_trainset()
# Se crea dataset de "prueba" con las entradas faltantes para generar las predicciones
test=rating_data.build_anti_testset()

# se crea el mismo modelo que el del ejemplo
sim_options = {'name': 'cosine',
               'user_based': False  # calcule similitud item-item
               }
algo = KNNBasic(k=20, min_k=2, sim_options=sim_options)
algo.fit(rating_data)
predictions=algo.test(test)


#10 primeras predicciones
predictions[0:10]


#Predicciones para usuario 196
user_predictions=list(filter(lambda x: x[0]=='43011772',predictions))
user_predictions


# tiempo transcurrido
time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
print(f"Tiempo transcurrido: {time_lapse}")
