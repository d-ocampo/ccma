#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 17 01:32:18 2022

@author: davidsaw
"""

import pandas as pd
from data_load import cargar_todo

from collections import Counter

# Librerías de SR
from surprise import Dataset
from surprise import Reader
from surprise.model_selection import cross_validate
from surprise.model_selection import train_test_split

from sklearn.metrics.pairwise import cosine_similarity

import joblib
import random


# modelo basado en contenido
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

# Algoritmos de surprise
from surprise import SVD
from surprise import SVDpp
from surprise import SlopeOne
from surprise import NormalPredictor

from surprise import KNNBaseline
from surprise import KNNBasic
from surprise import KNNWithMeans
from surprise import KNNWithZScore
from surprise import BaselineOnly
from surprise import CoClustering

####################################

# Cargar bases
# data_exp, interes, contactos, demanda, eventos, llamada, cuentas = cargar_todo(
#     70)


############################################
#### Unir las tablas con intersección ######
############################################


### eventos - eventos

# Cruzar nombre de eventos con ID
# Establecer a los eventos que han ido
# dem_base = demanda[['CEDULA_NEW', 'ID']]
# Pasos para fusionar la base
# Crear cada asistencia a lista
# dem_base_melt = dem_base.assign(ID=dem_base.ID.str.split(",")).sample(10000)

# Separa el listado a filas
# dem_base_melt = dem_base_melt.ID.apply(pd.Series) \
#     .merge(dem_base_melt, right_index=True, left_index=True) \
#     .drop(["ID"], axis=1) \
#     .melt(id_vars=['CEDULA_NEW'], value_name="ID") \
#     .drop('variable', axis=1)

# Variable binaria de asistencia
# dem_base_melt['ASISTENCIA'] = dem_base_melt['ID'].apply(
#     lambda x: 0 if pd.isna(x) else 1)
# # Crear base para el SR
# demanda_base = dem_base_melt.rename(columns={'CEDULA_NEW': 'userID',
#                                              'ID': 'itemID',
#                                              'ASISTENCIA': 'rating'})

# # Borrar la base para no consumir tanta memoria
# del(dem_base_melt)

# ### eventos - llamadas
# llam_base = pd.merge(demanda, llamada, on=['CEDULA_NEW'], how="inner")
# llam_base = llam_base.dropna(subset=['ID', 'TEMA DEL SERVICIO1'])


# ### eventos - intereses
# int_base = pd.merge(demanda, interes, on=['CEDULA_NEW'], how="inner")
# # Eliminar repetidos
# int_base = int_base.drop_duplicates(subset='CEDULA_NEW')


# ##################################
# #### Sistemas de recomendación ###
# ##################################

# ###### Eventos - eventos
# ####
# # Escala binaria
# reader = Reader(rating_scale=(0, 1))
# # obtener la base como se necesita
# data_e_e = Dataset.load_from_df(
#     demanda_base[['userID', 'itemID', 'rating']], reader)

# # Interés eventos
# ####
# # resumir la base por evento-interés
# int_base = int_base.dropna(subset=['TEMAS_INTERES', 'ID'])
# int_base_g = int_base[['CEDULA_NEW', 'ID', 'TEMAS_INTERES']]


def probar_algoritmos(data, nombre):
    # Probar algoritmos
    benchmark = []
    # Iterate over all algorithms
    for algorithm in [SVD(), SVDpp(), SlopeOne(), NormalPredictor()
                      # KNNBaseline(), KNNBasic(), KNNWithMeans(), KNNWithZScore(), BaselineOnly(), CoClustering()
                      ]:
        # Perform cross validation
        results = cross_validate(algorithm, data, measures=[
                                 'RMSE'], cv=3, verbose=False)

        # Get results & append algorithm name
        tmp = pd.DataFrame.from_dict(results).mean(axis=0)
        tmp = tmp.append(pd.Series([str(algorithm).split(
            ' ')[0].split('.')[-1]], index=['Algorithm']))
        benchmark.append(tmp)

    # Crear la tabla de comparaciones
    bench = pd.DataFrame(benchmark).set_index(
        'Algorithm').sort_values('test_rmse')
    # Exportar la tabla a un csv

    nombre = 'Evento - Evento'
    bench.to_csv('RES Algoritmos '+nombre + '.csv', sep=';', decimal=',')
    print('OK escritura de '+nombre)


# Entrenar con el algoritmo elegido
def crear_modelo(ALGO, data, mod_options, nombre):
    # usar modelo pequeño para entrenar
    binary_data, test = train_test_split(data, test_size=.2)

    # #usuarios para la predicción
    # users=list(ratings['userId'].unique())

    # se crea un modelo
    algo = ALGO()

    # ajustar algoritmo y crear matriz de predicciones
    algo.fit(trainset=binary_data)
    predictions = algo.test(test)
    # exportar el modelo
    joblib.dump(algo, 'models/model_'+nombre+'.pkl')
    print('OK')
    return algo, predictions


def get_Iu(uid):
    try:
        return len(trainset.ur[trainset.to_inner_uid(uid)])
    except ValueError:  # user was not part of the trainset
        return 0


def get_Ui(iid):
    try:
        return len(trainset.ir[trainset.to_inner_iid(iid)])
    except ValueError:
        return 0


def recomendacion_cf(data, algo, event, n):
    binary_data, test = train_test_split(data, test_size=.2)
    # crear las predicciones
    predictions = algo.test(test)
    # base para listado
    df_rec = pd.DataFrame(predictions, columns=[
                          'uid', 'iid', 'rui', 'est', 'details'])
    # filtrar por evento
    if len(df_rec[df_rec['iid'] == event]) < n:
        df_rec = df_rec.sample(n)
    else:
        df_rec = df_rec[df_rec['iid'] == event].sample(n)
    # eventos más frecuentes
    most_freq = Counter(
        " ".join(df_rec["iid"].astype(str)).split()).most_common(20)
    # eliminar vacíos
    df_rec = df_rec.fillna(0)
    # calcular el score
    df_rec['SCORE'] = abs(df_rec['est']-df_rec['rui'])
    return df_rec[['uid', 'SCORE']].rename(columns={'uid': 'CEDULA_NEW'}), most_freq


def volver_cat_prediccion(numero, maximo, inv):
    cat = ''
    # revisa si es invertido dependiendo lo que se esté buscando
    if inv == False:
        if numero < maximo/3:
            cat = 'Baja'
        elif numero < maximo*2/3:
            cat = 'Media'
        else:
            cat = 'Alta'
    elif inv == True:
        if numero < maximo/3:
            cat = 'Alta'
        elif numero < maximo*2/3:
            cat = 'Media'
        else:
            cat = 'Baja'
    return cat


def creacion_matrix_idf(data_text, nombre):
    tfidf = TfidfVectorizer()
    data_text = data_text.fillna("")
    # Construct the required TF-IDF matrix by applying the fit_transform method on the overview feature
    overview_matrix = tfidf.fit_transform(data_text)
    joblib.dump(tfidf, 'models/model_'+nombre+'.pkl')
    print('OK '+nombre)
    return tfidf


# Devolver una lista de recomendación con coseno
def recomendacion_coseno(data, event, n, idf, tipo):
    if len(data) > 10000:
        data = data.sample(10000).reset_index()
    # Crear la columna de texto
    if tipo == 'interes':
        data['data_text'] = data.apply(lambda x: str(
            x['ID']+','+x['TEMAS_INTERES']).replace(",", " "), axis=1)
    elif tipo == 'llamada':
        data['data_text'] = data.apply(lambda x: str(
            x['ID']+','+x['TEMA DEL SERVICIO1']).replace(",", " "), axis=1)

    # la matrix de terminos con el modelo ya calibrado
    overview_matrix = idf.fit_transform(data['data_text'])
    # Matriz de similiridad
    similarity_matrix = linear_kernel(overview_matrix, overview_matrix)

    # Listado de cédulas que han ido al evento
    if len(data[data['ID'].str.contains(event)]['CEDULA_NEW']) == 0:
        cedula_index = data['CEDULA_NEW'].sample(1).to_list()
    else:
        cedula_index = data[data['ID'].str.contains(
            event)]['CEDULA_NEW'].sample(1).to_list()

    print(cedula_index)
    # buscar el índice de esas cédulas para la matriz
    index_matriz = data[data['CEDULA_NEW'].isin(cedula_index)].index

    # similarity_score is the list of index and similarity matrix
    if index_matriz > len(similarity_matrix):
        similarity_score = list(
            enumerate(similarity_matrix[[random.randint(0, len(similarity_matrix)-2)]][0]))
    else:
        similarity_score = list(enumerate(similarity_matrix[index_matriz][0]))
    # sort in descending order the similarity score of movie inputted with all the other movies
    similarity_score = sorted(
        similarity_score, key=lambda x: x[1], reverse=True)
    similarity_score = similarity_score[1:n]

    # buscar las cédulas con ese índice
    data_rec = data.iloc[[i[0] for i in similarity_score]]
    data_rec['SCORE'] = [i[1] for i in similarity_score]
    most_freq = Counter(
        " ".join(data_rec["ID"].replace(",", " ")).split()).most_common(20)
    return data_rec[['CEDULA_NEW', 'SCORE']], most_freq, cedula_index


######################################
#### Modelos joblib de algoritmos ####
######################################

# Crear modelo evento - evento
# modelo_ev_ev, pred_modelo_e_e = crear_modelo(SlopeOne,
#                                              data_e_e,
#                                              {},
#                                              'modelo_evento_evento')

# cargar modelo
# model_cf_e_e = joblib.load('models/model_modelo_evento_evento.pkl')

# df_cf,most=recomendacion_cf(data_e_e, model_cf_e_e, 2428,100)


#############
# Crear modelo basado en contenido
#############

# Interés

# Crear matrix tfidf
# creacion_matrix_idf(int_base_g.apply(lambda x: str(
#     x['ID']+','+x['TEMAS_INTERES']).replace(",", " ")), 'tfdidf interes - evento')
# cargar idf
# idf_eventos_int = joblib.load('models/model_tfdidf interes - evento.pkl')

# df_cf,most,one=recomendacion_coseno(int_base_g,event,100,idf_eventos_int,'interes')

# Llamadas

# creacion_matrix_idf(llam_base.apply(lambda x: str(
#     x['ID']+','+x['TEMA DEL SERVICIO1']).replace(",", " "), axis=1), 'tfdidf llamada - evento')
# idf_eventos_llam = joblib.load('models/model_tfdidf llamada - evento.pkl')

# df_cf,most,one=recomendacion_coseno(llam_base[['CEDULA_NEW','ID','TEMA DEL SERVICIO1']],event,100,idf_eventos_llam,'llamada')
