#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 20:19:47 2022

@author: davidsaw
"""

#################################################
#### Para eliminar filas con muchos nulos #######
#################################################

from data_load import cargar_contactos, cargar_todo

#carga de archivos
test = cargar_todo()

def nulls_filter(data):

    # Cantidad de columnas
    cols_size = data.shape[1]

    # Cantidad de columnas con nulos
    data = data.replace("Otro", np.nan).replace("", np.nan)
    data['nulls_by_row'] = data.isnull().sum(axis = 1)

    # Porcentaje aceptable de eliminación de filas según cantidad de filas nulas
    percent_accept = 0.35

    data['percent'] = data.apply(lambda x: x['nulls_by_row']/cols_size, axis = 1)
    data['mantener'] = data['percent'].apply(lambda x: 1 if x <= percent_accept else 0)
    data['mantener'].value_counts()

    # Filtrar filas aceptables
    data = data[data['mantener']==1]
    data.drop(['nulls_by_row', 'mantener', 'percent'], inplace = True, axis = 1)
    return data

####################################

def cargar_cuentas():

    start_time = time.time()
    print('Inicia carga de cuentas')

    #Cargar los datos
    cuentas=pd.read_excel(data_path+'Cuentas.xlsx')
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    return cuentas


data_cuentas = cargar_cuentas()
data_cuentas.head()



