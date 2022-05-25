# -*- coding: utf-8 -*-
"""
Created on Tue May 17 16:37:47 2022

@author: D2O7L9C0
"""

from data_load import cargar_todo,conectar_colection_mongo_ccma, crear_cedulas_base, dict_personas_pm
import time
from tqdm import tqdm
####################################

#Cargar bases
data_exp, interes, contactos, demanda, eventos, llamada, cuentas=cargar_todo(70)

#Obtener cédulas
cedulas=crear_cedulas_base(data_exp,interes,contactos,demanda,llamada,cuentas)

#Conectar la colección elegida
clientes_col=conectar_colection_mongo_ccma('clientes',1)
eventos_col=conectar_colection_mongo_ccma('eventos',1)


#cargar la base una a una
start_time = time.time()
print('Inicia carga de cédulas')
pbar = tqdm(total=len(cedulas)) # Init pbar
count=0
for i,j in cedulas:
    #Conteo para ver el porcentaje
    count=count+1
    ced_no=[]
    pbar.update(n=1)
    try:
        #insertar 1 a 1 cada registro
        clientes_col.insert_one(dict_personas_pm(i,j,data_exp, interes, contactos, demanda, llamada, cuentas))
        #porcentaje de avance de la carga
        # print(round(count/len(cedulas)*100,3),'%')
    except:
        print('No entró')
        ced_no.append((i,j))
        continue
# tiempo transcurrido
time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
print(f"Tiempo transcurrido: {time_lapse}")
        

#cargar la base una a una
start_time = time.time()
print('Inicia carga de eventos')
pbar = tqdm(total=len(cedulas)) # Init pbar
count=0
for i,j in cedulas:
    #Conteo para ver el porcentaje
    count=count+1
    ced_no=[]
    pbar.update(n=1)
    try:
        #insertar 1 a 1 cada registro
        clientes_col.insert_one(dict_personas_pm(i,j,data_exp, interes, contactos, demanda, llamada, cuentas))
        #porcentaje de avance de la carga
        # print(round(count/len(cedulas)*100,3),'%')
    except:
        print('No entró')
        ced_no.append((i,j))
        continue
# tiempo transcurrido
time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
print(f"Tiempo transcurrido: {time_lapse}")



