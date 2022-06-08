#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 23:38:33 2022

@author: davidsaw
"""
from data_load import cargar_todo,conectar_colection_mongo_ccma, crear_cedulas_base, dict_personas_pm,cargar_demanda
import time
from tqdm import tqdm
####################################

#Cargar bases
# data_exp, interes, contactos, demanda, eventos, llamada, cuentas=cargar_todo(70)

demanda=cargar_demanda()
demanda,eventos=arreglar_demanda(demanda)

#Obtener cédulas
# cedulas=crear_cedulas_base(data_exp,interes,contactos,demanda,llamada,cuentas)

#Conectar la colección elegida
clientes_col=conectar_colection_mongo_ccma('clientes',1)
eventos_col=conectar_colection_mongo_ccma('eventos',1)




#cargar la base una a una
start_time = time.time()
print('Inicia carga de eventos')
pbar = tqdm(total=len(eventos)) # Init pbar
count=0
for i in eventos['ID'].unique():
    #Conteo para ver el porcentaje
    count=count+1
    ev_no=[]
    pbar.update(n=1)
    try:
        #insertar 1 a 1 cada registro
        eventos_col.insert_one(eventos[eventos['ID']==i].to_dict('records')[0])
    except Exception as e:
        print(e)
        print('No entró')
        ev_no.append(i)
        continue
# tiempo transcurrido
time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
print(f"Tiempo transcurrido: {time_lapse}")



