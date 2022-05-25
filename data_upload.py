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

# Para sacar el listado de cédulas ya cargadas
response = clientes_col.find({'identificación': {'$ne': ''}})
cedulas_ya_cargadas = [doc['identificación'] for doc in response]
cedulas_ya_cargadas = [(int(x.split('-')[0]),int(x.split('-')[1])) for x in cedulas_ya_cargadas]
print('Cédulas ya cargadas: ', len(cedulas_ya_cargadas))

#para filtrar las cédulas ya cargadas del listado de cédulas original
cedulas = [registro for registro in cedulas if registro not in cedulas_ya_cargadas]
print('Cédulas por cargar: ', len(cedulas))

# Para cargar en la base de MongoDB
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
for i in eventos['ID'].to_list():
    ev_dict=eventos[eventos['ID']==i].to_dict('records')[0]
    eventos_col.insert_one(ev_dict)
# tiempo transcurrido
time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
print(f"Tiempo transcurrido: {time_lapse}")



