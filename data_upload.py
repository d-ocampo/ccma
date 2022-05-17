# -*- coding: utf-8 -*-
"""
Created on Tue May 17 16:37:47 2022

@author: D2O7L9C0
"""

from data_load import cargar_todo,conectar_colection_mongo_ccma, crear_cedulas_base, dict_personas_pm
import time
####################################

#Cargar bases
data_exp, interes, contactos, demanda, eventos, llamada, cuentas=cargar_todo(70)

#Obtener cédulas
cedulas=crear_cedulas_base(data_exp,interes,contactos,demanda,llamada,cuentas)

#Conectar la colección elegida
clientes_col=conectar_colection_mongo_ccma('clientes',1)

#cargar la base una a una
start_time = time.time()
print('Inicia carga de cédulas')
count=0
for i,j in cedulas:
    count=count+1
    print(round(count/len(cedulas)*100,3))
    print(i,j)
    ced_no=[]
    try:
        #insertar 1 a 1 cada registro
        clientes_col.insert_one(dict_personas_pm(i,j))
    except:
        ced_no.append((i,j))
        continue
# tiempo transcurrido
time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
print(f"Tiempo transcurrido: {time_lapse}")
        





