#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 20:19:47 2022

@author: davidsaw
"""

#################################################
#### Para eliminar filas con muchos nulos #######
#################################################

from data_load import cargar_todo

####################################

#Cargar bases
data_exp, interes, contactos, demanda, eventos, llamada, cuentas=cargar_todo(70)

#Obtener cédulas
cedulas=crear_cedulas_base(data_exp,interes,contactos,demanda,llamada)

#Conectar la colección elegida
clientes_col=conectar_colection_mongo_ccma('clientes')

#cargar la base una a una
count=0
for i,j in cedulas_unicas:
    count=count+1
    print(round(count/len(cedulas_unicas)*100,3))
    print(i,j)
    ced_no=[]
    try:
        #insertar 1 a 1 cada registro
        clientes_col.insert_one(dict_personas_pm(i,j))
    except:
        ced_no.append((i,j))
        continue
        
        





