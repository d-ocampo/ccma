#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 20:19:47 2022

@author: davidsaw
"""

#################################################
#### Para eliminar filas con muchos nulos #######
#################################################

from data_load import cargar_contactos, cargar_todo, nulls_filter,traer_cuentas

#Para cargar el conector con mongo
import pymongo
from pymongo import MongoClient

####################################



data_exp, interes, contactos, demanda, eventos, llamada=cargar_todo(70)

cuentas=traer_cuentas(70)





#### 1. COnectar la base de datos

client = pymongo.MongoClient("mongodb+srv://proyecto_uniandes:ALGGKhn28wNgnGv@cluster0.66yl3.mongodb.net/ccma?retryWrites=true&w=majority")
#Cargar la base de ccma
db = client['ccma']
#cargar la colección elegida
collection_clientes = db["clientes"]



