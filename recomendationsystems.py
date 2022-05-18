#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 17 01:32:18 2022

@author: davidsaw
"""

import pandas as pd
from data_load import cargar_todo
from textdistance import levenshtein

####################################

#Cargar bases
data_exp, interes, contactos, demanda, eventos, llamada, cuentas=cargar_todo(70)


############################################
#### Unir las tablas con intersección ######
############################################


### eventos - eventos

#Cruzar nombre de eventos con ID
pd.merge(data_exp,eventos,left_on='NOMBRE_DEL_EVENTO',right)


data_exp.columns

demanda

### eventos - llamadas

pd.merge(demanda,llamada, on=['CEDULA_NEW'],how="inner")


### eventos - intereses

pd.merge(demanda,interes, on=['CEDULA_NEW'],how="inner")










