#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 17 01:32:18 2022

@author: davidsaw
"""

from data_load import cargar_todo

####################################

#Cargar bases
data_exp, interes, contactos, demanda, eventos, llamada, cuentas=cargar_todo(70)


############################################
#### Unir las tablas con intersección ######
############################################

