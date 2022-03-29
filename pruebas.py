#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 20:19:47 2022

@author: davidsaw
"""

#################################################
#### Para eliminar filas con muchos nulos #######
#################################################

from data_load import cargar_contactos, cargar_todo, nulls_filter


####################################



data_exp, interes, contactos, demanda, eventos, llamada,cuentas=cargar_todo(70)