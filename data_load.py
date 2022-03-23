#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: d-ocampo jdquinoneze
"""

#Librerías cargadas
import pandas as pd    # Para el manejo de dataframes
import numpy as np     # Para algunas operaciones numÃ©ricas
import time            # Para calcular tiempo de ejecuciÃ³n
import re              # Para manipular cadenas

import os
from os import listdir                   # Para listar directorios
from os.path import isfile, join, isdir  # Para manipular archivos

import json # Para manipulación de diccionarios
########################################################################

# Rutas
data_path = os.getcwd()+"/data/"
exp_path = "Experiencia del cliente/"

files_exp_path = [f for f in listdir(data_path + exp_path) if isfile(join(data_path + exp_path, f))]

# Columnas para verificar en los archivos de experiencia de datos
exp_cols = ['Origen', 'Identificación de respuesta ', 'Nombre del evento',
       'Fecha creación', 'Cédula ', 'Nombre ', 'Empresa', 'Cargo', 'Nit',
       'Correo electrónico', 'Teléfono', 'Sede ', 'Pregunta Nivel1',
       'Pregunta Nivel2', 'Respuesta']

#Diccionarios de agrupación de servicios
dict_agrupacion={
    "CP": "Capacitación",
    "SR": "Servicios registrales",
    "OT": "Otros",
    "SE": "Servicios especializados",
    "FE": "Fortalecimiento empresarial",
    "MA": "Métodos alternativos para la solución de conflictos"
    }

# Diccionario de nivel educativo
dict_nivel_educativo={4: "Universitario",
    5 :"Especialista",
    3 :"Tecnólogo",
    2 :"Técnico",
    6 :"Magister",
    1 :"Bachiller",
    10:"Secundaria",
    9:"Primaria",
    7:"Doctor",
    8:"Otro"   
    }


################################
### Funciones de arreglo data ##
################################


maestra_interes=pd.read_excel(data_path+"Maestra Temas Interes.xlsx")


#Crear diccionario de homolagación de cargos
def diccionario_cargos(ruta):
    areas_homologar=pd.read_excel(ruta)
    areas_homologar.index=areas_homologar['Área de cargo']
    areas_homologar=areas_homologar.drop(['Área de cargo'],axis=1)
    # areas_homologar['Palabras que contengan']=areas_homologar['Palabras que contengan'].apply(lambda x: x.lower().split(","))
    areas_homologar['Palabras que contengan']=areas_homologar['Palabras que contengan'].apply(lambda x: x.lower())
    dict_areas=areas_homologar.to_dict()['Palabras que contengan']
    return dict_areas

# Enviar diccionario de cargos a data
with open(data_path+'dict_cargo.json', 'w') as outfile:
    json.dump(diccionario_cargos(data_path + 'Tabla homologación áreas de cargo .xlsx'), outfile)

# Cargar diccionario de cargos al entorno
with open(data_path+'dict_cargo.json') as json_file:
    dict_cargo = json.load(json_file)


#Buscar cargo similar según validación
def homologar_cargo(dictionary, value):
    foundkeys = []
    for key in dictionary:
        for i in value.lower().split():
            if i in dictionary[key]:
                foundkeys.append(key)
    foundkeys.append("Otro")
    return foundkeys[0]

# Homologar tipo asistente
def validar_tipo_asistente(palabra):
    if 'EMPRE' in palabra.upper():
        return 'EMPRESA'
    else:
        return 'INDEPENDIENTE'




#####################################
### Funciones de minería de datos ###
#####################################

#### 1. Data experience #############


#Función que genera los archivos unificados de experiencias
def generar_data_exp(exp_cols):
    # Inicio de la ejecución
    print("Inicia unión de data experience")
    start_time = time.time()
    # Para cargar todos los archivos de experiencia
    data_experience = pd.DataFrame()
    
    #Ciclo para unir todos los archivos de cliente
    for file in files_exp_path:
       df = pd.concat(pd.read_excel(data_path + exp_path + file,
                      sheet_name=None, usecols=exp_cols), ignore_index=True, sort=False)
       data_experience = data_experience.append(df, ignore_index=True)
    
    
    # Para corregir los nombres de las columnas
    exp_cols = [col.strip().upper().replace(" ", "_").replace("Ã", "E").replace("Ã", "O").replace("É", "E").replace("Ó", "O") for col in exp_cols]
    data_experience.columns = exp_cols
    
    #FInaliza ejecución
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")

    return data_experience

########################################################################

def arreglar_data_exp(data_experience,exp_cols,dict_cargo):
    # Inicio de la ejecución
    start_time = time.time()
    print('Inicia arreglo de data experience')

    #Estabdarizar columnas
    exp_cols = [col.strip().upper().replace(" ", "_").replace("Ã", "E").replace("Ã", "O").replace("É", "E").replace("Ó", "O") for col in exp_cols]
    data_experience.columns = exp_cols
    
    # Nueva variable de categorí­a a partir del Origen
    data_experience['ORIGEN_CAT'] = data_experience['ORIGEN'].apply(lambda x: x[:2])
    
    # Para completar los datos nulos
    data_experience['NOMBRE_DEL_EVENTO'].fillna("Sin especificar", inplace = True)
    data_experience['SEDE'].fillna("Sin especificar", inplace = True)
    
    
    # Correcciones sobre el nÃºmero de cÃ©dula
    data_experience['CEDULA_NEW'] = data_experience['CEDULA'].apply(lambda x: "".join(re.findall('\d+', str(x))))
    data_experience['CEDULA_NEW'] = data_experience.apply(lambda x: "".join(re.findall('\d+', str(x['NOMBRE']))) if x['CEDULA_NEW'] == "" else x['CEDULA_NEW'], axis = 1)
    data_experience['CEDULA_NEW'] = data_experience['CEDULA_NEW'].apply(lambda x: 0 if x == "" else int(x))
    
    # Correcciones sobre el nombre
    data_experience['NOMBRE_NEW'] = data_experience.apply(lambda x: x['CEDULA'] if len("".join(re.findall('\d+', str(x['NOMBRE']))))>0  else x['NOMBRE'], axis = 1 )
    
    # Correcciones sobre el email y el telÃ©fono
    data_experience['CORREO_NEW'] = data_experience.apply(lambda x: str(x['CORREO_ELECTRONICO']) if "@" in str(x['CORREO_ELECTRONICO']) else "", axis = 1)
    data_experience['TELEFONO_NEW'] = data_experience.apply(lambda x: x['TELEFONO'] if len("".join(re.findall('\d+', str(x['TELEFONO']))))>=7 else "", axis = 1)

    #Homologar cargos con el diccionario de cargos
    data_experience['CARGO_HOMOLOGO']=data_experience['CARGO'].apply(lambda x: homologar_cargo(dict_cargo,str(x)))

    # Para presentar el tiempo transcurrido desde el inicio de la ejecuciÃ³n
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    
    return data_experience


#### 2. Temas de interés ##############


def cargar_interes()

#Cargar la tabla
temas_interes=pd.read_excel(data_path+'TemasInteres.xlsx')
#reemplazar columnas
temas_interes.columns=[col.upper() for col in temas_interes.columns]

#reemplazar nombre de cédula
temas_interes.rename(columns={'NRO_CEDULA':'CEDULA'},inplace=True)

#Poner en mayúscula los nombres
temas_interes['NOMBRE']=temas_interes['NOMBRE'].apply(lambda x: str(x).upper()) 
temas_interes['APELLIDO']= temas_interes['NOMBRE'].apply(lambda x: str(x).upper()) 

#Reemplazar cargo
temas_interes['CARGO_HOMOLOGO']=temas_interes['CARGO'].apply(lambda x: homologar_cargo(dict_cargo,str(x)))

#Arreglar nivel educativo
temas_interes['NIVEL_EDUCATIVO']=temas_interes['NIVEL_EDUCATIVO'].fillna(8).apply(lambda x: x if str(x).isdigit() else 8).astype(int).apply(lambda x: x if x in list(dict_nivel_educativo.keys()) else 8)
temas_interes['NOMB_NIVEL_EDUCATIVO']=temas_interes['NIVEL_EDUCATIVO'].apply(lambda x: dict_nivel_educativo[x])

#Arreglar tipo asistente
temas_interes['TIPO_ASISTENTE']=temas_interes['TIPO_ASISTENTE'].fillna('OTRO').apply(lambda x: 'OTRO' if str(x).isdigit() else validar_tipo_asistente(x).upper() )




##################################
### Funciones de carga de data ###
##################################

#### 1. Data experience ################
data_exp=generar_data_exp(exp_cols)
data_exp=arreglar_data_exp(data_exp,exp_cols,dict_cargo)





