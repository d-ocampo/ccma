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

#################################
### 0. Fixed Data ###############
#################################

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

#Diccionario de tipos de documentos
dict_tipo_doc={1:'Cedula de ciudadanía',
    2:'Cedula de extranjería',
    3:'Pasaporte',
    4:'Tarjeta de identidad',
    5:'NIT',
    6:'Empresa Extranjera',
    7:'Número único de identificación personal NUIP',
    8:'Registro Civil'
                }


#Cruce entre interés demanda e intereses
#Datos de demanda
# Registros                              117076
# Gerencia y Administración               75081
# Jurídica                                66807
# Innovación y Transformación Digital     39582
# Contabilidad y Finanzas                 32321
# Mercadeo y Servicio al Cliente          27926
# Ventas                                  20834
# Proyectos y servicios                   16998
# Gestión Humana                          14980
# Productividad                           11122
# Economía                                 8372
# Negocios y networking                    7467
# Gobierno Corporativo                     5372
# Internacionalización                     4482
# Música                                   3581
# Desarrollo del Ser                       1566
# Gobierno                                 1513
# Literatura                                810
# Cine                                      745
# Arte                                      736
# Solución de Conflictos                    706
# Propiedad intelectual                     146
# Sostenibilidad                            138

# maestra_interes=pd.read_excel(data_path+"Maestra Temas Interes.xlsx")

################################
### Funciones de arreglo data ##
################################


# function to get unique values
def unique(list1):
    # insert the list to the set
    list_set = set(list1)
    # convert the set to the list
    unique_list = (list(list_set))
    return unique_list


#Crear diccionario de homolagación de cargos a partir del excel
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

#Función para devolver la llave de un diccionario
def devolver_llave(diccionario, item):
    res=list(filter(lambda x: x[1]==item,list(diccionario.items())))
    return res[0][0]


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

# función para arreglar data de experiencia
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

#Cargar las bases de interés
def cargar_interes():
    start_time = time.time()
    print('Inicia carga de temas de interés')
    #Cargar la tabla
    temas_interes=pd.read_excel(data_path+'TemasInteres.xlsx')
    #tiempo
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    return temas_interes

# Arreglar la tabla de interés
def arreglar_interes(temas_interes):
    start_time = time.time()
    print('Inicia arreglo de temas de interés')
    #reemplazar columnas
    temas_interes.columns=[col.upper() for col in temas_interes.columns]
    
    #reemplazar nombre de cédula
    temas_interes.rename(columns={'NRO_CEDULA':'CEDULA'},inplace=True)
    
    # Correcciones sobre el nÃºmero de cÃ©dula
    temas_interes['CEDULA_NEW'] = temas_interes['CEDULA'].apply(lambda x: "".join(re.findall('\d+', str(x))))
    temas_interes['CEDULA_NEW'] = temas_interes.apply(lambda x: "".join(re.findall('\d+', str(x['NOMBRE']))) if x['CEDULA_NEW'] == "" else x['CEDULA_NEW'], axis = 1)
    temas_interes['CEDULA_NEW'] = temas_interes['CEDULA_NEW'].apply(lambda x: 0 if x == "" else int(x))
    
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

    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    return temas_interes

#### 3. Contactos ##############

def cargar_contactos():
    start_time = time.time()
    print('Inicia carga de contactos')
    #Cargar los datos
    contactos=pd.read_excel(data_path+'Contactos.xlsx')
    
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    return contactos
    
def arreglar_contactos(contactos,devolver_llave,dict_tipo_doc):
    start_time = time.time()
    print('Inicia arreglo de contactos')
    #modificar las columnas
    contactos.columns=[col.upper().strip() for col in contactos.columns]
    #Cambiar nombre de columna cedula
    contactos.rename(columns={'NÚMERO DE DOCUMENTO':'CEDULA'},inplace=True)
    #Arreglar el tipo de identificación
    contactos['TIPO DE DOCUMENTO']=contactos['TIPO DE DOCUMENTO'].replace('C','Cedula de ciudadanía')
    #Llenar el tipo de contacto
    contactos['TIPO DE CONTACTO'].fillna('Otro',inplace=True)
    #Resumir por número de cédula
    contactos=contactos.groupby(['TIPO DE DOCUMENTO','CEDULA']).agg(
        {'NOMBRE COMPLETO':['first','count'],
         'DIRECCIÓN':'first', 
         'CIUDAD':'first', 
         'DEPARTAMENTO':'first', 
         'PAÍS':'first', 
         'TIPO DE CONTACTO':'first',
         'MIEMBRO DE LA JUNTA DIRECTIVA CCMA':'max'}
        )
    #MOdificar índices de filas y columnas
    contactos.columns=['NOMBRE COMPLETO','CANTIDAD_LLAMADAS', 'DIRECCIÓN', 'CIUDAD', 'DEPARTAMENTO', 'PAÍS',
           'TIPO DE CONTACTO', 'MIEMBRO DE LA JUNTA DIRECTIVA CCMA']
    contactos.reset_index(inplace=True)
    #Arreglar cédula    
    contactos['CEDULA_NEW'] = contactos['CEDULA'].apply(lambda x: "".join(re.findall('\d+', str(x))))
    contactos['CEDULA_NEW'] = contactos.apply(lambda x: "".join(re.findall('\d+', str(x['NOMBRE COMPLETO']))) if x['CEDULA_NEW'] == "" else x['CEDULA_NEW'], axis = 1)
    contactos['CEDULA_NEW'] = contactos['CEDULA_NEW'].apply(lambda x: 0 if x == "" else int(x))
   
    #Cambiar el tipo doc por la codificación
    contactos['TIPO DE DOCUMENTO']=contactos['TIPO DE DOCUMENTO'].apply(lambda x: devolver_llave(dict_tipo_doc,x))

    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    return contactos

##### 4. Demanda #########

def cargar_demanda():
    start_time = time.time()
    print('Inicia carga de demanda')
    #Cargar los datos
    demanda=pd.read_excel(data_path+'Demanda de los servicios.xlsx')
    
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    return demanda

def arreglar_demanda(demanda):
    start_time = time.time()
    print('Inicia arreglo de demanda')
    #Modificar las columnas de demanda
    demanda.columns=[col.upper().strip() for col in demanda.columns]
    
    #eliminar datos de los eventos
    demanda_personas=demanda[['FECHAINSCRIPCIÓN', 'REGIÓN', 'IDENTASISTENTE', 'NOMBREASISTENTE',
           'NITRE', 'RAZÓNSOCIAL', 'CARGO', 'TIPOACTIVIDAD', 'FORMATONOMBRE']]
    
    #Cambiar todo a string
    demanda_personas=demanda_personas.fillna('')
    
    #Cambiar fecha por string
    demanda_personas['FECHAINSCRIPCIÓN']=demanda_personas['FECHAINSCRIPCIÓN'].astype(str)
    demanda_personas['FORMATONOMBRE']=demanda_personas['FORMATONOMBRE'].astype(str)
    
    #agrupar preferencias de la persona
    demanda_personas=demanda_personas.groupby('IDENTASISTENTE').agg(
        {'FECHAINSCRIPCIÓN':','.join,
         'REGIÓN':','.join,
         'NOMBREASISTENTE':'first',
         'NITRE':'first',
         'RAZÓNSOCIAL':'first',
         'CARGO':'first',
         # se propone, pero se puede cambiar
         'TIPOACTIVIDAD':','.join,
         'FORMATONOMBRE':','.join
         }
        ).reset_index()
    
    #Crear la validación de cargos
    demanda_personas['CARGO_HOMOLOGO']=demanda_personas['CARGO'].apply(lambda x: homologar_cargo(dict_cargo,str(x)))
    
    #Cambiar nombre de identifiación
    demanda_personas.rename(columns={'IDENTASISTENTE':'CEDULA'},inplace=True)
    
    
    #Arreglar cédula
    demanda_personas['CEDULA_NEW'] = demanda_personas['CEDULA'].apply(lambda x: "".join(re.findall('\d+', str(x))))
    demanda_personas['CEDULA_NEW'] = demanda_personas.apply(lambda x: "".join(re.findall('\d+', str(x['NOMBREASISTENTE']))) if x['CEDULA_NEW'] == "" else x['CEDULA_NEW'], axis = 1)
    demanda_personas['CEDULA_NEW'] = demanda_personas['CEDULA_NEW'].apply(lambda x: 0 if x == "" else int(x))
       
    
    
    #Agurpar los eventos que se ha asistido
    eventos=demanda[['ID','NOMBRE', 'LUGAR', 'PROYECTO', 'LÍNEA', 'TEMA','REGIÓN']]
    eventos=eventos.drop_duplicates(subset='ID')
    
    #tiempo
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    return demanda,eventos

##################################
### Funciones de carga de data ###
##################################

def cargar_todo():
    #### 1. Data experience ################
    data_exp=generar_data_exp(exp_cols)
    data_exp=arreglar_data_exp(data_exp,exp_cols,dict_cargo)

    #### 2. Temas de interés ###############
    interes=cargar_interes()
    interes=arreglar_interes(interes)

    #### 3. Contactos #####################
    contactos=cargar_contactos()
    contactos=arreglar_contactos(contactos,devolver_llave,dict_tipo_doc)

    #### 4. Demanda #######################

    demanda=cargar_demanda()
    demanda,eventos=arreglar_demanda(demanda)

    return data_exp, interes, contactos, demanda, eventos


##################################
### Funciones de creación de BD ##
##################################

#### Creación de las llaves

## Llave de la bd personas 
# cedulas=[]

# cedulas.extend(list(data_exp['CEDULA_NEW'].unique()))
# cedulas.extend(list(interes['CEDULA_NEW'].unique()))

# cedulas=unique(cedulas)

# ### Creación de los diccionarios

# dict_personas={}
# # Rellenar el diccionario de cédulas
# for ced in cedulas[0:100]:
#     dict_personas[ced]={'INTERES':interes[interes['CEDULA_NEW']==ced].to_dict('records'),
#                         'EXPERIENCIA':data_exp[data_exp['CEDULA_NEW']==ced].to_dict('records')}
    
    
# import sys
# tamano=sys.getsizeof(dict_personas)    


# import math

# def convert_size(size_bytes):
#    if size_bytes == 0:
#        return "0B"
#    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
#    i = int(math.floor(math.log(size_bytes, 1024)))
#    p = math.pow(1024, i)
#    s = round(size_bytes / p, 2)
#    return "%s %s" % (s, size_name[i])
    
# convert_size(tamano)
