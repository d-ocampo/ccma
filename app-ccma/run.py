# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from pymongo import MongoClient
import pymongo
from apps import create_app, db
from apps.config import config_dict
import pandas as pd
from flask_migrate import Migrate
from sys import exit
from decouple import config

from flask import render_template, redirect, request, url_for, Flask, flash, redirect
from werkzeug.utils import secure_filename

from surprise import Dataset
from surprise import Reader

import joblib

# Módulos de la app
from data_load import cargar_todo, conectar_colection_mongo_ccma, cargar_interes, arreglar_interes, cargar_demanda, arreglar_demanda, carga_llamadas, arreglo_llamadas, nulls_filter
from recomendationsystems import recomendacion_cf, recomendacion_coseno, volver_cat_prediccion


# Modelos entrenados
# slope one cf
model_cf_e_e = joblib.load('models/model_modelo_evento_evento.pkl')
# idf interes
idf_eventos_int = joblib.load('models/model_tfdidf interes - evento.pkl')
# idf llamadas
idf_eventos_llam = joblib.load('models/model_tfdidf llamada - evento.pkl')


# Para cargar el conector con mongo

UPLOAD_FOLDER = '/path/to/the/uploads'
ALLOWED_EXTENSIONS = {'csv'}


# WARNING: Don't run with debug turned on in production!
DEBUG = config('DEBUG', default=True, cast=bool)

# The configuration
get_config_mode = 'Debug' if DEBUG else 'Production'

try:

    # Load the configuration using the default values
    app_config = config_dict[get_config_mode.capitalize()]

except KeyError:
    exit('Error: Invalid <config_mode>. Expected values [Debug, Production] ')

app = create_app(app_config)
Migrate(app, db)

if DEBUG:
    app.logger.info('DEBUG       = ' + str(DEBUG))
    app.logger.info('Environment = ' + get_config_mode)
    app.logger.info('DBMS        = ' + app_config.SQLALCHEMY_DATABASE_URI)

################################
#### Data de la ccma ###########
################################
data = pd.read_csv('data_pr2.csv')
# Datos planos
# Cargar bases
# data_exp, interes, contactos, demanda, eventos, llamada, cuentas = cargar_todo(
#     20)

n = 20

interes = cargar_interes()
interes = arreglar_interes(interes)
interes = nulls_filter(n, interes)


#### 4. Demanda #######################

demanda = cargar_demanda()
demanda, eventos = arreglar_demanda(demanda)
demanda = nulls_filter(n, demanda)


#### 5. Llamadas ######################
llamada = carga_llamadas()
llamada = arreglo_llamadas(llamada)
llamada = nulls_filter(n, llamada)


# Cruzar nombre de eventos con ID
# Establecer a los eventos que han ido
dem_base = demanda[['CEDULA_NEW', 'ID']]
# Pasos para fusionar la base
# Crear cada asistencia a lista
dem_base_melt = dem_base.assign(ID=dem_base.ID.str.split(",")).sample(10000)

# Separa el listado a filas
dem_base_melt = dem_base_melt.ID.apply(pd.Series) \
    .merge(dem_base_melt, right_index=True, left_index=True) \
    .drop(["ID"], axis=1) \
    .melt(id_vars=['CEDULA_NEW'], value_name="ID") \
    .drop('variable', axis=1)

# Variable binaria de asistencia
dem_base_melt['ASISTENCIA'] = dem_base_melt['ID'].apply(
    lambda x: 0 if pd.isna(x) else 1)
# # Crear base para el SR
demanda_base = dem_base_melt.rename(columns={'CEDULA_NEW': 'userID',
                                             'ID': 'itemID',
                                             'ASISTENCIA': 'rating'})

# # Borrar la base para no consumir tanta memoria
del(dem_base_melt)

# ### eventos - llamadas
llam_base = pd.merge(demanda, llamada, on=['CEDULA_NEW'], how="inner")
llam_base = llam_base.dropna(subset=['ID', 'TEMA DEL SERVICIO1'])


### eventos - intereses
int_base = pd.merge(demanda, interes, on=['CEDULA_NEW'], how="inner")
# Eliminar repetidos
int_base = int_base.drop_duplicates(subset='CEDULA_NEW')


##################################
#### Sistemas de recomendación ###
##################################

###### Eventos - eventos
####
# Escala binaria
reader = Reader(rating_scale=(0, 1))
# obtener la base como se necesita
data_e_e = Dataset.load_from_df(
    demanda_base[['userID', 'itemID', 'rating']], reader)

# Interés eventos
####
# resumir la base por evento-interés
int_base = int_base.dropna(subset=['TEMAS_INTERES', 'ID'])
int_base_g = int_base[['CEDULA_NEW', 'ID', 'TEMAS_INTERES']]


# Conectar la base de datos
# Base en mongo atlas


def conectar_colection_mongo_ccma(coleccion, base):
    if base == 1:
        # client = pymongo.MongoClient('hostname', 27017)
        client = pymongo.MongoClient('mongodb://localhost:27017')
    else:
        client = pymongo.MongoClient(
            "mongodb+srv://proyecto_uniandes:WpyATG4YVumTaPEd@cluster0.66yl3.mongodb.net/ccma?retryWrites=true&w=majority")
    # Cargar la base de ccma
    db = client['ccma']
    # cargar la colección elegida
    collection = db[coleccion]
    return collection

# Conectar la base de datos


# Conectar la colección elegida
clientes_col = conectar_colection_mongo_ccma('clientes', 1)
eventos_col = conectar_colection_mongo_ccma('eventos', 1)

# DIccionario de eventos
dic_eventos = {doc['ID']: doc['NOMBRE']
               for doc in eventos_col.find({'ID': {'$ne': ''}})}
tup_eventos = [(doc['ID'], doc['NOMBRE'])
               for doc in eventos_col.find({'ID': {'$ne': ''}})]


################################
### Empiezan funciones app #####
################################
# funcion para vacios
def buscar(query):
    try:
        resultado = query
    except:
        resultado = 'nan'
    return resultado

# Ejemplo de enviar texto al html
# @app.route('/recomendation') #página a la que se debe enviar
# def recomendation():
#     names_of_instructors = ["Qué", "hace", "mi rey"]
#     random_name = "Qué hace mi rey"
#     return render_template('/home/recomendation.html', names=names_of_instructors, name=random_name) #render de las variables

# Función para el dash  board


@app.route("/index")
def index():
    clientes = 3423542

    # #Para obtener la cantidad de clientes
    # response = clientes_col.find({'CEDULA_NEW': {'$ne': ''}})
    # clientes = len([doc for doc in response])
    # print(clientes)
    return render_template('/home/index.html', clientes=clientes)


# Función para ejecutar el sistema de recomendación
@app.route("/recomendation", methods=['GET', 'POST'])
def recomendation():
    colours = tup_eventos
    errors = []
    results = {}
    describe = data.describe(
        include='all').fillna('').reset_index()
    df_cf = pd.DataFrame()

    if request.method == "POST":
        # get url that the user has entered

        results = request.form['cantidad']
        filter = request.form['search_filter']

        # eventos = request.form['eventos']
        # contacto = request.form['contactos']
        # intereses = request.form['interes']

        if request.form.get('eventos'):
            print('entro a eventos')
            n = results
            df_cf, most = recomendacion_cf(
                data_e_e, model_cf_e_e, filter, n)
            df_cf['CORREO'] = df_cf['CEDULA_NEW'].apply(lambda x: clientes_col.find_one({'identificación': '1-'+str(x)})[
                'características']['EXPERIENCIA'][0]['CORREO_ELECTRONICO'])
            df_cf['TELEFONO'] = df_cf['CEDULA_NEW'].apply(lambda x: clientes_col.find_one({'identificación': '1-'+str(x)})[
                'características']['EXPERIENCIA'][0]['TELEFONO'])
            df_cf['CLASIFICACION'] = df_cf['SCORE'].apply(
                lambda x: volver_cat_prediccion(x, 0.5, False))
            print(len(df_cf))
            describe = df_cf.describe(
                include='all').fillna('').reset_index()

        # except:
        #     errors.append(
        #         "Unable to get URL. Please make sure it's valid and try again."
        #     )
    return render_template('/home/recomendation.html',
                           errors=errors,
                           #    results=results,
                           colours=colours,
                           column_names=df_cf.columns.values,
                           # Categorizar el RMSE
                           row_data=list(df_cf.values.tolist()),
                           column_names_desc=describe.columns.values,
                           row_data_desc=list(describe.values.tolist()),
                           zip=zip)

# FUnción para cargar tablas
# Descomentar con los comentarios de la ccma
# @app.route('/metadata', methods = ['GET', 'POST'])
# def upload_file():
#     if request.method == 'POST':
#         f = request.files['file']
#         f.save(secure_filename(f.filename))
#         return render_template('/home/metadata.html')
#     else:
#         return render_template('/home/metadata.html')

# Función para ejecutar el sistema de recomendación


@app.route("/search", methods=['GET', 'POST'])
def search():
    errors = []
    results = {}
    id = {}
    nombre_cliente = ''
    edad = ''
    correo_cliente = ''
    tel_cliente = ''
    dir_cliente = ''
    cargo_cliente = ''
    empresa_cliente = ''

    p_experiencia = ''
    p_llamada = ''
    p_cuentas = ''
    p_demanda = ''
    p_interes = ''
    p_relacion = ''

    intereses = []
    if request.method == "POST":
        # get url that the user has entered
        try:
            id = request.form['buscar']
            iden = '1-'+str(id)
            print(id)

            contacto = clientes_col.find_one({'identificación': iden})[
                'características']['CONTACTOS'][0]
            experiencia = clientes_col.find_one({'identificación': iden})[
                'características']['EXPERIENCIA']

            nombre_cliente = buscar(contacto['NOMBRE COMPLETO'])

            edad = ''
            correo_cliente = buscar(experiencia[0]['CORREO_ELECTRONICO'])
            tel_cliente = buscar(experiencia[0]['TELEFONO'])
            dir_cliente = buscar(contacto['DIRECCIÓN'])
            cargo_cliente = buscar(experiencia[0]['CARGO_HOMOLOGO']
                                   )
            empresa_cliente = buscar(experiencia[0]['EMPRESA'])

            p_experiencia = 25
            p_llamada = 15
            p_cuentas = 45
            p_demanda = 13
            p_interes = 5
            p_relacion = 1

            intereses = buscar(clientes_col.find_one({'identificación': identificacion})[
                               'características']['INTERES'][0]['TEMAS_INTERES'].split(','))
        except:
            errors.append(
                "Unable to get URL. Please make sure it's valid and try again."
            )
    return render_template('/home/search.html',
                           errors=errors,
                           results=results,
                           id=id,
                           nombre_cliente=nombre_cliente,
                           edad=edad,
                           correo_cliente=correo_cliente,
                           tel_cliente=tel_cliente,
                           dir_cliente=dir_cliente,
                           cargo_cliente=cargo_cliente,
                           empresa_cliente=empresa_cliente,
                           p_experiencia=p_experiencia,
                           p_llamada=p_llamada,
                           p_cuentas=p_cuentas,
                           p_demanda=p_demanda,
                           p_interes=p_interes,
                           p_relacion=p_relacion,
                           intereses=intereses)


if __name__ == "__main__":
    app.run()
