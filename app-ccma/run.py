# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from flask_migrate import Migrate
from sys import exit
from decouple import config

from flask import render_template, redirect, request, url_for, Flask, flash, redirect
from werkzeug.utils import secure_filename

UPLOAD_FOLDER = '/path/to/the/uploads'
ALLOWED_EXTENSIONS = {'csv'}


import pandas as pd

from apps.config import config_dict
from apps import create_app, db

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
data=pd.read_csv('data_pr2.csv')




    
################################
### Empiezan funciones app #####
################################

#Ejemplo de enviar texto al html
# @app.route('/recomendation') #página a la que se debe enviar
# def recomendation():
#     names_of_instructors = ["Qué", "hace", "mi rey"]
#     random_name = "Qué hace mi rey"
#     return render_template('/home/recomendation.html', names=names_of_instructors, name=random_name) #render de las variables


# Función para ejecutar el sistema de recomendación
@app.route("/recomendation", methods=['GET', 'POST'])
def index():
    colours = ['Ev1', 'Ev2', 'Ev3', 'Ev4','Ev5','Ev6','Ev7','Ev8','Ev9']
    errors = []
    results = {}
    describe=data.describe(include='all').fillna('').reset_index()
    
    
    if request.method == "POST":
        # get url that the user has entered
        try:
            results = request.form['cantidad']
            eventos = request.form['eventos']
            contacto = request.form['contactos']
            intereses = request.form['interes']
            filter = request.form['search_filter']
            results=[results,eventos,contacto,intereses,filter]
            print(eventos)
        except:
            errors.append(
                "Unable to get URL. Please make sure it's valid and try again."
            )
    return render_template('/home/recomendation.html', 
                           errors=errors, 
                           results=results, 
                           colours=colours,
                           column_names=data.columns.values, 
                           #Categorizar el RMSE
                           row_data=list(data.values.tolist()), 
                           column_names_desc=describe.columns.values, 
                           row_data_desc=list(describe.values.tolist()),
                           zip=zip)

## FUnción para cargar tablas
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
    id={}
    nombre_cliente=''
    edad=''
    correo_cliente=''
    tel_cliente=''
    dir_cliente=''
    cargo_cliente=''
    empresa_cliente=''
    
    p_experiencia=''
    p_llamada=''
    p_cuentas=''
    p_demanda=''
    p_interes=''
    p_relacion=''
    
    intereses=[]
    if request.method == "POST":
        # get url that the user has entered
        try:
            id = request.form['buscar']
            nombre_cliente='Pedro Perez'
            edad=35
            correo_cliente='pperez@gmail.com'
            tel_cliente='3172994422'
            dir_cliente='Dg. 32 #33a Sur-96, Envigado, Antioquia'
            cargo_cliente='Representante legal'
            empresa_cliente='Empaques plásticos enviagado SA'
            
            p_experiencia=25
            p_llamada=15
            p_cuentas=45
            p_demanda=13
            p_interes=5
            p_relacion=1
            
            intereses=["Gobierno corporativo",
                        "Internacionalización",
                        "Jurídica",
                        "Desarrollo del ser",
                        "Contabilidad y finanzas"]
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
