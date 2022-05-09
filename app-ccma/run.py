# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from flask_migrate import Migrate
from sys import exit
from decouple import config

from flask import render_template, redirect, request, url_for

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
data=pd.read_csv('data_pr.csv')




    
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
    colours = ['Red', 'Blue', 'Black', 'Orange']
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
                           row_data=list(data.values.tolist()), 
                           column_names_desc=describe.columns.values, 
                           row_data_desc=list(describe.values.tolist()),
                           zip=zip)




if __name__ == "__main__":
    app.run()
