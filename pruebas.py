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

def cargar_cuentas():

    start_time = time.time()
    print('Inicia carga de cuentas')

    # Cargar los datos
    cuentas = pd.read_excel(data_path+'Cuentas.xlsx')
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))

    # Tabla de homologación de los tipos de documento de la base de Cuentas
    dict_tipo_doc_new = {
        'C': 1,
        'Cedula de ciudadanía': 1,
        'NIT': 5,
        'Pasaporte': 3,
        'Empresa Extranjera': 6,
        'Cedula de extranjería': 2,
        'Tarjeta de identidad': 4,
    }

    # Creación del dataframe desde el diccionario
    dict_tipo_doc_new = pd.DataFrame.from_dict([dict_tipo_doc_new]).T.reset_index()
    dict_tipo_doc_new.columns = ['Tipo de documento', 'Tipo_documento_value']

    # Para realizar la corrección en el dataframe original
    cuentas = cuentas.merge(
        dict_tipo_doc_new, on="Tipo de documento", how="left")
    cuentas['Tipo_documento_value'] = cuentas['Tipo_documento_value'].fillna(
        1)
    cuentas['Tipo_documento_value'] = cuentas['Tipo_documento_value'].astype(
        'int')

    # Para corregir las cédulas
    cuentas['CEDULA_NEW'] = cuentas['Número de documento'].apply(
        lambda x: "".join(re.findall('\d+', str(x))))
    cuentas['CEDULA_NEW'] = cuentas['CEDULA_NEW'].apply(
        lambda x: 0 if x == "" else int(x))
    cuentas = cuentas[cuentas['CEDULA_NEW'] != 0]

    # Para corregir la razón social y el nombre comercial
    cuentas['Razón Social'] = cuentas['Razón Social'].apply(
        lambda x: x.upper() if isinstance(x, str) else "NO ESPECIFICADO")
    cuentas['Nombre comercial'] = np.where(pd.isnull(
        cuentas['Nombre comercial']), cuentas['Razón Social'], cuentas['Nombre comercial'].str.upper())

    # Para corregir variables con valores Sin definir
    not_defined_vars = ['Naturaleza',
                        'TPCM',
                        'Tamaño',
                        'Es proponente',
                        'Representante legal',
                        'Empresa activa en Cámara',
                        'Ciudad dirección comercial',
                        'Departamento dirección comercial',
                        'Cámara de comercio/Regional',
                        'Ciudad  dirección notificación judicial',
                        'Departamento dirección notificación judicial',
                        'Dirección notificación judicial'
    ]

    for col in not_defined_vars:
        cuentas[col] = np.where(
            pd.isnull(cuentas[col]), "Sin definir", cuentas[col])


    # Para crear variables binarias con las que lo permiten
    dummy_vars = ["Es afiliado",
                "¿Empresa miembro de junta directiva?",
                "Clúster VIP",
                "Es empresa de familia",
                "Exporta",
                "Importa?",
                "Pertenece a RNT",
                "Es proveedor CCMA"]

    for col in dummy_vars:
        cuentas[col] = cuentas[col].apply(
            lambda x: 0 if x == "No" else 1)

    # Para convertir los números decimales en fechas (date)
    date_cols = ['Fecha de matrícula', 'Fecha de última renovación']

    for col in date_cols:
        cuentas[col + "_new"] = cuentas[col].fillna("")
        cuentas[col + "_new"] = cuentas[col + "_new"].apply(lambda x: datetime(*xlrd.xldate_as_tuple(x, 0)).date() if x != "" else "")

    # Para eliminar columnas sobrantes
    cols_to_drop = [
        "No. sucursales en Colombia",
        "No. Sucursales en el exterior",
        'Número de documento'
    ]
    cuentas.drop(cols_to_drop + date_cols, axis=1, inplace=True)

    print(f"Tiempo transcurrido: {time_lapse}")

    return cuentas

data_cuentas = cargar_cuentas()
data_cuentas.to_excel(os.getcwd() + "\\data\\Cuentas_corregida.xlsx", index = False)