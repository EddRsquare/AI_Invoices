# -*- coding: utf-8 -*-
"""
Created on Mon Jul 21 11:38:52 2025

@author: r_rsq
"""

import spacy
import pandas as pd
import os
from tqdm import tqdm
from dateutil import parser

# -------------------- CONFIGURACIÓN --------------------
BASE_PATH = r'C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI'
DATA_PATH = os.path.join(BASE_PATH, 'data')
MODEL_PATH = os.path.join(BASE_PATH, 'model', 'FechaFactura')
archivo_textos = os.path.join(DATA_PATH, '000_Textos_facturas_BBDD.xlsx')
salida = os.path.join(DATA_PATH, 'resultado_inferencia_FECHAFACTURA.xlsx')

# -------------------- CARGAMOS MODELO --------------------
print("📦 Cargando modelo NER para FechaFactura...")
nlp = spacy.load(MODEL_PATH)

# -------------------- LECTURA DE DATOS --------------------
df_textos = pd.read_excel(archivo_textos)
print(f"📄 Facturas cargadas: {len(df_textos)}")

# -------------------- FUNCIÓN DE PARSEO --------------------
def convertir_a_fecha(texto):
    try:
        return parser.parse(texto, dayfirst=True).date()
    except:
        return None

# -------------------- INFERENCIA --------------------
print("🤖 Ejecutando inferencia FechaFactura...")
resultados = []

for idx, fila in tqdm(df_textos.iterrows(), total=len(df_textos)):
    texto = str(fila['texto_extraido'])
    doc = nlp(texto)

    fecha_detectada = None
    formato_fecha = None
    estado = 'no_detectada'

    for ent in doc.ents:
        if ent.label_ == 'FechaFactura':
            fecha_detectada = ent.text.strip()
            formato_fecha = convertir_a_fecha(fecha_detectada)
            estado = 'convertida' if formato_fecha else 'no_convertible'
            break

    resultados.append({
        'archivo': fila.get('archivo', f'factura_{idx}'),
        'FechaFactura': fecha_detectada,
        'FormatoFecha': formato_fecha,
        'EstadoConversion': estado  # Opcional: para trazabilidad
    })

# -------------------- EXPORTACIÓN --------------------
df_resultado = pd.DataFrame(resultados)
df_resultado.to_excel(salida, index=False)

# -------------------- RESUMEN --------------------
print("\n✅ Resultado guardado en:", salida)
print(f"🧠 Fechas detectadas: {df_resultado['FechaFactura'].notna().sum()}")
print(f"📅 Fechas convertidas correctamente: {df_resultado['FormatoFecha'].notna().sum()}")
print(f"⚠️ Fechas no convertibles: {(df_resultado['FormatoFecha'].isna() & df_resultado['FechaFactura'].notna()).sum()}")
