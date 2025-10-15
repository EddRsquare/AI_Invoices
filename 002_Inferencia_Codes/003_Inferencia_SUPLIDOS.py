# -*- coding: utf-8 -*-
"""
Created on Wed Jul 16 00:35:25 2025

@author: r_rsq
"""

import pandas as pd
import os
import spacy
from tqdm import tqdm

# ---------------- CONFIGURACIÓN --------------------
BASE_PATH = r'C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI'
DATA_PATH = os.path.join(BASE_PATH, 'data')
MODEL_PATH = os.path.join(BASE_PATH, 'model', 'SUPLIDOS')
archivo_textos = os.path.join(DATA_PATH, '000_Textos_facturas_BBDD.xlsx')  # Asegúrate que este es el correcto

# ---------------- CARGAMOS MODELO --------------------
print("📦 Cargando modelo NER SUPLIDOS...")
nlp = spacy.load(MODEL_PATH)

# ---------------- LEEMOS NUEVO OCR --------------------
print("📥 Cargando archivo de textos...")
df_textos = pd.read_excel(archivo_textos)

# ---------------- INFERENCIA --------------------
print("🔍 Iniciando inferencia con modelo NER SUPLIDOS...")
resultados = []

for idx, fila in tqdm(df_textos.iterrows(), total=len(df_textos)):
    texto = str(fila['texto_extraido'])
    doc = nlp(texto)

    resultado = {
        'archivo': fila.get('archivo', f"factura_{idx}"),
        'SUPLIDOS': None,
        'origen': 'sin_resultado'
    }

    for ent in doc.ents:
        if ent.label_ == 'SUPLIDOS':
            resultado['SUPLIDOS'] = ent.text
            resultado['origen'] = 'modelo'
            break  # Salta en cuanto lo encuentre

    resultados.append(resultado)

# ---------------- EXPORTAMOS --------------------
df_resultado = pd.DataFrame(resultados)
salida = os.path.join(DATA_PATH, 'resultado_inferencia_SUPLIDOS.xlsx')
df_resultado.to_excel(salida, index=False)

# ---------------- RESUMEN FINAL --------------------
conteo_origen = df_resultado['origen'].value_counts()

resultado_modelo = conteo_origen.get('modelo', 0)
resultado_nulo = df_resultado['SUPLIDOS'].isna().sum()

print("\n📊 Resumen de inferencia SUPLIDOS:")
print(f"🧠 Por modelo:        {resultado_modelo}")
print(f"❌ Sin resultado:     {resultado_nulo}")
print("\n✅ Inferencia completada y archivo generado:", salida)




import pandas as pd
import os
import spacy
import re
from tqdm import tqdm

# ---------------- CONFIGURACIÓN --------------------
BASE_PATH = r'C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI'
DATA_PATH = os.path.join(BASE_PATH, 'data')
MODEL_PATH = os.path.join(BASE_PATH, 'model', 'SUPLIDOS')

# ---------------- FUNCIÓN DE DETECCIÓN DE FECHAS --------------------
def es_fecha(texto):
    texto = texto.strip()
    # Formatos comunes como 12/05/2023 o 12-05-23 o 12.05.2023
    patrones_fecha = [
        r"\b\d{1,2}[-/\.]\d{1,2}[-/\.]\d{2,4}\b",
        r"\b\d{4}[-/\.]\d{1,2}[-/\.]\d{1,2}\b"
    ]
    for patron in patrones_fecha:
        if re.search(patron, texto):
            return True
    return False

# ---------------- CARGAMOS MODELO --------------------
print("🧠 Cargando modelo SUPLIDOS...")
nlp = spacy.load(MODEL_PATH)

# ---------------- LEEMOS FACTURAS --------------------
archivo_textos = os.path.join(BASE_PATH, '000_Textos_facturas_BBDD.xlsx')
df_textos = pd.read_excel(archivo_textos)

# ---------------- INFERENCIA --------------------
print("🚀 Iniciando inferencia SUPLIDOS con filtro de fechas...")
resultados = []

for idx, fila in tqdm(df_textos.iterrows(), total=len(df_textos)):
    texto = str(fila['texto_extraido'])
    doc = nlp(texto)

    resultado = {
        'archivo': fila.get('archivo', f"factura_{idx}"),
        'SUPLIDOS': None,
        'origen': 'sin_resultado'
    }

    for ent in doc.ents:
        if ent.label_ == 'SUPLIDOS':
            valor = ent.text.strip()
            if not es_fecha(valor):  # ✅ Filtramos si parece una fecha
                resultado['SUPLIDOS'] = valor
                resultado['origen'] = 'modelo'
                break

    resultados.append(resultado)

# ---------------- EXPORTAMOS --------------------
df_resultado = pd.DataFrame(resultados)
salida = os.path.join(DATA_PATH, 'resultado_inferencia_SUPLIDOS.xlsx')
df_resultado.to_excel(salida, index=False)

# ---------------- RESUMEN --------------------
conteo = df_resultado['origen'].value_counts()
print("\n📊 Resumen SUPLIDOS (con filtro fechas):")
print(f"🧠 Modelo: {conteo.get('modelo', 0)}")
print(f"❌ Sin resultado: {conteo.get('sin_resultado', 0)}")
print("\n✅ Archivo generado:", salida)