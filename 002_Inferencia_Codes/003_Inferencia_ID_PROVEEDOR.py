# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 14:55:34 2025

@author: r_rsq
"""

import pandas as pd
import os
import spacy
from tqdm import tqdm

# ---------------- CONFIGURACIÓN --------------------
BASE_PATH = r'C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI'
DATA_PATH = os.path.join(BASE_PATH, 'data')
MODEL_PATH = os.path.join(BASE_PATH, 'model', 'ID_PROVEEDOR')

# ---------------- CARGAMOS MODELO --------------------
print("🧠 Cargando modelo NER ID_PROVEEDOR...")
nlp = spacy.load(MODEL_PATH)

# ---------------- LEEMOS TEXTO EXTRAÍDO --------------------
archivo_textos = os.path.join(DATA_PATH, '000_Textos_facturas_BBDD.xlsx')
df_textos = pd.read_excel(archivo_textos)

# ---------------- INFERENCIA --------------------
print("🔎 Iniciando inferencia sobre textos...")
resultados = []

for idx, fila in tqdm(df_textos.iterrows(), total=len(df_textos)):
    texto = str(fila['texto_extraido'])
    doc = nlp(texto)

    resultado = {
        'archivo': fila.get('archivo', f"factura_{idx}"),
        'ID_PROVEEDOR': None,
        'origen': 'sin_resultado'
    }

    for ent in doc.ents:
        if ent.label_ == 'ID_PROVEEDOR':
            resultado['ID_PROVEEDOR'] = ent.text.strip()
            resultado['origen'] = 'modelo'
            break

    resultados.append(resultado)

# ---------------- EXPORTAMOS --------------------
df_resultado = pd.DataFrame(resultados)
salida = os.path.join(DATA_PATH, 'resultado_ID_PROVEEDOR.xlsx')
df_resultado.to_excel(salida, index=False)

# ---------------- RESUMEN FINAL --------------------
conteo_origen = df_resultado['origen'].value_counts()
modelo = conteo_origen.get('modelo', 0)
nulo = df_resultado['ID_PROVEEDOR'].isna().sum()

print("\n📊 Resumen de inferencia ID_PROVEEDOR:")
print(f"🧠 Por modelo:        {modelo}")
print(f"❌ Sin resultado:     {nulo}")
print("📄 Archivo generado:", salida)
