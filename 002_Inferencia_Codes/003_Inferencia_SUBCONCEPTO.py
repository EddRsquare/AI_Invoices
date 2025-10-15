# -*- coding: utf-8 -*-
"""
Created on Tue Jul 15 22:43:43 2025

@author: r_rsq
"""

import pandas as pd
import os
import spacy
from tqdm import tqdm

# ---------------- CONFIGURACIÓN --------------------
BASE_PATH = r'C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI'
DATA_PATH = os.path.join(BASE_PATH, 'data')
MODEL_PATH = os.path.join(BASE_PATH, 'model', 'SUBCONCEPTO')
archivo_textos = os.path.join(BASE_PATH, '000_Textos_facturas_BBDD.xlsx')

# ---------------- CARGAMOS MODELO --------------------
print("📦 Cargando modelo NER SUBCONCEPTO...")
nlp = spacy.load(MODEL_PATH)

# ---------------- LEEMOS TEXTO EXTRAÍDO --------------------
print("📥 Cargando archivo de OCR...")
df_textos = pd.read_excel(archivo_textos)

# ---------------- INFERENCIA --------------------
print("🧠 Iniciando inferencia sobre textos...")
resultados = []

for idx, fila in tqdm(df_textos.iterrows(), total=len(df_textos)):
    texto = str(fila['texto_extraido'])
    doc = nlp(texto)

    resultado = {
        'archivo': fila.get('archivo', f"factura_{idx}"),
        'SUBCONCEPTO': None,
        'origen': 'sin_resultado'
    }

    for ent in doc.ents:
        if ent.label_ == 'SUBCONCEPTO':
            resultado['SUBCONCEPTO'] = ent.text
            resultado['origen'] = 'modelo'
            break  # Usamos la primera aparición

    resultados.append(resultado)

# ---------------- EXPORTAMOS --------------------
df_resultado = pd.DataFrame(resultados)
salida = os.path.join(DATA_PATH, 'resultado_inferencia_SUBCONCEPTO.xlsx')
df_resultado.to_excel(salida, index=False)

# ---------------- RESUMEN FINAL --------------------
conteo_origen = df_resultado['origen'].value_counts()

print("\n📊 Resumen de inferencia SUBCONCEPTO:")
print(f"🧠 Por modelo:        {conteo_origen.get('modelo', 0)}")
print(f"❌ Sin resultado:     {df_resultado['SUBCONCEPTO'].isna().sum()}")
print(f"\n✅ Archivo generado: {salida}")
