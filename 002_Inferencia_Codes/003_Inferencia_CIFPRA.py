# -*- coding: utf-8 -*-
"""
Created on Sat Jul 12 01:35:47 2025

@author: r_rsq
"""

# 000_Inferencia_CIFPRA.py

import pandas as pd
import os
import spacy
import re
from tqdm import tqdm

# ---------------- CONFIGURACIÓN --------------------
BASE_PATH = r'C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI'
DATA_PATH = os.path.join(BASE_PATH, 'data')
MODEL_PATH = os.path.join(BASE_PATH, 'model', 'CIFPRA')
archivo_textos = os.path.join(DATA_PATH, '000_Textos_facturas_BBDD.xlsx')
archivo_salida = os.path.join(DATA_PATH, 'resultado_inferencia_CIFPRA.xlsx')

# ---------------- FUNCIÓN DE RESPALDO --------------------
def extraer_cif_backup(texto):
    patrones = [
        r'\b([A-ZÑ]\s?\d{7,8})\b',
        r'\b([A-ZÑ][-_]\d{7,8})\b',
        r'\b([A-ZÑ]\s?[-_ ]?\s?\d{7,8})\b'
    ]
    for patron in patrones:
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None

# ---------------- CARGA DE MODELO Y DATOS --------------------
print("🧠 Cargando modelo NER CIFPRA...")
nlp = spacy.load(MODEL_PATH)

print("📥 Leyendo archivo OCR:", archivo_textos)
df_textos = pd.read_excel(archivo_textos)

# ---------------- INFERENCIA --------------------
print("🚀 Iniciando inferencia...")

resultados = []

for idx, fila in tqdm(df_textos.iterrows(), total=len(df_textos), desc="Procesando facturas", ncols=100):
    texto = str(fila['texto_extraido'])
    doc = nlp(texto)

    resultado = {
        'archivo': fila.get('archivo', f"factura_{idx}"),
        'CIFPRA': None,
        'origen': 'sin_resultado'
    }

    # Buscar con modelo
    for ent in doc.ents:
        if ent.label_ == 'CIFPRA':
            resultado['CIFPRA'] = ent.text.strip()
            resultado['origen'] = 'modelo'
            break

    # Backup por patrón
    if resultado['CIFPRA'] is None:
        cif_backup = extraer_cif_backup(texto)
        if cif_backup:
            resultado['CIFPRA'] = cif_backup
            resultado['origen'] = 'backup'

    resultados.append(resultado)

# ---------------- EXPORTAMOS --------------------
df_resultado = pd.DataFrame(resultados)
df_resultado.to_excel(archivo_salida, index=False)

# ---------------- RESUMEN FINAL --------------------
conteo_origen = df_resultado['origen'].value_counts()
modelo = conteo_origen.get('modelo', 0)
backup = conteo_origen.get('backup', 0)
sin_res = df_resultado['CIFPRA'].isna().sum()

print("\n📊 Resumen de inferencia CIFPRA:")
print(f"🧠 Por modelo:        {modelo}")
print(f"🛠️  Por regla backup: {backup}")
print(f"❌ Sin resultado:     {sin_res}")
print("\n✅ Archivo generado:", archivo_salida)
