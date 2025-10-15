# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 11:15:21 2025

@author: r_rsq
"""

import pandas as pd
import os
import spacy
import re
from tqdm import tqdm

# ---------------- CONFIGURACIÓN --------------------
BASE_PATH = r'C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI'
DATA_PATH = os.path.join(BASE_PATH, 'data')
MODEL_PATH = os.path.join(BASE_PATH, 'model', 'IGIC')
archivo_textos = os.path.join(DATA_PATH, '000_Textos_facturas_BBDD.xlsx')
salida = os.path.join(DATA_PATH, 'resultado_inferencia_IGIC.xlsx')

# ---------------- CARGAMOS MODELO --------------------
print("🔍 Cargando modelo NER para IGIC...")
nlp = spacy.load(MODEL_PATH)

# ---------------- LECTURA DEL TEXTO --------------------
df_textos = pd.read_excel(archivo_textos)
print(f"📄 Total facturas cargadas: {len(df_textos)}")

# ---------------- FUNCIÓN BACKUP --------------------
def extraer_igic_backup(texto):
    patrones = [
        r"(\d{1,2}[.,]\d{2})\s*%?\s*de\s*I[\s\.]?G[\s\.]?I[\s\.]?C\.?\s*(\d{1,3}(?:[.,]\d{3})*[.,]\d{2})",
        r"I[\s\.]?G[\s\.]?I[\s\.]?C\.?\s*(\d{1,3}(?:[.,]\d{3})*[.,]\d{2})",
        r"I[\s\.]?G[\s\.]?I[\s\.]?C\.?.{0,40}(\d{1,3}(?:[.,]\d{3})*[.,]\d{2})"
    ]
    for patron in patrones:
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            try:
                for g in reversed(match.groups()):
                    if g and re.match(r"\d{1,3}(?:[.,]\d{3})*[.,]\d{2}", g):
                        return g
            except:
                continue
    return None

# ---------------- INFERENCIA --------------------
print("🤖 Ejecutando inferencia IGIC...")
resultados = []

for idx, fila in tqdm(df_textos.iterrows(), total=len(df_textos)):
    texto = str(fila['texto_extraido'])
    doc = nlp(texto)

    resultado = {
        'archivo': fila.get('archivo', f"factura_{idx}"),
        'IGIC': None,
        'origen': 'sin_resultado'
    }

    # Verificamos si hay presencia textual de IGIC
    contiene_igic = bool(re.search(r"I[\s\.]?G[\s\.]?I[\s\.]?C", texto, re.IGNORECASE))

    if contiene_igic:
        for ent in doc.ents:
            if ent.label_ == 'IGIC':
                resultado['IGIC'] = ent.text
                resultado['origen'] = 'modelo'
                break

        if resultado['IGIC'] is None:
            igic_backup = extraer_igic_backup(texto)
            if igic_backup is not None:
                resultado['IGIC'] = igic_backup
                resultado['origen'] = 'backup'
    else:
        resultado['IGIC'] = '0,00'
        resultado['origen'] = 'no_igic'

    resultados.append(resultado)

# ---------------- EXPORTACIÓN --------------------
df_resultado = pd.DataFrame(resultados)
df_resultado.to_excel(salida, index=False)

# ---------------- RESUMEN --------------------
conteo = df_resultado['origen'].value_counts()
print("\n📊 Resumen inferencia IGIC:")
print(f"🧠 Modelo:   {conteo.get('modelo', 0)}")
print(f"🛠️  Backup:  {conteo.get('backup', 0)}")
print(f"🚫 Sin IGIC: {conteo.get('no_igic', 0)}")
print(f"❌ Nulos:    {df_resultado['IGIC'].isna().sum()}")
print(f"📁 Archivo generado: {salida}")
