# -*- coding: utf-8 -*-
"""
Created on Tue Jul 15 23:49:59 2025

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
MODEL_PATH = os.path.join(BASE_PATH, 'model', 'TOTALDERECHOS')

# ---------------- FUNCIÓN DE RESPALDO --------------------
def extraer_total_derechos_backup(texto):
    patrones = [
        r'TOTAL\s+DERECHOS[^\d]{0,10}([\d.,]+)',
        r'DERECHOS\s+TOTAL[^\d]{0,10}([\d.,]+)',
        r'DERECHOS\s+[^\d]{0,10}([\d.,]+)'
    ]
    for patron in patrones:
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            valor = match.group(1).replace('.', '').replace(',', '.')
            try:
                return float(valor)
            except ValueError:
                continue
    return None

# ---------------- CARGAMOS MODELO --------------------
print("🔍 Cargando modelo TOTALDERECHOS...")
nlp = spacy.load(MODEL_PATH)

# ---------------- LEEMOS NUEVO OCR --------------------
archivo_textos = os.path.join(DATA_PATH, '000_Textos_facturas_BBDD.xlsx')
df_textos = pd.read_excel(archivo_textos)

# ---------------- INFERENCIA --------------------
print("🚀 Iniciando inferencia TOTALDERECHOS...")
resultados = []

for idx, fila in tqdm(df_textos.iterrows(), total=len(df_textos), desc="Procesando facturas", ncols=100):
    texto = str(fila['texto_extraido'])
    doc = nlp(texto)

    resultado = {
        'archivo': fila.get('archivo', f"factura_{idx}"),
        'TOTALDERECHOS': None,
        'origen': 'sin_resultado'
    }

    for ent in doc.ents:
        if ent.label_ == 'TOTALDERECHOS':
            resultado['TOTALDERECHOS'] = ent.text
            resultado['origen'] = 'modelo'
            break

    if resultado['TOTALDERECHOS'] is None:
        total_backup = extraer_total_derechos_backup(texto)
        if total_backup is not None:
            resultado['TOTALDERECHOS'] = total_backup
            resultado['origen'] = 'backup'

    resultados.append(resultado)

# ---------------- EXPORTAMOS --------------------
df_resultado = pd.DataFrame(resultados)
salida = os.path.join(DATA_PATH, 'resultado_inferencia_TOTALDERECHOS.xlsx')
df_resultado.to_excel(salida, index=False)

# ---------------- RESUMEN FINAL --------------------
conteo_origen = df_resultado['origen'].value_counts()

resultado_modelo = conteo_origen.get('modelo', 0)
resultado_backup = conteo_origen.get('backup', 0)
resultado_nulo = df_resultado['TOTALDERECHOS'].isna().sum()

print("\n📊 Resumen de inferencia TOTALDERECHOS:")
print(f"🧠 Por modelo:        {resultado_modelo}")
print(f"🛠️  Por regla backup: {resultado_backup}")
print(f"❌ Sin resultado:     {resultado_nulo}")
print("\n✅ Archivo generado:", salida)
