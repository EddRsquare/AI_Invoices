# -*- coding: utf-8 -*-
"""
Created on Tue Jul 15 23:40:53 2025

@author: r_rsq
"""

# 000_Tagging_TOTALDERECHOS.py

import pandas as pd
import re
import os
from tqdm import tqdm

# -------------------- CONFIGURACIÓN --------------------
file_path = r"C:\Users\r_rsq\Documents\001_AI_Facturas\003_MatrizDatosTotalDerechos.xlsx"
output_path = os.path.join(os.path.dirname(file_path), 'TrainingSet_TOTALDERECHOS.xlsx')

# -------------------- CARGA DE DATOS --------------------
print("📥 Cargando archivo:", file_path)
df = pd.read_excel(file_path)

# Limpieza base
df['texto_extraido'] = df['texto_extraido'].fillna('').astype(str)
df['start'] = None
df['end'] = None
df['valor_detectado'] = None
df['fiabilidad'] = None
df['entidad'] = 'TOTALDERECHOS'

# -------------------- PATRONES --------------------
# Alta fiabilidad
patrones_alta = [
    r"TOTAL\s+DERECHOS",
    r"TOTAL\s+HONORARIOS",
    r"DERECHOS\s+TOTAL",
    r"HONORARIOS\s+TOTAL"
]

# Media fiabilidad
patrones_media = [
    r"DERECHOS\s*[:\-]",
    r"HONORARIOS\s*[:\-]",
    r"HONORARIOS\s+importes?",
    r"DERECHOS\s+importes?",
]

# Baja fiabilidad
patrones_baja = [
    r"DERECHOS",
    r"HONORARIOS"
]

# Patrón numérico
patron_importe = r'([0-9]{1,3}(?:[\.,][0-9]{3})*(?:[\.,][0-9]{2}))'

# -------------------- DETECCIÓN --------------------
print("🔍 Analizando textos y buscando patrones...")

for idx in tqdm(df.index, desc="Procesando facturas", ncols=100):
    texto = df.at[idx, 'texto_extraido']
    
    encontrado = False
    for patron, nivel in [(p, 'alta') for p in patrones_alta] + \
                        [(p, 'media') for p in patrones_media] + \
                        [(p, 'baja') for p in patrones_baja]:
        regex = patron + r"[^\d]{0,10}" + patron_importe
        match = re.search(regex, texto, re.IGNORECASE)
        if match:
            start = match.start(1)
            end = match.end(1)
            valor = match.group(1)

            df.at[idx, 'start'] = start
            df.at[idx, 'end'] = end
            df.at[idx, 'valor_detectado'] = valor
            df.at[idx, 'fiabilidad'] = nivel
            encontrado = True
            break

# -------------------- GUARDADO --------------------
df_resultado = df.dropna(subset=['start', 'end'])

df_resultado.to_excel(output_path, index=False)
print(f"\n✅ Archivo de entrenamiento generado: {output_path}")
print(f"📊 Registros detectados correctamente: {len(df_resultado)} de {len(df)}")
print("ℹ️ Incluye fiabilidad alta, media y baja. Filtra luego como mejor te convenga.")
