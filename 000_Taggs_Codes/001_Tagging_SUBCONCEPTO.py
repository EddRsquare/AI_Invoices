# -*- coding: utf-8 -*-
"""
Created on Tue Jul 15 17:28:05 2025

@author: r_rsq
"""
# -*- coding: utf-8 -*-
"""
Tagging SUBCONCEPTO desde textos OCR de facturas
"""
import pandas as pd
import re
import os
from tqdm import tqdm

# -------------------- CONFIGURACIÓN --------------------
file_path = r"C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI\data\000_Textos_facturas_BBDD_v3.xlsx"
output_path = os.path.join(os.path.dirname(file_path), 'TrainingSet_SUBCONCEPTO.xlsx')

# -------------------- CARGA DE DATOS --------------------
print("📥 Cargando archivo:", file_path)
df = pd.read_excel(file_path)
df['texto_extraido'] = df['texto_extraido'].fillna('').astype(str)

# Inicialización de columnas
df['start'] = None
df['end'] = None
df['valor_detectado'] = None
df['fiabilidad'] = None
df['entidad'] = 'SUBCONCEPTO'

# -------------------- PATRONES --------------------
patrones_fiables = [
    r'IMPORTE\s+DERECHOS\s*[:\-]*\s*(Art\.\s*[\w\d\.\(\)]+(?:\s+\w+){0,5})',
]

patrones_baja = [
    r'IMPORTE\s+DERECHOS\s*[:\-]*\s*([^\d\n\r]{4,80})'
]

# -------------------- DETECCIÓN --------------------
print("🔍 Analizando textos y buscando subconceptos...")

for idx in tqdm(df.index, desc="Procesando facturas", ncols=100):
    texto = df.at[idx, 'texto_extraido']

    encontrado = False

    for patron in patrones_fiables + patrones_baja:
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            valor = match.group(1).strip()

            # CORTAMOS si termina en número
            valor = re.split(r'\s+\d{1,4}([^\w]|$)', valor)[0].strip()

            start = texto.find(valor)
            end = start + len(valor)

            df.at[idx, 'start'] = start
            df.at[idx, 'end'] = end
            df.at[idx, 'valor_detectado'] = valor
            df.at[idx, 'fiabilidad'] = 'alta' if patron in patrones_fiables else 'baja'
            encontrado = True
            break

# -------------------- GUARDADO --------------------
df.to_excel(output_path, index=False)

total_detectados = df['start'].notna().sum()
alta = df[df['fiabilidad'] == 'alta'].shape[0]
baja = df[df['fiabilidad'] == 'baja'].shape[0]

print(f"\n✅ Archivo generado: {output_path}")
print(f"📊 Total detectados: {total_detectados} de {len(df)}")
print(f"  🔹 Alta fiabilidad: {alta}")
print(f"  🔸 Baja fiabilidad: {baja}")
