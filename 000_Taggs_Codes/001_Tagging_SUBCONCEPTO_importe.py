# -*- coding: utf-8 -*-
"""
Created on Tue Jul 15 20:58:27 2025

@author: r_rsq
"""

import pandas as pd
import re
import os
from tqdm import tqdm

# -------------------- CONFIGURACIÓN --------------------
file_path = r"C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI\data\000_Textos_facturas_BBDD_v3.xlsx"
output_path = os.path.join(os.path.dirname(file_path), 'TrainingSet_SUBCONCEPTO_importe.xlsx')

# -------------------- CARGA DE DATOS --------------------
print("📥 Cargando archivo:", file_path)
df = pd.read_excel(file_path)
df['texto_extraido'] = df['texto_extraido'].fillna('').astype(str)

# Inicialización de columnas
df['start'] = None
df['end'] = None
df['valor_detectado'] = None
df['fiabilidad'] = 'ninguna'
df['entidad'] = 'SUBCONCEPTO_importe'

# -------------------- PATRONES --------------------
# Patrón principal confiable
patron_fiable = r'IMPORTE\s+DERECHOS.*?(\d{1,3}(?:[\.,]\d{3})*(?:[\.,]\d{2}))'

# Patrón adicional más laxo (para capturar cosas raras)
patron_condicional = r'(\d{1,3}(?:[\.,]\d{3})*(?:[\.,]\d{2}))'

# -------------------- DETECCIÓN --------------------
print("🔍 Analizando textos y buscando importes...")

for idx in tqdm(df.index, desc="Procesando facturas", ncols=100):
    texto = df.at[idx, 'texto_extraido']

    match = re.search(patron_fiable, texto, re.IGNORECASE)
    if match:
        valor = match.group(1).strip()
        start = match.start(1)
        end = match.end(1)

        df.at[idx, 'start'] = start
        df.at[idx, 'end'] = end
        df.at[idx, 'valor_detectado'] = valor
        df.at[idx, 'fiabilidad'] = 'alta'
    else:
        match2 = re.search(patron_condicional, texto)
        if match2:
            valor = match2.group(1).strip()
            start = match2.start(1)
            end = match2.end(1)

            df.at[idx, 'start'] = start
            df.at[idx, 'end'] = end
            df.at[idx, 'valor_detectado'] = valor
            df.at[idx, 'fiabilidad'] = 'baja'

# -------------------- GUARDADO --------------------
df.to_excel(output_path, index=False)

print(f"\n✅ Archivo de entrenamiento generado: {output_path}")
print(f"📊 Coincidencias con alta fiabilidad: {(df['fiabilidad'] == 'alta').sum()}")
print(f"⚠️  Coincidencias con baja fiabilidad: {(df['fiabilidad'] == 'baja').sum()}")
print(f"❌ Sin detección: {(df['start'].isna()).sum()}")
