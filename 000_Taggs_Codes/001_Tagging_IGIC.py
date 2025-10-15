# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 10:09:13 2025

@author: r_rsq
"""
import pandas as pd
import re
import os
from tqdm import tqdm

# -------------------- CONFIGURACIÓN --------------------
file_path = r"C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI\data\003_MatrizDatosIGIC.xlsx"
output_path = os.path.join(os.path.dirname(file_path), 'TrainingSet_IGIC.xlsx')

# -------------------- CARGA DE DATOS --------------------
print("📥 Cargando archivo:", file_path)
df = pd.read_excel(file_path)
df['texto_extraido'] = df['texto_extraido'].fillna('').astype(str)
df['IGIC'] = df['IGIC'].fillna('').astype(str)

# Inicialización de columnas
df['start'] = None
df['end'] = None
df['valor_detectado'] = None
df['fiabilidad'] = None
df['entidad'] = 'IGIC'

# -------------------- DETECCIÓN --------------------
print("🔍 Buscando importes IGIC...")

for idx in tqdm(df.index, desc="Procesando"):
    texto = df.at[idx, 'texto_extraido']
    valor_esperado = df.at[idx, 'IGIC']

    if not valor_esperado.strip():
        continue

    # Escapar puntos y comas correctamente
    valor_patron = re.escape(valor_esperado.strip())

    # Buscar en el texto el valor tal cual (con coma)
    match = re.search(valor_patron, texto)
    if match:
        df.at[idx, 'start'] = match.start()
        df.at[idx, 'end'] = match.end()
        df.at[idx, 'valor_detectado'] = match.group()
        df.at[idx, 'fiabilidad'] = 'alta'
    else:
        df.at[idx, 'fiabilidad'] = 'no_detectado'

# -------------------- GUARDADO --------------------
df_filtrado = df.dropna(subset=['start', 'end'])
df_filtrado.to_excel(output_path, index=False)

print(f"\n✅ Archivo de entrenamiento generado: {output_path}")
print(f"📊 Registros detectados correctamente: {len(df_filtrado)} de {len(df)}")
