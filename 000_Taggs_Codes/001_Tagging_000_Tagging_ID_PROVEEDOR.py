# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 12:32:50 2025

@author: r_rsq
"""

import pandas as pd
import re
import os
from tqdm import tqdm

# ---------------- CONFIGURACIÓN ----------------
file_path = r"C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI\data\000_Textos_facturas_BBDD.xlsx"
output_path = os.path.join(os.path.dirname(file_path), 'TrainingSet_ID_PROVEEDOR.xlsx')
CIF_PROPIO = {'B80568769', 'B-80568769', 'B_80568769', 'B 80568769'}

# ---------------- CARGA DE DATOS ----------------
print("📥 Cargando archivo:", file_path)
df = pd.read_excel(file_path)
df['texto_extraido'] = df['texto_extraido'].fillna('').astype(str)

# Inicializamos columnas de salida
df['start'] = None
df['end'] = None
df['valor_detectado'] = None
df['fiabilidad'] = None
df['entidad'] = 'ID_PROVEEDOR'

# ---------------- PATRONES ----------------
encabezados = [
    r'\bNIF\b', r'N\.I\.F\.', r'Nº NIF', r'nif', r'n i f',
    r'\bCIF\b', r'C\.I\.F\.', r'cif', r'c i f',
    r'FNI', r'fni'
]
cif_patterns = [
    r'[A-Z][-_\s]?\d{8}\b',
    r'\b\d{8}[-_\s]?[A-Z]\b',
    r'\b\d{1,2}[.,]\d{3}[.,]\d{3}[-_\s]?[A-Z]\b'
]

# ---------------- FUNCIÓN DE NORMALIZACIÓN ----------------
def normalizar(cif):
    return re.sub(r'[\s\-_.]', '', cif).upper()

# ---------------- BÚSQUEDA ----------------
print("🔍 Analizando patrones y etiquetando...")

for idx in tqdm(df.index, desc="Procesando facturas", ncols=100):
    texto = df.at[idx, 'texto_extraido']

    encontrado = False

    # 1. Buscar encabezados tipo NIF/CIF
    for encabezado in encabezados:
        patron = encabezado + r'[^\w\d]{0,5}(' + r'|'.join(cif_patterns) + r')'
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            valor = match.group(1)
            if normalizar(valor) not in {normalizar(c) for c in CIF_PROPIO}:
                start = match.start(1)
                end = match.end(1)
                df.at[idx, 'start'] = start
                df.at[idx, 'end'] = end
                df.at[idx, 'valor_detectado'] = valor
                df.at[idx, 'fiabilidad'] = 'alta'
                encontrado = True
                break

    # 2. Si no encontró por encabezado, buscar directamente
    if not encontrado:
        for patron in cif_patterns:
            match = re.search(patron, texto, re.IGNORECASE)
            if match:
                valor = match.group(0)
                if normalizar(valor) not in {normalizar(c) for c in CIF_PROPIO}:
                    start = match.start(0)
                    end = match.end(0)
                    df.at[idx, 'start'] = start
                    df.at[idx, 'end'] = end
                    df.at[idx, 'valor_detectado'] = valor
                    df.at[idx, 'fiabilidad'] = 'baja'
                    break

# ---------------- GUARDADO ----------------
df_filtrado = df.dropna(subset=['start', 'end'])
df_filtrado.to_excel(output_path, index=False)

print(f"\n✅ Archivo de entrenamiento generado: {output_path}")
print(f"📊 Registros etiquetados: {len(df_filtrado)} de {len(df)}")
