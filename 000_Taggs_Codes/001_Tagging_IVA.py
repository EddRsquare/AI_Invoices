# -*- coding: utf-8 -*-
"""
Created on Wed Jul 16 01:02:36 2025

@author: r_rsq
"""
import pandas as pd
import re
import os
from tqdm import tqdm

# -------------------- CONFIGURACIÓN --------------------
file_path = r"C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI\data\003_MatrizDatosIVA.xlsx"
output_path = os.path.join(os.path.dirname(file_path), 'TrainingSet_IVA.xlsx')

# -------------------- CARGA DE DATOS --------------------
print("📥 Cargando archivo:", file_path)
df = pd.read_excel(file_path)

# Limpieza de columnas base
df['texto_extraido'] = df['texto_extraido'].fillna('').astype(str)
df['IVA'] = df['IVA'].astype(str)
df['Importe'] = df['Importe'].astype(str)

# Inicialización
df['start'] = None
df['end'] = None
df['valor_detectado'] = None
df['fiabilidad'] = None
df['entidad'] = 'IVA'

# -------------------- DETECCIÓN --------------------
print("🔍 Buscando importes de IVA dentro del texto...")

for idx in tqdm(df.index, desc="Procesando", ncols=100):
    texto = df.at[idx, 'texto_extraido']
    referencia = df.at[idx, 'IVA']
    importe_objetivo = df.at[idx, 'Importe']

    start_busqueda = texto.find(referencia)
    if start_busqueda == -1:
        continue  # No encontramos el patrón base

    # Buscamos importes numéricos a partir de esa posición
    fragmento_derecha = texto[start_busqueda:]

    # Detectamos todos los importes posibles
    posibles_importes = re.findall(r'([0-9]{1,3}(?:[\.,][0-9]{3})*(?:[\.,][0-9]{2}))', fragmento_derecha)

    # Normalizamos importe objetivo
    importe_normalizado = importe_objetivo.replace('.', '').replace(',', '.').strip()

    match_detectado = False
    for imp in posibles_importes:
        imp_normalizado = imp.replace('.', '').replace(',', '.').strip()
        if imp_normalizado == importe_normalizado:
            abs_start = texto.find(imp, start_busqueda)
            abs_end = abs_start + len(imp)
            df.at[idx, 'start'] = abs_start
            df.at[idx, 'end'] = abs_end
            df.at[idx, 'valor_detectado'] = imp
            df.at[idx, 'fiabilidad'] = 'alta'
            match_detectado = True
            break

    if not match_detectado:
        # Si no lo encontró desde la referencia, intenta buscar en todo el texto (baja fiabilidad)
        posibles_importes_global = re.findall(r'([0-9]{1,3}(?:[\.,][0-9]{3})*(?:[\.,][0-9]{2}))', texto)
        for imp in posibles_importes_global:
            imp_normalizado = imp.replace('.', '').replace(',', '.').strip()
            if imp_normalizado == importe_normalizado:
                abs_start = texto.find(imp)
                abs_end = abs_start + len(imp)
                df.at[idx, 'start'] = abs_start
                df.at[idx, 'end'] = abs_end
                df.at[idx, 'valor_detectado'] = imp
                df.at[idx, 'fiabilidad'] = 'baja'
                break

# -------------------- EXPORTACIÓN --------------------
df_filtrado = df.dropna(subset=['start', 'end'])
df_filtrado.to_excel(output_path, index=False)

print(f"\n✅ Archivo de entrenamiento generado: {output_path}")
print(f"📊 Registros detectados: {len(df_filtrado)} de {len(df)}")
print("ℹ️ Incluye fiabilidad alta y baja.")
