# -*- coding: utf-8 -*-
"""
Created on Fri Jul 11 13:36:57 2025

@author: r_rsq
"""

# 000_Tagging_CIFPRA.py

import pandas as pd
import re
import os
from tqdm import tqdm

# -------------------- CONFIGURACIÓN --------------------
file_path = r"C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI\data\000_Textos_facturas_BBDD_v2.xlsx"
output_path = os.path.join(os.path.dirname(file_path), 'TrainingSet_CIFPRA.xlsx')

# -------------------- CARGA DE DATOS --------------------
print("📥 Cargando archivo:", file_path)
df = pd.read_excel(file_path)

# Limpieza de columnas base
df['texto_extraido'] = df['texto_extraido'].fillna('').astype(str)

# 🔧 Limpieza básica del texto
df['texto_extraido'] = (ex
    df['texto_extraido']
    .str.replace(r'\s+', ' ', regex=True)                     # Espacios múltiples, tabs, saltos
    .str.replace(r'[\u200b\u200e\u200f\xa0]', ' ', regex=True)  # Caracteres invisibles
    .str.replace(r'[^\x00-\x7F]+', ' ', regex=True)           # Símbolos unicode raros
    .str.strip()
)

# Inicialización de columnas
df['start'] = None
df['end'] = None
df['valor_detectado'] = None
df['fiabilidad'] = 'alta'
df['entidad'] = 'CIFPRA'

# -------------------- PATRONES DE DETECCIÓN --------------------
# NIF/CIF español: Letra + 8 dígitos, con o sin separadores (espacio, guión, guión bajo)
# Ejemplos válidos: B66766866, B 66766866, B-66766866, B_66766866

patron_cif = re.compile(
    r'\b([A-ZÑ]\s?[-_]?\s?\d{7,8})\b', re.IGNORECASE
)

# -------------------- DETECCIÓN --------------------
print("🔍 Buscando CIF/NIF (CIFPRA) en los textos...")

for idx in tqdm(df.index, desc="Procesando facturas", ncols=100):
    texto = df.at[idx, 'texto_extraido']

    match = patron_cif.search(texto)
    if match:
        start = match.start(1)
        end = match.end(1)
        valor = match.group(1)

        df.at[idx, 'start'] = start
        df.at[idx, 'end'] = end
        df.at[idx, 'valor_detectado'] = valor.strip()

# -------------------- GUARDADO --------------------
import spacy
from spacy.training import offsets_to_biluo_tags

print("\n🔎 Verificando alineación de entidades con SpaCy...")

nlp_tmp = spacy.blank("es")  # no necesita modelo entrenado
malas = 0
mal_alineado_flags = []

for _, row in tqdm(df.iterrows(), total=len(df), desc="Validando alineación", ncols=100):
    texto = row['texto_extraido']
    start = row['start']
    end = row['end']

    if pd.notnull(start) and pd.notnull(end):
        entidad = row.get('entidad', 'ENTIDAD')
        try:
            doc = nlp_tmp.make_doc(texto)
            biluo_tags = offsets_to_biluo_tags(doc, [(int(start), int(end), entidad)])
            if '-' in biluo_tags:
                mal_alineado_flags.append(True)
                malas += 1
            else:
                mal_alineado_flags.append(False)
        except:
            mal_alineado_flags.append(True)
            malas += 1
    else:
        mal_alineado_flags.append(False)

df['mal_alineado'] = mal_alineado_flags
print(f"⚠️  Entidades mal alineadas: {malas} de {len(df)}")

df_filtrado = df.dropna(subset=['start', 'end'])
df_filtrado.to_excel(output_path, index=False)

print(f"\n✅ Archivo de entrenamiento CIFPRA generado: {output_path}")
print(f"📊 Registros detectados correctamente: {len(df_filtrado)} de {len(df)}")
