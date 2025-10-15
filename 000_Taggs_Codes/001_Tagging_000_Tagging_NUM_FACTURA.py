# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 10:43:01 2025

@author: r_rsq
"""

# 000_Tagging_NUM_FACTURA.py

import pandas as pd
import re
import os
from tqdm import tqdm
import spacy
from spacy.training import offsets_to_biluo_tags

# -------------------- CONFIGURACIÓN --------------------
file_path = r"C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI\data\000_Textos_facturas_BBDD_v2.xlsx"
output_path = os.path.join(os.path.dirname(file_path), 'TrainingSet_NUM_FACTURA.xlsx')

# -------------------- CARGA DE DATOS --------------------
print("📥 Cargando archivo:", file_path)
df = pd.read_excel(file_path)

df['texto_extraido'] = df['texto_extraido'].fillna('').astype(str)

# 🔧 Limpieza básica del texto
df['texto_extraido'] = (
    df['texto_extraido']
    .str.replace(r'\s+', ' ', regex=True)
    .str.replace(r'[\u200b\u200e\u200f\xa0]', ' ', regex=True)
    .str.replace(r'[^\x00-\x7F]+', ' ', regex=True)
    .str.strip()
)

# Inicializamos columnas
df['start'] = None
df['end'] = None
df['valor_detectado'] = None
df['fiabilidad'] = None
df['entidad'] = 'NUM_FACTURA'

# -------------------- PATRONES DE DETECCIÓN --------------------
encabezados_factura = [
    r'Nº\s*Factura',
    r'Numero\s+de\s+Factura',
    r'Número\s+de\s+Factura',
    r'N\s*mero\s+de\s+Factura',        
    r'Factura\s*:',
    r'Factura\s*Nº',
    r'Factura\s+NUM',
    r'Nº\s*Fac',
    r'FACTURA',
    r'No\.?\s*Fra'
]

# Valor posible: alfanumérico, puede contener / - . guiones bajos
patron_valor = r'([A-Z0-9\-/]{3,})'

# -------------------- DETECCIÓN --------------------
print("🔍 Buscando patrones de número de factura...")

for idx in tqdm(df.index, desc="Procesando facturas", ncols=100):
    texto = df.at[idx, 'texto_extraido']

    for encabezado in encabezados_factura:
        patron = encabezado + r"[^\w\d]{0,5}" + patron_valor
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            valor = match.group(1).strip()

            # Limpieza extra: descartar si termina con letras (ej: SCPFC24-00260Fecha)
            if re.search(r'[A-Za-z]{2,}$', valor):
                continue  # descartamos este match por tener palabra al final

            start = match.start(1)
            end = match.start(1) + len(valor)

            df.at[idx, 'start'] = start
            df.at[idx, 'end'] = end
            df.at[idx, 'valor_detectado'] = valor
            df.at[idx, 'fiabilidad'] = 'alta'
            break  # salimos del loop de encabezados si encontramos uno válido

# -------------------- VERIFICACIÓN DE ALINEACIÓN --------------------
print("\n🔎 Verificando alineación de entidades con SpaCy...")

nlp_tmp = spacy.blank("es")
malas = 0
mal_alineado_flags = []

for _, row in tqdm(df.iterrows(), total=len(df), desc="Validando alineación", ncols=100):
    texto = row['texto_extraido']
    start = row['start']
    end = row['end']

    if pd.notnull(start) and pd.notnull(end):
        entidad = row.get('entidad', 'NUM_FACTURA')
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

# -------------------- GUARDADO --------------------
df_filtrado = df.dropna(subset=['start', 'end'])
df_filtrado.to_excel(output_path, index=False)

print(f"\n✅ Archivo de entrenamiento NUM_FACTURA generado: {output_path}")
print(f"📊 Registros detectados correctamente: {len(df_filtrado)} de {len(df)}")
