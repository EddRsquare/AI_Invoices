# -*- coding: utf-8 -*-
"""
Created on Fri Jul 11 13:08:14 2025

@author: r_rsq
"""

# 000_Tagging_TOTAL_FACTURA.py

import pandas as pd
import re
import os
from tqdm import tqdm

# -------------------- CONFIGURACIÓN --------------------
file_path = r"C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI\data\000_Textos_facturas_BBDD_v2.xlsx"
output_path = os.path.join(os.path.dirname(file_path), 'TrainingSet_TOTAL_FACTURA.xlsx')

# -------------------- CARGA DE DATOS --------------------
print("📥 Cargando archivo:", file_path)
df = pd.read_excel(file_path)

# Limpieza de columnas base
df['texto_extraido'] = df['texto_extraido'].fillna('').astype(str)
df['TOTAL_FACTURA'] = df['TOTAL_FACTURA'].astype(str)

# Inicialización de columnas
df['start'] = None
df['end'] = None
df['valor_detectado'] = None
df['fiabilidad'] = None
df['entidad'] = 'TOTAL_FACTURA'

# -------------------- PATRONES DE DETECCIÓN --------------------
# Alta fiabilidad
patrones_total_fiables = [
    r"TOTAL\s+FACTURA",
    r"IMPORTE\s+TOTAL",
    r"TOTAL\s+FACTURADO",
    r"TOTAL\s*:",
    r"IMPORTE\s+FACTURA",
    r"FACTURA\s+TOTAL",
    r"TOTAL\s+EUROS",
    r"TOTAL\s+FINAL",
]

# Baja fiabilidad
patrones_total_condicionales = [
    r"Total\s+minuta",
    r"Líquido\s+a\s+mi\s+favor\s+\(s\.e\.u\.o\.\)",
    r"Total\s+Factura\s+\(s\.e\.u\.o\.\)",
    r"T\s+O\s+T\s+A\s+L\s+euros\s+\(s\.e\.u\.o\.\)",
    r"T\s+O\s+T\s+A\s+L\s+euros\s+\(s\.e\.u\.\)\s+\.\.\.\s+\.\.\.\s+\.\.\.",
    r"Total\s+Minuta\s+\(s\.e\.u\.o\.\)",
    r"Tasa\s+autonómica",
    r"Tasa\s+Judicial\s+Autonómica",
    r"Tasa\s+Generalitat\s+de\s+Catalunya"
]

# Patrón numérico (valor total)
patron_numero = r'([0-9]{1,3}(?:[\.,][0-9]{3})*(?:[\.,][0-9]{2}))'

# -------------------- DETECCIÓN --------------------
print("🔍 Analizando textos y buscando patrones...")

for idx in tqdm(df.index, desc="Procesando facturas", ncols=100):
    texto = df.at[idx, 'texto_extraido']

    for patron in patrones_total_fiables + patrones_total_condicionales:
        match = re.search(patron + r"[^\d]{0,10}" + patron_numero, texto, re.IGNORECASE)
        if match:
            start = match.start(1)
            end = match.end(1)
            valor = match.group(1)

            df.at[idx, 'start'] = start
            df.at[idx, 'end'] = end
            df.at[idx, 'valor_detectado'] = valor
            df.at[idx, 'fiabilidad'] = 'alta' if patron in patrones_total_fiables else 'baja'
            break  # Solo detectamos la primera coincidencia válida

# -------------------- GUARDADO --------------------
df_filtrado = df.dropna(subset=['start', 'end'])

df_filtrado.to_excel(output_path, index=False)

print(f"\n✅ Archivo de entrenamiento generado: {output_path}")
print(f"📊 Registros detectados correctamente: {len(df_filtrado)} de {len(df)}")
print("ℹ️ Incluye datos con fiabilidad alta y baja. La columna 'fiabilidad' te permitirá filtrar si es necesario.")
