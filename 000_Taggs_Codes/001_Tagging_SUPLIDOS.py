# -*- coding: utf-8 -*-
"""
Created on Wed Jul 16 00:15:39 2025

@author: r_rsq
"""

import pandas as pd
import re
import os
from tqdm import tqdm

# -------------------- CONFIGURACIÓN --------------------
file_path = r"C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI\data\000_Textos_facturas_BBDD_v3.xlsx"
output_path = os.path.join(os.path.dirname(file_path), 'TrainingSet_SUPLIDOS.xlsx')

# -------------------- CARGA DE DATOS --------------------
print("📥 Cargando archivo:", file_path)
df = pd.read_excel(file_path)

df['texto_extraido'] = df['texto_extraido'].fillna('').astype(str)
df['start'] = None
df['end'] = None
df['valor_detectado'] = None
df['fiabilidad'] = None
df['entidad'] = 'SUPLIDOS'

# -------------------- PATRONES --------------------
# Variantes confiables
patrones_suplidos_fiables = [
    r"SUPLIDOS",
    r"S\s*U\s*P\s*L\s*I\s*D\s*O\s*S",   # S U P L I D O S
]

# Variantes menos confiables
patrones_suplidos_bajas = [
    r"gastos\s+reembolsables",
    r"otros\s+gastos",
    r"desembolsos\s+varios"
]

# Patrón numérico
patron_valor = r"([0-9]{1,3}(?:[\.,][0-9]{3})*(?:[\.,][0-9]{2}))"

# -------------------- DETECCIÓN --------------------
print("🔍 Buscando patrones SUPLIDOS...")

for idx in tqdm(df.index, desc="Procesando facturas", ncols=100):
    texto = df.at[idx, 'texto_extraido']

    encontrado = False
    for patron in patrones_suplidos_fiables + patrones_suplidos_bajas:
        regex = patron + r"[^\d]{0,10}" + patron_valor
        match = re.search(regex, texto, re.IGNORECASE)
        if match:
            start = match.start(1)
            end = match.end(1)
            valor = match.group(1)

            df.at[idx, 'start'] = start
            df.at[idx, 'end'] = end
            df.at[idx, 'valor_detectado'] = valor
            df.at[idx, 'fiabilidad'] = 'alta' if patron in patrones_suplidos_fiables else 'baja'
            encontrado = True
            break

# -------------------- EXPORTACIÓN --------------------
df.to_excel(output_path, index=False)

print(f"\n✅ Archivo generado: {output_path}")
print(f"📊 Coincidencias con start/end detectadas: {df['start'].notnull().sum()} de {len(df)}")
print("ℹ️ Puedes filtrar por columna 'fiabilidad' para revisar.")


