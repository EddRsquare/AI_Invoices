# -*- coding: utf-8 -*-
"""
Created on Thu Jul 17 12:41:21 2025

@author: r_rsq
"""
import pandas as pd
import re
import os
from tqdm import tqdm

# -------------------- CONFIGURACIÓN --------------------
file_path = r"C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI\data\003_MatrizDatosIRPF.xlsx"
output_path = os.path.join(os.path.dirname(file_path), 'TrainingSet_IRPF.xlsx')

# -------------------- CARGA DE DATOS --------------------
print("📥 Cargando archivo:", file_path)
df = pd.read_excel(file_path)
df['texto_extraido'] = df['texto_extraido'].fillna('').astype(str)

# Inicialización de columnas
df['start'] = None
df['end'] = None
df['valor_detectado'] = None
df['fiabilidad'] = None
df['entidad'] = 'IRPF'

# -------------------- VARIANTES IRPF --------------------
variantes_irpf = [
    r"I[\s\.]?R[\s\.]?P[\s\.]?F\.?",  # IRPF, I.R.P.F, I R P F, etc.
]

patron_importe = r'(\-?\d{1,3}(?:[\.,]\d{3})*[\.,]\d{2})'

# -------------------- DETECCIÓN --------------------
print("🔍 Buscando importes IRPF...")

for idx in tqdm(df.index, desc="Procesando"):
    texto = df.at[idx, 'texto_extraido']

    for patron_irpf in variantes_irpf:
        # Casuística 1: I.R.P.F seguido de % y TOTAL DERECHOS y luego dos importes
        match = re.search(
            patron_irpf + r'[^\d]{0,10}(?:\d{1,2}[.,]\d{2})?[^\d]{0,20}TOTAL[\s]?DERECHOS[^\d]{0,10}' +
            patron_importe + r'[^\d-]{0,10}-?'+ patron_importe,
            texto,
            re.IGNORECASE
        )
        if match:
            try:
                valor = match.group(3).replace('-', '').strip()  # Tercer número → IRPF negativo
                start = match.start(3)
                end = match.end(3)

                df.at[idx, 'start'] = start
                df.at[idx, 'end'] = end
                df.at[idx, 'valor_detectado'] = valor
                df.at[idx, 'fiabilidad'] = 'alta'
                break
            except:
                continue

        # Casuística 2: I.R.P.F con % S/ base seguido de importe negativo → -8,25
        match_neg = re.search(
            patron_irpf + r'.{0,40}?' + patron_importe + r'[^\d-]{0,5}-?' + patron_importe,
            texto,
            re.IGNORECASE
        )
        if match_neg and df.at[idx, 'valor_detectado'] is None:
            try:
                valor = match_neg.group(2).replace('-', '').strip()
                start = match_neg.start(2)
                end = match_neg.end(2)

                df.at[idx, 'start'] = start
                df.at[idx, 'end'] = end
                df.at[idx, 'valor_detectado'] = valor
                df.at[idx, 'fiabilidad'] = 'alta'
                break
            except:
                continue

        # Patrón alternativo más genérico: IRPF cerca de cualquier importe
        match_alt = re.search(
            patron_irpf + r'.{0,60}?' + patron_importe,
            texto,
            re.IGNORECASE
        )
        if match_alt and df.at[idx, 'valor_detectado'] is None:
            try:
                valor = match_alt.group(1).replace('-', '').strip()
                start = match_alt.start(1)
                end = match_alt.end(1)

                df.at[idx, 'start'] = start
                df.at[idx, 'end'] = end
                df.at[idx, 'valor_detectado'] = valor
                df.at[idx, 'fiabilidad'] = 'media'
                break
            except:
                continue


# -------------------- GUARDADO --------------------
df_filtrado = df.dropna(subset=['start', 'end'])
df_filtrado.to_excel(output_path, index=False)

print(f"\n✅ Archivo de entrenamiento generado: {output_path}")
print(f"📊 Registros detectados correctamente: {len(df_filtrado)} de {len(df)}")
