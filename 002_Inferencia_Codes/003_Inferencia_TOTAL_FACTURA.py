# -*- coding: utf-8 -*-
"""
Created on Fri Jul 11 13:17:58 2025

@author: r_rsq
"""

import pandas as pd
import os
# ---------------- FUNCIÓN DE RESPALDO TOTAL -------------------
import spacy
import re
from tqdm import tqdm

# ---------------- CONFIGURACIÓN --------------------
BASE_PATH = r'C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI'
DATA_PATH = os.path.join(BASE_PATH, 'data')
MODEL_PATH = os.path.join(BASE_PATH, 'model', 'TOTAL_FACTURA')
archivo_textos = os.path.join(DATA_PATH, '000_Textos_facturas_BBDD.xlsx') 

def extraer_total_backup(texto):
    patrones = [
        r'TOTAL FACTURA[^\d]*([\d.,]+)',
        r'TOTAL A PAGAR[^\d]*([\d.,]+)',
        r'IMPORTE TOTAL[^\d]*([\d.,]+)',
        r'TOTAL[^\d]*([\d.,]+)',
        r'TOTAL EUROS[^\d]*([\d.,]+)'
    ]
    for patron in patrones:
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            valor = match.group(1).replace('.', '').replace(',', '.')
            try:
                return float(valor)
            except ValueError:
                continue
    return None

# ---------------- CARGAMOS MODELO --------------------
print("Cargando modelo NER...")
nlp = spacy.load(MODEL_PATH)

# ---------------- LEEMOS NUEVO OCR --------------------
archivo_textos = os.path.join(BASE_PATH, archivo_textos)
df_textos = pd.read_excel(archivo_textos)

# ---------------- INFERENCIA --------------------
print("Iniciando inferencia con modelo y reglas...")
resultados = []

for idx, fila in tqdm(df_textos.iterrows(), total=len(df_textos)):
    texto = str(fila['texto_extraido'])
    doc = nlp(texto)

    resultado = {
        'archivo': fila.get('archivo', f"factura_{idx}"),
        'TOTAL_FACTURA': None,
        # 'PROVEEDOR': None,
        # 'DERECHOS': None
        # 'IGIC': None,
        # 'IBAN': None,
        'origen': 'sin_resultado'
    }

    for ent in doc.ents:
        label = ent.label_
        if label == 'TOTAL_FACTURA':
            resultado['TOTAL_FACTURA'] = ent.text
            resultado['origen'] = 'modelo'
            break  # Si ya se encuentra, no se sigue buscando

    # BACKUP TOTAL_FACTURA (si no lo detectó el modelo)
    if resultado['TOTAL_FACTURA'] is None:
        total_backup = extraer_total_backup(texto)
        if total_backup is not None:
            resultado['TOTAL_FACTURA'] = total_backup
            resultado['origen'] = 'backup'

    resultados.append(resultado)

# ---------------- EXPORTAMOS --------------------
df_resultado = pd.DataFrame(resultados)
salida = os.path.join(DATA_PATH, 'resultado_inferencia_TOTALFACTURA.xlsx')
df_resultado.to_excel(salida, index=False)

# ---------------- RESUMEN FINAL --------------------
conteo_origen = df_resultado['origen'].value_counts()

resultado_modelo = conteo_origen.get('modelo', 0)
resultado_backup = conteo_origen.get('backup', 0)
resultado_nulo = df_resultado['TOTAL_FACTURA'].isna().sum()

print("\n📊 Resumen de inferencia TOTAL_FACTURA:")
print(f"🧠 Por modelo:        {resultado_modelo}")
print(f"🛠️  Por regla backup: {resultado_backup}")
print(f"❌ Sin resultado:     {resultado_nulo}")

print("\n✅ Inferencia completada y archivo generado:", salida)
