# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 11:11:58 2025

@author: r_rsq
"""

import pandas as pd
import os
import spacy
import re
from tqdm import tqdm

# ---------------- CONFIGURACIÓN --------------------
BASE_PATH = r'C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI'
DATA_PATH = os.path.join(BASE_PATH, 'data')
MODEL_PATH = os.path.join(BASE_PATH, 'model', 'NUM_FACTURA')

# ---------------- FUNCIÓN DE RESPALDO --------------------
def extraer_num_factura_backup(texto):
    encabezados = [
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
    patron_valor = r'([A-Z0-9/\-]{4,})'  # permite letras/números con guiones, barras, etc.

    for encabezado in encabezados:
        patron = encabezado + r"[^\w\d]{0,5}" + patron_valor
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            valor = match.group(1).strip()
            # Eliminar si tiene letras pegadas tipo "MADRID10/07/2024Precio"
            if re.search(r'[A-Za-z]{2,}$', valor):
                continue
            return valor
    return None

# ---------------- CARGAMOS MODELO --------------------
print("Cargando modelo NER...")
nlp = spacy.load(MODEL_PATH)

# ---------------- LEEMOS NUEVO OCR --------------------
archivo_textos = os.path.join(BASE_PATH, '000_Textos_facturas_BBDD.xlsx')
df_textos = pd.read_excel(archivo_textos)

# ---------------- INFERENCIA --------------------
print("Iniciando inferencia con modelo y reglas...")
resultados = []

for idx, fila in tqdm(df_textos.iterrows(), total=len(df_textos)):
    texto = str(fila['texto_extraido'])
    doc = nlp(texto)

    resultado = {
        'archivo': fila.get('archivo', f"factura_{idx}"),
        'NUM_FACTURA': None,
        'origen': 'sin_resultado'
    }

    for ent in doc.ents:
        if ent.label_ == 'NUM_FACTURA':
            resultado['NUM_FACTURA'] = ent.text.strip()
            resultado['origen'] = 'modelo'
            break

    # BACKUP si modelo no detectó
    if resultado['NUM_FACTURA'] is None:
        num_backup = extraer_num_factura_backup(texto)
        if num_backup:
            resultado['NUM_FACTURA'] = num_backup
            resultado['origen'] = 'backup'

    resultados.append(resultado)

# ---------------- EXPORTAMOS --------------------
df_resultado = pd.DataFrame(resultados)
salida = os.path.join(DATA_PATH, 'resultado_inferencia_NUM_FACTURA.xlsx')
df_resultado.to_excel(salida, index=False)

# ---------------- RESUMEN FINAL --------------------
conteo_origen = df_resultado['origen'].value_counts()

resultado_modelo = conteo_origen.get('modelo', 0)
resultado_backup = conteo_origen.get('backup', 0)
resultado_nulo = df_resultado['NUM_FACTURA'].isna().sum()

print("\n📊 Resumen de inferencia NUM_FACTURA:")
print(f"🧠 Por modelo:        {resultado_modelo}")
print(f"🛠️  Por regla backup: {resultado_backup}")
print(f"❌ Sin resultado:     {resultado_nulo}")
print("\n✅ Inferencia completada y archivo generado:", salida)
