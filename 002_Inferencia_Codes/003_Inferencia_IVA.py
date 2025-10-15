# -*- coding: utf-8 -*-
"""
Created on Wed Jul 16 13:28:24 2025

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
MODEL_PATH = os.path.join(BASE_PATH, 'model', 'IVA')
archivo_textos = os.path.join(DATA_PATH, '000_Textos_facturas_BBDD.xlsx')
salida = os.path.join(DATA_PATH, 'resultado_inferencia_IVA.xlsx')

# ---------------- CARGAMOS MODELO --------------------
print("🔍 Cargando modelo NER para IVA...")
nlp = spacy.load(MODEL_PATH)

# ---------------- LECTURA DEL TEXTO --------------------
df_textos = pd.read_excel(archivo_textos)
print(f"📄 Total facturas cargadas: {len(df_textos)}")

# ---------------- FUNCIÓN BACKUP --------------------
def extraer_iva_backup(texto):
    # Detecta patrones como: I.V.A 21,00 % S/ 850,00 178,50 → debe capturar el 178,50
    patrones = [
        r"(I\.?V\.?A\.?|IVA)[^\d]{0,6}(?:\d{1,2}[.,]\d{2})?\s*(?:S\/)?\s*(\d{1,3}(?:[\.,]\d{3})*[\.,]\d{2})\s*(\d{1,3}(?:[\.,]\d{3})*[\.,]\d{2})",  # 3 números (tipo Total + base + iva)
        r"(I\.?V\.?A\.?|IVA)[^\d]{0,6}(?:\d{1,2}[.,]\d{2})?\s*(\d{1,3}(?:[\.,]\d{3})*[\.,]\d{2})",  # solo un número
    ]
    for patron in patrones:
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            try:
                # Si hay al menos 3 grupos, tomamos el último como IVA real
                if len(match.groups()) >= 3:
                    valor = match.group(3)
                else:
                    valor = match.group(len(match.groups()))
                valor = valor.replace('.', '').replace(',', '.')
                return float(valor)
            except:
                continue
    return None

# ---------------- INFERENCIA --------------------
print("🤖 Ejecutando inferencia...")
resultados = []

for idx, fila in tqdm(df_textos.iterrows(), total=len(df_textos)):
    texto = str(fila['texto_extraido'])
    doc = nlp(texto)

    resultado = {
        'archivo': fila.get('archivo', f"factura_{idx}"),
        'IVA': None,
        'origen': 'sin_resultado'
    }

    for ent in doc.ents:
        if ent.label_ == 'IVA':
            resultado['IVA'] = ent.text
            resultado['origen'] = 'modelo'
            break

    # Si no detectó el modelo, aplicamos backup
    if resultado['IVA'] is None:
        iva_backup = extraer_iva_backup(texto)
        if iva_backup is not None:
            resultado['IVA'] = iva_backup
            resultado['origen'] = 'backup'

    resultados.append(resultado)

# ---------------- EXPORTACIÓN --------------------
df_resultado = pd.DataFrame(resultados)
df_resultado.to_excel(salida, index=False)

# ---------------- RESUMEN --------------------
conteo = df_resultado['origen'].value_counts()
print("\n📊 Resumen inferencia IVA:")
print(f"🧠 Modelo:   {conteo.get('modelo', 0)}")
print(f"🛠️  Backup:  {conteo.get('backup', 0)}")
print(f"❌ Nulos:    {df_resultado['IVA'].isna().sum()}")
print(f"📁 Archivo generado: {salida}")
