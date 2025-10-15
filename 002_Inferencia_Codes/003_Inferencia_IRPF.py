# -*- coding: utf-8 -*-
"""
Created on Fri Jul 18 14:12:46 2025

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
MODEL_PATH = os.path.join(BASE_PATH, 'model', 'IRPF')
archivo_textos = os.path.join(DATA_PATH, '000_Textos_facturas_BBDD.xlsx')
salida = os.path.join(DATA_PATH, 'resultado_inferencia_IRPF.xlsx')

# ---------------- CARGAMOS MODELO --------------------
print("🔍 Cargando modelo NER para IRPF...")
nlp = spacy.load(MODEL_PATH)

# ---------------- LECTURA DEL TEXTO --------------------
df_textos = pd.read_excel(archivo_textos)
print(f"📄 Total facturas cargadas: {len(df_textos)}")

# ---------------- FUNCIÓN BACKUP --------------------
def extraer_irpf_backup(texto):
    patrones = [
        # Ejemplo: I.R.P.F. 15,00 % TOTAL DERECHOS 784,79 € -117,72
        r"I[\s\.]?R[\s\.]?P[\s\.]?F[\s\.]?\.?[^\d]{0,10}(?:\d{1,2}[.,]\d{2})?[^\d]{0,20}TOTAL[\s]?DERECHOS[^\d]{0,10}" +
        r"(\d{1,3}(?:[.,]\d{3})*[.,]\d{2})[^\d]{0,10}-?(\d{1,3}(?:[.,]\d{3})*[.,]\d{2})",

        # Ejemplo: I.R.P.F. 15,00 % S/ 55,00 -8,25
        r"I[\s\.]?R[\s\.]?P[\s\.]?F[\s\.]?\.?[^\d]{0,20}(?:\d{1,2}[.,]\d{2})?[^\d]{0,20}S/?[^\d]{0,10}" +
        r"\d{1,3}(?:[.,]\d{3})*[.,]\d{2}[^\d]{0,10}-?(\d{1,3}(?:[.,]\d{3})*[.,]\d{2})",

        # Ejemplo: IRPF sobre Honorarios y Gastos -28,90
        r"I[\s\.]?R[\s\.]?P[\s\.]?F[\s\.]?\.?.{0,40}-?(\d{1,3}(?:[.,]\d{3})*[.,]\d{2})"
    ]

    if not re.search(r"I[\s\.]?R[\s\.]?P[\s\.]?F", texto, re.IGNORECASE):
        return None

    for patron in patrones:
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            for g in reversed(match.groups()):
                if g:
                    return g.replace('-', '').strip()
    return None

# ---------------- INFERENCIA --------------------
print("🤖 Ejecutando inferencia IRPF...")
resultados = []

for idx, fila in tqdm(df_textos.iterrows(), total=len(df_textos)):
    texto = str(fila['texto_extraido'])
    doc = nlp(texto)

    resultado = {
        'archivo': fila.get('archivo', f"factura_{idx}"),
        'IRPF': None,
        'origen': 'sin_resultado'
    }

    for ent in doc.ents:
        if ent.label_ == 'IRPF':
            resultado['IRPF'] = ent.text.replace('-', '').strip()
            resultado['origen'] = 'modelo'
            break

    # Si no detectó el modelo, aplicamos backup solo si aparece IRPF en texto
    if resultado['IRPF'] is None:
        irpf_backup = extraer_irpf_backup(texto)
        if irpf_backup is not None:
            resultado['IRPF'] = irpf_backup
            resultado['origen'] = 'backup'

    resultados.append(resultado)

# ---------------- EXPORTACIÓN --------------------
df_resultado = pd.DataFrame(resultados)
df_resultado.to_excel(salida, index=False)

# ---------------- RESUMEN --------------------
conteo = df_resultado['origen'].value_counts()
print("\n📊 Resumen inferencia IRPF:")
print(f"🧠 Modelo:   {conteo.get('modelo', 0)}")
print(f"🛠️  Backup:  {conteo.get('backup', 0)}")
print(f"❌ Nulos:    {df_resultado['IRPF'].isna().sum()}")
print(f"📁 Archivo generado: {salida}")
