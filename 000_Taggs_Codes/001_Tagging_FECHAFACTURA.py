# -*- coding: utf-8 -*-
"""
Created on Fri Jul 18 14:42:38 2025

@author: r_rsq
"""
import pandas as pd
import os
import re
from tqdm import tqdm

# ---------------- CONFIGURACIÓN --------------------
BASE_PATH = r'C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI'
DATA_PATH = os.path.join(BASE_PATH, 'data')
archivo_textos = os.path.join(DATA_PATH, '000_Textos_facturas_BBDD_v2.xlsx')

salida_tagging = os.path.join(DATA_PATH, 'TrainingSet_FechaFactura.xlsx')
salida_detectadas = os.path.join(DATA_PATH, 'FechasDetectadas_FechaFactura.xlsx')

# ---------------- CARGA DE DATOS --------------------
print("📥 Cargando textos...")
df = pd.read_excel(archivo_textos)
df['texto_extraido'] = df['texto_extraido'].fillna('').astype(str)

# ---------------- PATRONES DE FECHA --------------------
patrones_fecha = [
    r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',               # 01/01/25, 01-01-2025
    r'\b\d{1,2}[.]\d{1,2}[.]\d{2,4}',                 # 01.01.2025
    r'\b\d{1,2}\s+de\s+\w+\s+de\s+\d{4}',             # 1 de enero de 2024
    r'\b\d{1,2}\s+\w+\s+\d{4}',                       # 01 enero 2025
]

# ---------------- EXTRACCIÓN DE FECHAS ORIGINALES --------------------
fechas_detectadas = []

print("🔍 Buscando fechas...")

for idx, row in tqdm(df.iterrows(), total=len(df)):
    texto = row['texto_extraido']
    fecha_encontrada = None
    start, end = None, None
    fiabilidad = None

    for patron in patrones_fecha:
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            fecha_encontrada = match.group(0)
            start = match.start()
            end = match.end()

            # Mejora de detección de la palabra "fecha"
            contexto = texto[max(0, start - 30):start].lower()
            if re.search(r'fecha\s*:?', contexto):
                fiabilidad = 'alta'
            else:
                fiabilidad = 'media'
            break

    fechas_detectadas.append({
        'archivo': row.get('archivo', f"factura_{idx}"),
        'texto_extraido': texto,
        'fecha_detectada_original': fecha_encontrada,
        'start': start,
        'end': end,
        'entidad': 'FechaFactura' if fecha_encontrada else None,
        'fiabilidad': fiabilidad
    })

# ---------------- SALIDA A EXCEL --------------------
df_detectadas = pd.DataFrame(fechas_detectadas)
df_detectadas.to_excel(salida_detectadas, index=False)

df_tagging = df_detectadas.dropna(subset=['start', 'end'])[
    ['texto_extraido', 'start', 'end', 'entidad', 'fecha_detectada_original']
]
df_tagging.to_excel(salida_tagging, index=False)

# ---------------- RESUMEN --------------------
print(f"\n✅ Tagging guardado en: {salida_tagging}")
print(f"📝 Fechas detectadas guardadas en: {salida_detectadas}")
print(f"📊 Registros listos para entrenamiento: {len(df_tagging)} de {len(df)}")
