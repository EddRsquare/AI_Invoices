# -*- coding: utf-8 -*-
"""
Created on Mon Jul 21 14:11:22 2025

@author: r_rsq
"""
import pandas as pd
import os
from tqdm import tqdm

# Ruta base
base_path = r'C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI\data'

# Archivo base
df_base = pd.read_excel(os.path.join(base_path, '000_Textos_facturas_BBDD.xlsx'))

# Archivos de inferencia
archivos_inferencia = [
    'resultado_inferencia_CIFPRA.xlsx',
    'resultado_inferencia_FECHAFACTURA.xlsx',
    'resultado_ID_PROVEEDOR.xlsx',
    'resultado_inferencia_IGIC.xlsx',
    'resultado_inferencia_IRPF.xlsx',
    'resultado_inferencia_IVA.xlsx',
    'resultado_inferencia_NUM_FACTURA.xlsx',
    'resultado_inferencia_SUBCONCEPTO.xlsx',
    'resultado_inferencia_SUBCONCEPTO_importe.xlsx',
    'resultado_inferencia_SUPLIDOS.xlsx',
    'resultado_inferencia_TOTALDERECHOS.xlsx',
    'resultado_inferencia_TOTALFACTURA.xlsx',
]

# Inicializar DataFrame final con columnas base
df_final = df_base[['archivo', 'texto_extraido']].copy()

# Merge con barra de progreso
for archivo in tqdm(archivos_inferencia, desc="📂 Combinando archivos de inferencia"):
    path = os.path.join(base_path, archivo)
    df_temp = pd.read_excel(path)
    
    # Eliminar columnas no deseadas
    columnas_a_eliminar = [col for col in df_temp.columns if 'origen' in col.lower() or 'estadoconversion' in col.lower()]
    df_temp.drop(columns=columnas_a_eliminar, inplace=True, errors='ignore')

    df_final = df_final.merge(df_temp, on='archivo', how='left')

# ------------------ Aplicar formato correcto por campo ------------------
# Columnas tipo número con 2 decimales (formato europeo)
columnas_numericas = [
    'SUBCONCEPTO_importe', 'TOTALDERECHOS', 'SUPLIDOS',
    'IGIC', 'IRPF', 'IVA', 'TOTAL_FACTURA'
]

for col in columnas_numericas:
    if col in df_final.columns:
        df_final[col] = (
            df_final[col]
            .astype(str)
            .str.replace('.', '', regex=False)       # eliminar puntos de miles
            .str.replace(',', '.', regex=False)      # cambiar coma a punto decimal
        )
        df_final[col] = pd.to_numeric(df_final[col], errors='coerce').round(2)

# Convertir a fecha
if 'FormatoFecha' in df_final.columns:
    df_final['FormatoFecha'] = pd.to_datetime(df_final['FormatoFecha'], errors='coerce').dt.date

# Reordenar columnas
orden_columnas = [
    'archivo', 'texto_extraido', 'CIFPRA', 'FormatoFecha',
    'ID_PROVEEDOR', 'NUM_FACTURA', 'SUBCONCEPTO', 'SUBCONCEPTO_importe',
    'TOTALDERECHOS', 'SUPLIDOS', 'IGIC', 'IRPF', 'IVA', 'TOTAL_FACTURA'
]
df_final = df_final[[col for col in orden_columnas if col in df_final.columns]]

# Guardar resultado
salida = os.path.join(base_path, 'resultado_final_consolidado_afinado.xlsx')
df_final.to_excel(salida, index=False)

print("✅ Resultado final afinado guardado en:", salida)
