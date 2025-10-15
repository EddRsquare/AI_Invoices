# -*- coding: utf-8 -*-
"""
Created on Fri Jul 11 13:47:17 2025

@author: r_rsq
"""

# 000_Training_CIFPRA.py

import spacy
from spacy.training import Example
import pandas as pd
import random
import os
from tqdm import tqdm

# -------------------- CONFIGURACIÓN --------------------
BASE_PATH = r'C:\Users\r_rsq\Documents\001_AI_Facturas\Extractor_AI'
DATA_PATH = os.path.join(BASE_PATH, 'data')
MODEL_PATH = os.path.join(BASE_PATH, 'model', 'CIFPRA')
archivo_entrenamiento = os.path.join(DATA_PATH, 'TrainingSet_CIFPRA.xlsx')

entrenar_desde_cero = True  # Cambia a False si quieres continuar un modelo anterior

# -------------------- CARGA DATOS ENTRENAMIENTO --------------------
print("📥 Cargando datos de entrenamiento para entidad CIFPRA...")
df = pd.read_excel(archivo_entrenamiento)
df = df.dropna(subset=['start', 'end'])

train_data = []
for _, row in df.iterrows():
    texto = str(row['texto_extraido'])
    start = int(row['start'])
    end = int(row['end'])
    entidad = row.get('entidad', 'CIFPRA')  # Por defecto
    train_data.append((texto, {'entities': [(start, end, entidad)]}))

print(f"✅ Muestras cargadas: {len(train_data)} para entrenar la entidad CIFPRA.")

# -------------------- PREPARAR MODELO --------------------
if entrenar_desde_cero:
    print("🧪 Entrenamiento desde cero para CIFPRA...")
    nlp = spacy.blank('es')
    ner = nlp.add_pipe('ner')
    for _, annotations in train_data:
        for ent in annotations.get('entities'):
            ner.add_label(ent[2])
else:
    print("🧠 Cargando modelo anterior de CIFPRA...")
    nlp = spacy.load(MODEL_PATH)
    ner = nlp.get_pipe('ner')

# -------------------- ENTRENAMIENTO --------------------
optimizer = nlp.initialize() if entrenar_desde_cero else nlp.resume_training()

print(f"\n🚀 Iniciando entrenamiento con {len(train_data)} muestras...")

for itn in tqdm(range(10), desc="🔁 Iteraciones", ncols=100):
    random.shuffle(train_data)
    losses = {}

    for text, annotations in tqdm(train_data, desc=f"🧠 Iter {itn+1}", leave=False, ncols=100):
        doc = nlp.make_doc(text)
        example = Example.from_dict(doc, annotations)
        nlp.update([example], drop=0.3, losses=losses)

    tqdm.write(f"📉 Pérdidas en iteración {itn+1}: {losses}")

# -------------------- GUARDAR MODELO --------------------
nlp.to_disk(MODEL_PATH)
print("\n✅ Modelo CIFPRA guardado en:", MODEL_PATH)
