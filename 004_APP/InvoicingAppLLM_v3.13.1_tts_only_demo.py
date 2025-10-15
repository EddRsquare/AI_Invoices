# -*- coding: utf-8 -*-
"""
InvoicingAppLLM_v3.12.3_tts_only.py
LAIM — Excel + LLM GGUF + (opcional) Wikipedia + Voz de salida (TTS Piper CLI, 100% local)

Cambios clave v3.12.3:
- Filtro por "CIF/NIF/NIE o ID_PROVEEDOR" (auto‑detección).
- Métrica y agregados de proveedores únicos priorizando ID_PROVEEDOR, luego CIFPRA,
  y si no, extracción regex desde TextoExtraido.
- Lectura directa por archivo → devuelve TextoExtraido (p.ej. "lee la factura G_7708_2024411" o "lee G_7708_2024411").
- Cobertura de campos adicionales: TOTALDERECHOS, SUPLIDOS, SUBCONCEPTO(+importe).
- Regex de factura y normalización más robustos.
"""

# ========= IMPORTS =========
import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, List, Tuple
import re, json, os, time, difflib, subprocess, tempfile, base64, hashlib

from llama_cpp import Llama
import wikipedia
import soundfile as sf

# ========= PAGE =========
st.set_page_config(page_title="LAIM — your Local AI assistant", layout="wide")



# --- Rutas locales (ajusta si cambian) ---
EXCEL_PATH    = r"C:\\Users\\r_rsq\\Documents\\001_AI_Facturas\\004_ScriptsFinalesAPP\\resultado_final_consolidado_afinado.xlsx"
GGUF_PATH     = r"C:\\Users\\r_rsq\\Documents\\001_AI_Facturas\\Extractor_AI\\modelos_llm\\Rsquare_V5.Q4_K_M.gguf"
RESP_BAS_PATH = r"C:\\Users\\r_rsq\\Documents\\001_AI_Facturas\\004_ScriptsFinalesAPP\\respuestas_basicas.json"  # opcional



# ========= CACHES / CARGA =========
@st.cache_data
def cargar_datos(path: str):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"No encuentro el Excel: {p}")
    df = pd.read_excel(p)
    df.rename(columns={c: c.strip() for c in df.columns}, inplace=True)
    if "FormatoFecha" in df.columns:
        df["FormatoFecha"] = pd.to_datetime(df["FormatoFecha"], errors="coerce", dayfirst=True)
    return df

@st.cache_resource
def cargar_llm(path:str, n_threads:int, n_ctx:int):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"No encuentro el modelo GGUF: {p}")
    return Llama(model_path=str(p), n_threads=n_threads, n_ctx=n_ctx)

@st.cache_data
def cargar_respuestas_basicas(path:str)->dict:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

# ========= DATA =========
try:
    df = cargar_datos(EXCEL_PATH)
except Exception as e:
    st.error(f"Error cargando Excel: {e}")
    st.stop()

# ========= COLUMNAS CLAVE =========
COL_NUM    = "NUM_FACTURA"
COL_TOTAL  = "TOTAL_FACTURA"
COL_IGIC   = "IGIC"
COL_IRPF   = "IRPF"
COL_IVA    = "IVA"
COL_CIF    = "CIFPRA"
COL_FECHA  = "FormatoFecha"
COL_PROV_ID= "ID_PROVEEDOR" if "ID_PROVEEDOR" in df.columns else None
COL_ARCHIVO= "archivo" if "archivo" in df.columns else None
COL_TEXTO  = "TextoExtraido" if "TextoExtraido" in df.columns else None

# Adicionales del DF
COL_TOTALD = "TOTALDERECHOS" if "TOTALDERECHOS" in df.columns else None
COL_SUPL   = "SUPLIDOS" if "SUPLIDOS" in df.columns else None
COL_SUBTXT = "SUBCONCEPTO" if "SUBCONCEPTO" in df.columns else None
COL_SUBIMP = "SUBCONCEPTO_importe" if "SUBCONCEPTO_importe" in df.columns else None

# Detecta proveedor/cliente (para agregados alternativos por texto)
PROV_CANDIDATES = [
    "PROVEEDOR","Proveedor","NOMBRE_PROVEEDOR","NOMBRE PROVEEDOR","NOMBRE",
    "ACREEDOR","Acreedor","SUPPLIER","Supplier",
    "CLIENTE","Cliente","NOMBRE_CLIENTE",
    "RAZON_SOCIAL","RAZON SOCIAL","Razón social",
    "DEUDOR","Deudor",
]

def pick_first_existing(cols, candidates):
    s = set(cols)
    for c in candidates:
        if c in s:
            return c
    return None
COL_PROV = pick_first_existing(df.columns, PROV_CANDIDATES)


#===========================================================================FACTURACION==================================================================
# ====== BANDEJAS DE VALIDACIÓN (estado de sesión) ======
def _ensure_validation_boxes():
    base_cols = list(df.columns)
    extra_cols = ["__decision", "__motivo", "__timestamp", "__clave_grupo", "__row_id"]
    cols_all = base_cols + [c for c in extra_cols if c not in base_cols]

    if "df_aprobadas" not in st.session_state:
        st.session_state["df_aprobadas"] = pd.DataFrame(columns=cols_all)
    if "df_rechazadas" not in st.session_state:
        st.session_state["df_rechazadas"] = pd.DataFrame(columns=cols_all)

_ensure_validation_boxes()

# ============ VALIDACIÓN DE FACTURAS (PDF + DATOS) ============
# Requisitos: pip install pymupdf
from pathlib import Path
from typing import List, Optional, Tuple
import io
from datetime import datetime

# --- Ajuste en sidebar: carpeta base donde están los PDFs ---
st.sidebar.subheader("📂 Ubicación de PDFs (para validación)")
pdf_base_dir = st.sidebar.text_input(
    r"C:\Users\r_rsq\Documents\001_AI_Facturas\006_FacturaDemo",
    value=str(Path.home()),
    key="val_pdf_base_dir"
)

# --- Helpers PDF / matching de archivo ---
def _safe_join_pdf(base: str, archivo: str) -> Path:
    """
    Une base + archivo con tolerancia si no lleva .pdf.
    """
    a = str(archivo).strip()
    p = Path(a)
    if p.is_absolute():
        final = p
    else:
        final = Path(base) / a
    if not final.exists() and final.suffix.lower() != ".pdf":
        alt = final.with_suffix(".pdf")
        if alt.exists():
            final = alt
    return final

@st.cache_data(show_spinner=False)
def _read_row_by_archivo(df_in: pd.DataFrame, archivo_key: str) -> Optional[pd.Series]:
    """
    Búsqueda ESTRICTA de fila por 'archivo':
    1) igualdad exacta sobre el valor guardado en DF
    2) igualdad por basename
    3) igualdad por stem (sin .pdf)
    """
    col = "archivo"
    if col not in df_in.columns:
        return None

    def _norm(s: str) -> str:
        return str(s).strip().casefold()

    key_full  = _norm(archivo_key)
    key_name  = _norm(Path(archivo_key).name)
    key_stem  = _norm(Path(archivo_key).stem)

    s_full = df_in[col].astype(str).fillna("").map(_norm)
    m = s_full.eq(key_full)
    if m.any():
        return df_in[m].iloc[0]

    s_name = df_in[col].astype(str).fillna("").map(lambda x: _norm(Path(x).name))
    m = s_name.eq(key_name)
    if m.any():
        return df_in[m].iloc[0]

    s_stem = df_in[col].astype(str).fillna("").map(lambda x: _norm(Path(x).stem))
    m = s_stem.eq(key_stem)
    if m.any():
        return df_in[m].iloc[0]

    return None

@st.cache_resource(show_spinner=False)
def _render_pdf_pages_fitz(pdf_path: str, dpi: int = 130) -> List[bytes]:
    """Devuelve lista de imágenes PNG (bytes) de cada página usando PyMuPDF."""
    import fitz  # PyMuPDF
    doc = fitz.open(pdf_path)
    imgs = []
    mat = fitz.Matrix(dpi/72, dpi/72)
    for page in doc:
        pix = page.get_pixmap(matrix=mat, alpha=False)
        imgs.append(pix.tobytes("png"))
    doc.close()
    return imgs

def _render_pdf_pages(pdf_path: str, dpi: int = 130) -> List[bytes]:
    """Wrapper con fallback a pdf2image si no hay fitz."""
    try:
        return _render_pdf_pages_fitz(str(pdf_path), dpi=dpi)
    except Exception:
        try:
            from pdf2image import convert_from_path
        except Exception:
            st.error("No pude renderizar el PDF. Instala PyMuPDF (`pip install pymupdf`) o pdf2image + poppler.")
            return []
        try:
            pages = convert_from_path(str(pdf_path), dpi=dpi)
            buf_list = []
            for im in pages:
                bio = io.BytesIO()
                im.save(bio, format="PNG")
                buf_list.append(bio.getvalue())
            return buf_list
        except Exception as e2:
            st.error(f"No pude renderizar el PDF: {e2}")
            return []

def _verticalize_row(row: pd.Series, columnas_prioridad: Optional[List[str]] = None) -> pd.DataFrame:
    """Convierte una fila en tabla vertical 'Campo'/'Valor'. Permite ordenar columnas prioritarias primero."""
    dfv = row.to_frame().reset_index()
    dfv.columns = ["Campo", "Valor"]
    if columnas_prioridad:
        pri = [c for c in columnas_prioridad if c in dfv["Campo"].tolist()]
        rest = [c for c in dfv["Campo"].tolist() if c not in pri]
        orden = pri + rest
        dfv["__ord"] = dfv["Campo"].apply(lambda x: orden.index(x) if x in orden else 10_000)
        dfv = dfv.sort_values("__ord").drop(columns="__ord")
    return dfv

# --- UI principal del módulo ---
st.markdown("## -LAIM Validación de facturas")

if "archivo" not in df.columns:
    st.warning("No encuentro la columna **'archivo'** en el DataFrame. No puedo validar PDFs.")
else:
    # Selector con búsqueda
    archivos_unicos = sorted(df["archivo"].dropna().astype(str).unique().tolist())
    col_sel1, col_sel2 = st.columns([1,1])
    with col_sel1:
        archivo_pick = st.selectbox(
            "Selecciona una factura (campo **archivo**):",
            options=["(elige…)"] + archivos_unicos,
            index=0,
            key="val_archivo_pick",
        )
    with col_sel2:
        dpi_view = st.slider("Zoom", 90, 100, 120, 5, key="val_dpi_render")

    if archivo_pick and archivo_pick != "(elige…)":
        pdf_path = _safe_join_pdf(pdf_base_dir, archivo_pick)
        colL, colR = st.columns([3,2])

# ---- PANEL IZQUIERDO: PDF ----
pdf_path = _safe_join_pdf(pdf_base_dir, archivo_pick)
colL, colR = st.columns([3, 2])

with colL:
    st.markdown("##### 📄 Documento")
    if not pdf_path.exists():
        st.error(f"No encuentro el PDF: {pdf_path}")
    else:
        imgs = _render_pdf_pages(pdf_path, dpi=dpi_view)
        if not imgs:
            st.info("No hay páginas renderizadas.")
        else:
            import hashlib
            # Clave única por documento + dpi
            slider_key = "page_" + hashlib.sha1(f"{pdf_path}_{dpi_view}".encode()).hexdigest()[:10]

            def _paint(buf: bytes, caption: str):
                scale = max(0.5, min(3.0, dpi_view / 130.0))
                base_width = 900
                target_width = int(base_width * scale)
                target_width = max(500, min(1600, target_width))
                st.image(buf, caption=caption, width=target_width, clamp=True)

            ver_todas = st.toggle("Ver todas las páginas", value=False, key=f"val_ver_todas_{slider_key}")
            n_pages = len(imgs)

            if ver_todas:
                for i, buf in enumerate(imgs, start=1):
                    _paint(buf, f"Página {i}/{n_pages} · {dpi_view} DPI")
                    if i < n_pages:
                        st.divider()
            else:
                if n_pages == 1:
                    _paint(imgs[0], f"Página 1/1 · {dpi_view} DPI")
                else:
                    prev_val = int(st.session_state.get(slider_key, 1))
                    if prev_val < 1 or prev_val > n_pages:
                        prev_val = 1
                    num = st.slider("Página", 1, n_pages, prev_val, 1, key=slider_key)
                    _paint(imgs[num-1], f"Página {num}/{n_pages} · {dpi_view} DPI")

# ---- PANEL DERECHO: DATOS ----
with colR:
    st.markdown("##### 🧾 Datos extraídos (vertical)")
    row = _read_row_by_archivo(df, archivo_pick) if (archivo_pick and archivo_pick != "(elige…)") else None
    if row is None:
        st.warning("No encontré la fila de esa factura en el DataFrame.")
    else:
        prioridad = [
            "archivo", "NUM_FACTURA", "FormatoFecha",
            "ID_PROVEEDOR", "CIFPRA", "TOTAL_FACTURA",
            "IVA", "IRPF", "IGIC",
            "TOTALDERECHOS", "SUPLIDOS",
            "SUBCONCEPTO", "SUBCONCEPTO_importe",
            "TextoExtraido"
        ]
        tabla_v = _verticalize_row(row, columnas_prioridad=prioridad)

        if "TextoExtraido" in tabla_v["Campo"].values:
            mask_tx = tabla_v["Campo"] == "TextoExtraido"
            tabla_min = tabla_v[~mask_tx]
            st.dataframe(tabla_min, use_container_width=True, height=560)
            with st.expander("Ver TextoExtraido", expanded=False):
                st.text_area("TextoExtraido", str(row.get("TextoExtraido","")), height=260)
        else:
            st.dataframe(tabla_v, use_container_width=True, height=420)

        # Descarga de fila en CSV
        csv_row = tabla_v.to_csv(index=False).encode("utf-8")
        st.download_button(
            "📥 Descargar datos de esta factura (CSV)",
            csv_row,
            file_name=f"datos_{Path(archivo_pick).stem}.csv",
            key="val_dl_row"
        )


# ======================= REFERENCIAS (TextoExtraido -> BBDD) =======================
st.markdown("## -LAIM Facturas ↔ Referencias (match por TextoExtraido)")

from pathlib import Path

# ---- Cargar BBDD de referencias (Excel con columnas: Referencia, ActNo) ----
st.caption("Sube tu BBDD de referencias (Excel con columnas **Referencia** y **ActNo**).")
ref_file = st.file_uploader(
    "BBDD referencias", type=["xlsx", "xls", "csv"],
    key="ref_uploader_v2", accept_multiple_files=False
)

df_refbbdd = None
if ref_file is not None:
    try:
        if ref_file.name.lower().endswith(".csv"):
            df_refbbdd = pd.read_csv(ref_file, dtype=str)
        else:
            df_refbbdd = pd.read_excel(ref_file, dtype=str)
        df_refbbdd.columns = [c.strip() for c in df_refbbdd.columns]
        if not {"Referencia", "ActNo"}.issubset(df_refbbdd.columns):
            st.error("La BBDD debe tener al menos las columnas: Referencia, ActNo.")
            df_refbbdd = None
    except Exception as e:
        st.error(f"No pude leer la BBDD: {e}")
        df_refbbdd = None

# ---- Normalizadores y regex ----
def _norm_ref(s):
    s = re.sub(r"\s+", "", str(s)).strip()
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"[^\w-]", "", s, flags=re.UNICODE)  # solo alfanum y guión
    return s.upper()

AMOUNT_RE = re.compile(r"(?<!\d)(\d{1,3}(?:\.\d{3})*,\d{2}|\d+,\d{2})(?!\d)")

def _parse_amount_es(s):
    try:
        s = s.replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return None

# Línea típica:  S/REF. XXXXXXX  / N/REF. A-YYYY  50,00
# Captura SOLO la referencia principal (la de S/REF./REF./MI REF.) y el importe que la sigue.
REF_LINE_RE = re.compile(
    r"(?:S/?\s*REF\.?|MI\s+REF\.?|REF\.?)\s*[:\-]?\s*([0-9A-Z\-]{6,60})"      # ref principal
    r"(?:\s*/\s*(?:N/?\s*REF\.?|MI\s+REF\.?)\s*[.:]?\s*[A-Z]?-?[\w\.-]+)?"    # opcional cola con N/REF.
    r"\s*(\d{1,3}(?:\.\d{3})*,\d{2}|\d+,\d{2})",                              # importe
    flags=re.IGNORECASE
)

def _find_amount_near(text, start, end, radius=120):
    win = text[max(0, start - radius): min(len(text), end + radius)]
    m = AMOUNT_RE.search(win)
    return _parse_amount_es(m.group(1)) if m else None

def _fmt_eur_local(val):
    try:
        return f"{float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + " €"
    except Exception:
        return "—"

def _fmt_num_local(val):
    try:
        return f"{float(val):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return ""

def _impute_mode_inplace(df_in, aplicar):
    """Imputa la moda por RefTextoExtraido (excluye 0 y NaN) y sincroniza ImporteFactura."""
    if df_in is None or getattr(df_in, "empty", True):
        return df_in

    df_local = df_in.copy()
    df_local["ImporteNum"] = pd.to_numeric(df_local.get("ImporteNum"), errors="coerce")

    moda_map = {}
    if aplicar:
        def _mode_nonzero(series):
            s = pd.to_numeric(series, errors="coerce")
            s = s[(s.notna()) & (s != 0)]
            if s.empty:
                return None
            m = s.mode(dropna=True)
            return float(m.iloc[0]) if not m.empty else None

        moda_map = (
            df_local.groupby("RefTextoExtraido")["ImporteNum"]
            .apply(_mode_nonzero)
            .to_dict()
        )

    def _fill_row(row):
        val = row.get("ImporteNum")
        if aplicar and (pd.isna(val) or val == 0):
            m = moda_map.get(row.get("RefTextoExtraido"))
            if m is not None:
                row["ImporteNum"] = m
                row["ImporteFactura"] = _fmt_num_local(m)

        impf = row.get("ImporteFactura", "")
        if (not isinstance(impf, str)) or (isinstance(impf, str) and not impf.strip()):
            impn = row.get("ImporteNum")
            if pd.notna(impn) and float(impn) != 0.0:
                row["ImporteFactura"] = _fmt_num_local(impn)

        return row

    df_local = df_local.apply(_fill_row, axis=1)
    return df_local

# ---- Motor de matching (Regla #1 y Regla #2) ----
if ("archivo" in df.columns) and (df_refbbdd is not None):
    archivos = sorted(df["archivo"].dropna().astype(str).unique().tolist())
    colR1, colR2 = st.columns([2, 1])
    with colR1:
        pick_arch = st.selectbox(
            "Archivo a analizar (por TextoExtraido):",
            ["(elige…)"] + archivos, key="refs_pick_arch_v2"
        )
    with colR2:
        aplicar_moda = st.toggle(
            "Imputar moda del importe (ref iguales)", value=True, key="refs_moda_toggle"
        )

    if pick_arch and pick_arch != "(elige…)":
        # fila origen
        row = _read_row_by_archivo(df, pick_arch)
        if row is None or (COL_TEXTO is None) or (COL_TEXTO not in df.columns):
            st.info("No hay TextoExtraido disponible para este archivo.")
        else:
            texto = str(row[COL_TEXTO])
            texto_flat = " ".join(texto.split())

            # --- Universo BBDD normalizado
            ref_map = {}  # norm -> (ref_original, actno)
            for _, rr in df_refbbdd[["Referencia", "ActNo"]].dropna().iterrows():
                n = _norm_ref(rr["Referencia"])
                if n:
                    ref_map[n] = (rr["Referencia"], rr["ActNo"])
            universo_norm = set(ref_map.keys())

            rows_encontradas = []
            ya_encontradas_norm = set()

            # -------- Regla #1: referencias BBDD que están literalmente en el texto
            texto_norm = _norm_ref(texto_flat)
            for nref in universo_norm:
                if nref and (nref in texto_norm):
                    raw = ref_map[nref][0]
                    raw_idx = texto_flat.upper().find(raw.upper())
                    if raw_idx == -1:  # fallback por si normalización cambió offsets
                        raw_idx = max(0, len(texto_flat) // 2)
                    imp = _find_amount_near(texto_flat, raw_idx, raw_idx + len(raw))
                    rows_encontradas.append({
                        "archivo": pick_arch,
                        "RefTextoExtraido": raw,
                        "Referencia BBDD": raw,
                        "%Coincidencia": "100%",
                        "ImporteFactura": _fmt_num_local(imp) if imp is not None else "",
                        "ImporteNum": imp if imp is not None else 0.0,
                        "Actno": ref_map[nref][1],
                    })
                    ya_encontradas_norm.add(nref)

            # -------- Regla #2: líneas S/REF./REF./MI REF. + importe → solo si NO ya encontradas
            rows_sin_bbdd = []
            for m in REF_LINE_RE.finditer(texto_flat):
                ref_main = m.group(1).strip()
                imp_txt = m.group(2)
                imp_val = _parse_amount_es(imp_txt)

                n = _norm_ref(ref_main)
                if n in ya_encontradas_norm:
                    continue  # ya está en EN_BBDD, no duplicamos

                rows_sin_bbdd.append({
                    "archivo": pick_arch,
                    "RefTextoExtraido": ref_main,
                    "Referencia BBDD": "",
                    "%Coincidencia": "—",
                    "ImporteFactura": _fmt_num_local(imp_val) if imp_val is not None else "",
                    "ImporteNum": imp_val if imp_val is not None else 0.0,
                    "Actno": "",
                })

            # ---- DataFrames base
            if rows_encontradas:
                df_encontradas = pd.DataFrame(rows_encontradas)
            else:
                df_encontradas = pd.DataFrame(columns=[
                    "archivo", "RefTextoExtraido", "Referencia BBDD",
                    "%Coincidencia", "ImporteFactura", "ImporteNum", "Actno"
                ])

            if rows_sin_bbdd:
                df_sin_bbdd = pd.DataFrame(rows_sin_bbdd)
            else:
                df_sin_bbdd = pd.DataFrame(columns=[
                    "archivo", "RefTextoExtraido", "Referencia BBDD",
                    "%Coincidencia", "ImporteFactura", "ImporteNum", "Actno"
                ])

            # Quitar duplicados exactos por referencia principal
            if not df_encontradas.empty:
                df_encontradas = df_encontradas.drop_duplicates(
                    subset=["archivo", "RefTextoExtraido", "Referencia BBDD", "Actno"], keep="first"
                )
            if not df_sin_bbdd.empty:
                df_sin_bbdd = df_sin_bbdd.drop_duplicates(
                    subset=["archivo", "RefTextoExtraido"], keep="first"
                )

            # ---- Imputar MODA en ambas tablas y sincronizar ImporteFactura
            df_encontradas = _impute_mode_inplace(df_encontradas, aplicar_moda)
            df_sin_bbdd   = _impute_mode_inplace(df_sin_bbdd, aplicar_moda)

            # ---- KPIs (misma línea)
            n_en = len(df_encontradas)
            sum_en = float(pd.to_numeric(df_encontradas.get("ImporteNum"), errors="coerce").sum()) if n_en else 0.0
            n_out = len(df_sin_bbdd)
            sum_out = float(pd.to_numeric(df_sin_bbdd.get("ImporteNum"), errors="coerce").sum()) if n_out else 0.0

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("🧾 ConteoRefBBDD", n_en)
            k2.metric("💶 SumaRefBBDD", _fmt_eur_local(sum_en))
            k3.metric("🚫 ConteoRefNoBBDD", n_out)
            k4.metric("💸 SumaRefNoBBDD", _fmt_eur_local(sum_out))

            # ---- Tablas
            st.subheader("✅ Coincidencias (Regla #1 · EN_BBDD)")
            if df_encontradas.empty:
                st.info("No hubo coincidencias exactas con la BBDD en este documento.")
            else:
                st.dataframe(df_encontradas, use_container_width=True, height=260)
                st.download_button(
                    "📥 Descargar EN_BBDD (CSV)",
                    df_encontradas.to_csv(index=False).encode("utf-8"),
                    file_name=f"refs_ENBBDD_{Path(pick_arch).stem}.csv",
                    key="dl_refs_enbbdd_csv"
                )

            st.subheader("🧩 Referencias extraídas SIN BBDD (Regla #2)")
            if df_sin_bbdd.empty:
                st.success("No hay referencias fuera de la BBDD según las reglas definidas.")
            else:
                st.dataframe(df_sin_bbdd, use_container_width=True, height=260)
                st.download_button(
                    "📥 Descargar SIN_BBDD (CSV)",
                    df_sin_bbdd.to_csv(index=False).encode("utf-8"),
                    file_name=f"refs_SINBBDD_{Path(pick_arch).stem}.csv",
                    key="dl_refs_sinbbdd_csv"
                )
else:
    st.info("Sube la BBDD de referencias y selecciona un archivo con **TextoExtraido**.")


# =================== FIN REFERENCIAS ===================



# ======================================================= APROBACIÓN / NO APROBACIÓN ===================================================
st.markdown("---")
st.markdown("### ✅ Aprobación de factura")

# --- logger simple en sesión ---
st.session_state.setdefault("val_logs", [])
def _log(action: str, clave: str, *, from_box: str = "", to_box: str = "", rows:int|None=None, extra:str=""):
    st.session_state["val_logs"].append({
        "__ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "accion": action,
        "clave_grupo": clave,
        "from": from_box,
        "to": to_box,
        "filas_afectadas": rows if rows is not None else "",
        "nota": extra,
    })

def _grupo_mask(df_src: pd.DataFrame, archivo_pick: str, row_sel: Optional[pd.Series]) -> Tuple[pd.Series, str]:
    """Prioriza ARCHIVO (full/basename/stem), luego NUM_FACTURA, si no, fila única."""
    def _norm(s: str) -> str:
        return str(s).strip().casefold()

    # 1) ARCHIVO
    if COL_ARCHIVO and (COL_ARCHIVO in df_src.columns) and archivo_pick:
        s = df_src[COL_ARCHIVO].astype(str).fillna("")
        # full
        m = s.map(_norm).eq(_norm(archivo_pick))
        if not m.any():
            # basename
            ap_base = _norm(Path(archivo_pick).name)
            m = s.map(lambda x: _norm(Path(x).name)).eq(ap_base)
        if not m.any():
            # stem
            ap_stem = _norm(Path(archivo_pick).stem)
            m = s.map(lambda x: _norm(Path(x).stem)).eq(ap_stem)
        if m.any():
            return m, f"ARCH={Path(archivo_pick).name}"

    # 2) NUM_FACTURA
    if (row_sel is not None) and (COL_NUM and (COL_NUM in df_src.columns)):
        num_val = str(row_sel.get(COL_NUM, "")).strip()
        if num_val:
            m = df_src[COL_NUM].astype(str).str.strip().str.casefold().eq(num_val.casefold())
            if m.any():
                return m, f"NUM={num_val}"

    # 3) Fila aislada
    if row_sel is not None:
        idxmask = df_src.index.isin([row_sel.name])
        return idxmask, f"IDX={row_sel.name}"

    return df_src.index.isin([]), "SIN_FILA"

def _ensure_validation_boxes():
    base_cols = list(df.columns)
    extra_cols = ["__decision", "__motivo", "__timestamp", "__clave_grupo", "__row_id"]
    cols_all = base_cols + [c for c in extra_cols if c not in base_cols]
    if "df_aprobadas" not in st.session_state:
        st.session_state["df_aprobadas"] = pd.DataFrame(columns=cols_all)
    if "df_rechazadas" not in st.session_state:
        st.session_state["df_rechazadas"] = pd.DataFrame(columns=cols_all)

def _add_to_box(df_src: pd.DataFrame, box_name: str, decision: str, motivo: str, clave: str):
    """Inserta grupo en la bandeja con metadatos y sin duplicar."""
    _ensure_validation_boxes()
    df_add = df_src.copy()
    df_add["__decision"] = decision
    df_add["__motivo"] = (motivo or "").strip()
    df_add["__timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_add["__clave_grupo"] = clave
    df_add["__row_id"] = df_src.index.astype(str)

    other = "df_rechazadas" if box_name == "df_aprobadas" else "df_aprobadas"
    if not st.session_state[other].empty and "__clave_grupo" in st.session_state[other].columns:
        st.session_state[other] = st.session_state[other][st.session_state[other]["__clave_grupo"] != clave]

    current = st.session_state[box_name]
    combined = pd.concat([current, df_add], ignore_index=True)
    st.session_state[box_name] = combined.drop_duplicates(subset=["__clave_grupo", "__row_id"], keep="last")

def _remove_group(box_name: str, clave: str) -> int:
    """Quita completamente un grupo. Devuelve filas eliminadas."""
    if box_name not in st.session_state:
        return 0
    df_box = st.session_state[box_name]
    if df_box.empty or "__clave_grupo" not in df_box.columns:
        return 0
    before = len(df_box)
    st.session_state[box_name] = df_box[df_box["__clave_grupo"] != clave]
    after = len(st.session_state[box_name])
    return before - after

def _move_group(from_box: str, to_box: str, clave: str, decision_label: str, motivo: str = "") -> int:
    """Mueve un grupo (actualiza metadatos; evita duplicados). Devuelve filas movidas."""
    if from_box not in st.session_state or to_box not in st.session_state:
        return 0
    df_from = st.session_state[from_box]
    if df_from.empty:
        return 0
    grp = df_from[df_from["__clave_grupo"] == clave].copy()
    if grp.empty:
        return 0
    n = len(grp)
    grp["__decision"] = decision_label
    grp["__motivo"] = (motivo or "").strip()
    grp["__timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _remove_group(from_box, clave)
    _remove_group(to_box, clave)
    st.session_state[to_box] = pd.concat([st.session_state[to_box], grp], ignore_index=True) \
        .drop_duplicates(subset=["__clave_grupo", "__row_id"], keep="last")
    return n

# ---------- UI principal ----------
_ensure_validation_boxes()

archivo_pick_state = st.session_state.get("val_archivo_pick")

if ("archivo" in df.columns) and archivo_pick_state and (archivo_pick_state != "(elige…)"):
    row_sel = _read_row_by_archivo(df, archivo_pick_state)
    if row_sel is not None:
        mask_grupo, clave_grupo = _grupo_mask(df, archivo_pick_state, row_sel)
        df_grupo = df[mask_grupo].copy()

        st.caption(f"Grupo de validación: **{clave_grupo}** · Filas en el grupo: **{len(df_grupo)}**")

        col_dec1, col_dec2 = st.columns(2)
        with col_dec1:
            motivo_ok = st.text_input("Motivo/nota (opcional)", key="val_motivo_ok", placeholder="Aprobada por…")
            aprobar = st.button("✅ Factura Aprobada", key="btn_aprobar")
        with col_dec2:
            motivo_no = st.text_input("Motivo rechazo (opcional)", key="val_motivo_no", placeholder="No aprobada por…")
            rechazar = st.button("⛔ Factura No Aprobada", key="btn_rechazar")

        if aprobar:
            try:
                _add_to_box(df_grupo, "df_aprobadas", "APROBADA", motivo_ok, clave_grupo)
                _log("APROBAR", clave_grupo, to_box="df_aprobadas", rows=len(df_grupo), extra=motivo_ok)
                st.success(f"Factura agregada a **APROBADAS** (grupo {clave_grupo}).")
                st.rerun()  # <-- refresco inmediato (evita doble click)
            except Exception as e:
                st.exception(e)

        if rechazar:
            try:
                _add_to_box(df_grupo, "df_rechazadas", "NO_APROBADA", motivo_no, clave_grupo)
                _log("RECHAZAR", clave_grupo, to_box="df_rechazadas", rows=len(df_grupo), extra=motivo_no)
                st.warning(f"Factura agregada a **NO APROBADAS** (grupo {clave_grupo}).")
                st.rerun()  # <-- refresco inmediato
            except Exception as e:
                st.exception(e)
    else:
        st.info("No encontré la fila del DataFrame para ese archivo. Revisa el valor de 'archivo'.")
else:
    st.info("Selecciona un archivo válido para poder aprobar o rechazar.")

# ======================= BANDEJAS DE VALIDACIÓN =======================
st.markdown("## 📦 Bandejas de validación")

dfa = st.session_state["df_aprobadas"]
dfr = st.session_state["df_rechazadas"]

from io import BytesIO

col_b1, col_b2 = st.columns(2)

# ---- Aprobadas ----
with col_b1:
    st.subheader(f"✅ Aprobadas ({len(dfa)})")

    if dfa.empty:
        st.info("No hay facturas aprobadas todavía.")
        sel_apr = None
    else:
        grp_apr_counts = dfa["__clave_grupo"].value_counts()
        opciones_apr = [f"{k}  ·  {grp_apr_counts[k]} fila(s)" for k in grp_apr_counts.index]
        sel_apr_label = st.selectbox("Grupo:", options=opciones_apr, key="apr_sel_grp")
        sel_apr = sel_apr_label.split("  ·  ")[0] if sel_apr_label else None

        st.dataframe(dfa, use_container_width=True, height=260)

        c1, _c2, _c3 = st.columns(3)
        with c1:
            # Quitar en Aprobadas => mover a No Aprobadas
            if st.button("↩️ Quitar del listado (pasa a No Aprobadas)", key="apr_quitar_move") and sel_apr:
                moved = _move_group("df_aprobadas", "df_rechazadas", sel_apr, "NO_APROBADA", "auto: quitado en aprobadas")
                _log("MOVER_A_NO_APROBADA", sel_apr, from_box="df_aprobadas", to_box="df_rechazadas", rows=moved, extra="auto")
                st.warning(f"Movido el grupo {sel_apr} a No Aprobadas.")
                st.rerun()  # <-- refresco inmediato

        # Descargas
        st.download_button(
            "📥 Descargar Aprobadas (CSV)",
            dfa.to_csv(index=False).encode("utf-8"),
            file_name="facturas_aprobadas.csv",
            key="dl_aprobadas_csv"
        )
        bio_a = BytesIO()
        with pd.ExcelWriter(bio_a, engine="xlsxwriter") as writer:
            dfa.to_excel(writer, index=False, sheet_name="Aprobadas")
        st.download_button(
            "📥 Descargar Aprobadas (Excel)",
            data=bio_a.getvalue(),
            file_name="facturas_aprobadas.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_aprobadas_xlsx"
        )

# ---- No Aprobadas ----
with col_b2:
    st.subheader(f"⛔ No Aprobadas ({len(dfr)})")

    if dfr.empty:
        st.info("No hay facturas no aprobadas todavía.")
        sel_rej = None
    else:
        grp_rej_counts = dfr["__clave_grupo"].value_counts()
        opciones_rej = [f"{k}  ·  {grp_rej_counts[k]} fila(s)" for k in grp_rej_counts.index]
        sel_rej_label = st.selectbox("Grupo:", options=opciones_rej, key="rej_sel_grp")
        sel_rej = sel_rej_label.split("  ·  ")[0] if sel_rej_label else None

        st.dataframe(dfr, use_container_width=True, height=260)

        c1, _c2, _c3 = st.columns(3)
        with c1:
            # Quitar en No Aprobadas => eliminar
            if st.button("❌ Quitar del listado (eliminar)", key="rej_quitar_remove") and sel_rej:
                removed = _remove_group("df_rechazadas", sel_rej)
                _log("ELIMINAR_NO_APROBADA", sel_rej, from_box="df_rechazadas", rows=removed, extra="por quitar en No Aprobadas")
                st.success(f"Quitado el grupo {sel_rej} de No Aprobadas.")
                st.rerun()  # <-- refresco inmediato

        # Descargas
        st.download_button(
            "📥 Descargar No Aprobadas (CSV)",
            dfr.to_csv(index=False).encode("utf-8"),
            file_name="facturas_no_aprobadas.csv",
            key="dl_rechazadas_csv"
        )
        bio_r = BytesIO()
        with pd.ExcelWriter(bio_r, engine="xlsxwriter") as writer:
            dfr.to_excel(writer, index=False, sheet_name="NoAprobadas")
        st.download_button(
            "📥 Descargar No Aprobadas (Excel)",
            data=bio_r.getvalue(),
            file_name="facturas_no_aprobadas.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_rechazadas_xlsx"
        )

# --- Registro de operaciones (oculto por defecto)
with st.expander("🧾 Registro de operaciones", expanded=False):
    if st.session_state["val_logs"]:
        df_logs = pd.DataFrame(st.session_state["val_logs"])
        st.dataframe(df_logs.sort_values("__ts", ascending=False), use_container_width=True, height=220)
        st.download_button(
            "📥 Descargar log (CSV)",
            df_logs.to_csv(index=False).encode("utf-8"),
            file_name="validacion_log.csv",
            key="dl_validacion_log_csv"
        )
    else:
        st.caption("No hay acciones registradas todavía.")
# ===================================================================== FIN VALIDACIÓN ======================================================================

    

st.markdown("## -LAIM Tabla de facturas")

# ========= UTILS =========
def available_cols_msg():
    return f"Columnas disponibles: {list(df.columns)}"

# Regex factura más amplia
INVOICE_REGEXES = [
    r"\b\d{4}[-_/]\d{1,6}\b",
    r"\b[A-Z]{1,6}\s*\d{2,6}[-_/]\d{1,6}\b",     # FAC 23/001, F 2023-12
    r"\b[A-Z]{1,6}[-_\/]?\s*\d{4}\s*[-_\/]?\s*\d{1,6}\b",  # FAC-2023-0001
    r"\b[\w-]{3,}\b",  # comodín para códigos tipo G_7708_2024411
]

# Regex CIF/NIF/NIE (español)
CIF_PATTERN = r"([XYZxyz]\d{7}[A-Za-z]|\d{8}[A-Za-z]|[ABCDEFGHJKLMNPQRSUVW]\d{7}[0-9A-J])"

SALUDOS = {"hola","buenos días","buenas","buenas tardes","hey","qué tal","que tal","hello","hi"}

AGG_SUM_TOTAL_KWS = [
    "suma total","suma de las facturas","total de las facturas","importe total",
    "total facturación","total facturacion","facturación total","facturacion total",
    "suma global","sumatorio total"
]
AGG_COUNT_KWS = [
    "cuántas facturas","cuantas facturas","número de facturas","numero de facturas",
    "cuenta de facturas","cantidad de facturas","cuantas hay","cuántas hay"
]
AGG_PROV_TOP_KWS = [
    "proveedor con mas facturas","proveedor con más facturas","top proveedor","proveedor top",
    "quien factura mas","quién factura más","mayor numero de facturas por proveedor",
]
AGG_PROV_COUNT_KWS = [
    "total proveedores","cuantos proveedores","cuántos proveedores","número de proveedores",
    "numero de proveedores","proveedores distintos","proveedores únicos","proveedores unicos"
]
DATA_KWS = [
    "factura","total","igic","irpf","iva","importe","retención","retencion",
    "proveedor","proveedores","cliente","clientes","cif","nif","nie",
    "totalderechos","total derechos","suplidos","subconcepto","base imponible","archivo","texto","contenido","leer","lee"
]

# ---- formatos ----
def fmt_eur(x)->str:
    try:
        return f"{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + " €"
    except Exception:
        return str(x)

def safe_sum(dfv: pd.DataFrame, col: str) -> Tuple[Optional[float], int]:
    if col not in dfv.columns:
        return None, 0
    s = pd.to_numeric(dfv[col], errors="coerce")
    return float(s.sum(skipna=True)), int(s.notna().sum())

def now_str()->str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ---- Anti-duplicados para pending_question / TTS ----
def _hash_text(s: str) -> str:
    return hashlib.sha1(s.strip().encode("utf-8")).hexdigest()

def _should_process(text: str, ttl_seconds: float = 2.0) -> bool:
    now = time.monotonic()
    h = _hash_text(text)
    last = st.session_state.get("last_hash")
    last_t = st.session_state.get("last_hash_time", 0.0)
    if h == last and (now - last_t) < ttl_seconds:
        return False
    st.session_state["last_hash"] = h
    st.session_state["last_hash_time"] = now
    return True

# ========= TTS (Piper CLI) =========
def tts_with_piper_cli(
    texto: str,
    piper_exe: str,
    piper_model: str,
    sentence_silence: float = 0.35,
    length_scale: float = 1.00,
    noise_scale: float = 0.667,
    noise_w: float = 0.8
) -> bytes:
    if not os.path.exists(piper_exe):
        raise FileNotFoundError(f"No encuentro Piper (piper.exe): {piper_exe}")
    if not os.path.exists(piper_model):
        raise FileNotFoundError(f"No encuentro el modelo Piper (.onnx): {piper_model}")

    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp_wav:
        wav_path = tmp_wav.name

    cmd = [
        piper_exe, "-m", piper_model, "-f", wav_path, "-q",
        "--sentence_silence", str(sentence_silence),
        "--length_scale", str(length_scale),
        "--noise_scale", str(noise_scale),
        "--noise_w", str(noise_w),
    ]
    proc = subprocess.run(
        cmd, input=texto.encode("utf-8"),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if proc.returncode != 0:
        raise RuntimeError(f"Piper falló: {proc.stderr.decode('utf-8', errors='ignore')}")

    data, sr = sf.read(wav_path, dtype="float32", always_2d=False)
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as out:
        sf.write(out.name, data, sr, format="WAV")
        out.seek(0)
        wav_bytes = out.read()
    try: os.remove(wav_path)
    except Exception: pass
    return wav_bytes

def speak_response_if_needed(texto, *, piper_exe, piper_model, sentence_sil, length_scale, auto_tts=True):
    if not auto_tts or not texto:
        return

    # anti-duplicado simple por hash (evita re-hablar lo mismo si se re-renderiza)
    h = hashlib.sha1(texto.encode("utf-8")).hexdigest()
    last_h = st.session_state.get("last_tts_hash")
    if last_h == h and len(texto) > 80:
        return
    st.session_state["last_tts_hash"] = h

    try:
        wav_bytes = tts_with_piper_cli(
            texto, piper_exe, piper_model,
            sentence_silence=sentence_sil,
            length_scale=length_scale
        )
    except Exception as e:
        st.error(f"(TTS) Piper falló: {e}")
        return

    if st.session_state.get("audio_ok", False):
        # Autoplay real (si ya se habilitó la voz en esta pestaña)
        b64 = base64.b64encode(wav_bytes).decode("ascii")
        st.markdown(
            f"""
            <audio autoplay preload="auto" playsinline>
              <source src="data:audio/wav;base64,{b64}" type="audio/wav">
            </audio>
            """,
            unsafe_allow_html=True
        )
    else:
        # Fallback con botón Play, y aviso
        st.info("🔇 Autoplay bloqueado por el navegador. Pulsa **'🔓 Habilitar voz (una vez)'** en la barra lateral.")
        st.audio(wav_bytes, format="audio/wav")

# ========= LLM helpers =========
def _safe_prompt(txt: str, max_chars: int = 6000) -> str:
    if len(txt) > max_chars:
        txt = txt[-max_chars:]
    return txt

def llm_answer(llm, txt:str, max_tokens:int, temperature:float) -> str:
    core = f"[INST] {txt.strip()} [/INST]"
    core = _safe_prompt(core, max_chars=6000)
    try:
        out = llm(core, max_tokens=max_tokens, temperature=temperature)
        return out["choices"][0]["text"].strip()
    except Exception:
        try:
            msg = _safe_prompt(txt, max_chars=6000)
            out = llm.create_chat_completion(messages=[{"role":"user","content":msg}],
                                             max_tokens=max_tokens, temperature=temperature)
            return out["choices"][0]["message"]["content"].strip()
        except Exception as e:
            return f"(LLM error) {e}"

# ========= FACTURAS: helpers =========
def normalize_inv(s: str) -> str:
    s = str(s).lower().strip()
    s = re.sub(r'[^a-z0-9]', '', s)
    s = re.sub(r'^(fac|fact|f|inv|invoice)', '', s)
    s = s.lstrip('0')
    return s

if COL_NUM in df.columns:
    df["_NUM_NORM"] = df[COL_NUM].astype(str).map(normalize_inv)

# --- búsqueda de factura por id texto libre ---
def suggest_invoices(inv: str, k: int = 3) -> List[str]:
    try:
        pool = []
        if COL_NUM in df.columns:
            pool.extend(df[COL_NUM].dropna().astype(str).unique().tolist())
        pool = list(dict.fromkeys(pool))
        return difflib.get_close_matches(inv, pool, n=k, cutoff=0.6)
    except Exception:
        return []


def find_row_by_invoice(inv: str):
    if COL_NUM not in df.columns:
        return None
    t_raw = str(inv).strip()
    s_raw = df[COL_NUM].astype(str).str.strip()
    mask_raw = s_raw.str.casefold().eq(t_raw.casefold()) | s_raw.str.contains(re.escape(t_raw), case=False, na=False)
    if mask_raw.any():
        return df[mask_raw].iloc[0]
    if "_NUM_NORM" in df.columns:
        t_norm = normalize_inv(t_raw)
        s_norm = df["_NUM_NORM"]
        mask_norm = s_norm.eq(t_norm) | s_norm.str.contains(re.escape(t_norm), na=False)
        if mask_norm.any():
            return df[mask_norm].iloc[0]
    return None

# --- detectar id de factura desde pregunta ---
def extract_invoice_id(text:str) -> Optional[str]:
    for p in INVOICE_REGEXES:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            return m.group(0).strip()
    # Fallback: aproximación contra toda la columna
    try:
        if COL_NUM in df.columns:
            pool = df[COL_NUM].dropna().astype(str).unique().tolist()
            cand = difflib.get_close_matches(text.strip(), pool, n=1, cutoff=0.5)
            if cand:
                return cand[0]
    except Exception:
        pass
    return None

# --- detectar archivo desde pregunta ---
def extract_archivo_key(q: str) -> Optional[str]:
    if not COL_ARCHIVO:
        return None
    # Primero, si aparece exacto
    tokens = re.findall(r"[\w.-]+", q)
    archs = set(df[COL_ARCHIVO].dropna().astype(str))
    for t in tokens:
        if t in archs:
            return t
    # Aproximación
    try:
        pool = list(archs)
        cand = difflib.get_close_matches(q.strip(), pool, n=1, cutoff=0.6)
        if cand:
            return cand[0]
    except Exception:
        pass
    return None

# --- extraer un CIF desde un texto por fila ---
def extract_first_cif_from_text(text: str) -> Optional[str]:
    if not isinstance(text, str):
        return None
    m = re.search(CIF_PATTERN, text)
    return m.group(0).upper() if m else None

# --- respuesta por-factura extendida ---
def answer_from_df(intent:str, inv:str) -> str:
    row = find_row_by_invoice(inv)
    if row is None:
        sugg = suggest_invoices(inv, k=3)
        tip = f" ¿Quisiste decir: {', '.join(sugg)}?" if sugg else ""
        return f"No encuentro la factura '{inv}'. {available_cols_msg()}{tip}"
    low = intent.lower()
    if "igic" in low and COL_IGIC in df.columns:
        return f"El IGIC de la factura {inv} es: {fmt_eur(row[COL_IGIC])}"
    if "irpf" in low and COL_IRPF in df.columns:
        return f"El IRPF de la factura {inv} es: {fmt_eur(row[COL_IRPF])}"
    if ("iva" in low or "impuesto" in low) and COL_IVA in df.columns:
        return f"El IVA de la factura {inv} es: {fmt_eur(row[COL_IVA])}"
    if "total" in low and COL_TOTAL in df.columns and "total derechos" not in low and "totalderechos" not in low:
        return f"El TOTAL de la factura {inv} es: {fmt_eur(row[COL_TOTAL])}"
    if (("total derechos" in low) or ("totalderechos" in low)) and COL_TOTALD and COL_TOTALD in df.columns:
        return f"El TOTAL DE DERECHOS de la factura {inv} es: {fmt_eur(row[COL_TOTALD])}"
    if ("suplidos" in low) and COL_SUPL and COL_SUPL in df.columns:
        return f"Los SUPLIDOS de la factura {inv} son: {fmt_eur(row[COL_SUPL])}"
    if ("subconcepto" in low):
        partes = []
        if COL_SUBTXT and COL_SUBTXT in df.columns:
            partes.append(f"SUBCONCEPTO: {str(row[COL_SUBTXT])}")
        if COL_SUBIMP and COL_SUBIMP in df.columns:
            partes.append(f"IMPORTE SUBCONCEPTO: {fmt_eur(row[COL_SUBIMP])}")
        if partes:
            return f"Factura {inv} → " + " | ".join(partes)

    # Si llegó aquí: localizada la factura pero no detectó campo
    campos = []
    for tag, col in [
        ("TOTAL", COL_TOTAL), ("IGIC", COL_IGIC), ("IRPF", COL_IRPF), ("IVA", COL_IVA),
        ("TOTALDERECHOS", COL_TOTALD), ("SUPLIDOS", COL_SUPL),
        ("SUBCONCEPTO", COL_SUBTXT), ("SUBCONCEPTO_importe", COL_SUBIMP)
    ]:
        if col and col in df.columns:
            campos.append(f"{tag}→{col}")
    extra = (" Campos mapeados: " + ", ".join(campos)) if campos else ""
    return f"He localizado la factura '{inv}', pero no identifiqué el campo solicitado.{extra} {available_cols_msg()}"

# --- lectura por archivo → TextoExtraido ---
def read_texto_from_archivo(arch_key: str) -> Optional[str]:
    if not (COL_ARCHIVO and COL_TEXTO):
        return None
    key = arch_key.strip()
    key_noext = re.sub(r"\\.pdf$", "", key, flags=re.IGNORECASE)

    series = df[COL_ARCHIVO].astype(str)
    mask = series.str.strip().str.casefold().eq(key.casefold())
    if not mask.any():
        mask = series.str.contains(re.escape(key), case=False, na=False)
    if not mask.any():
        # probar sin extensión
        mask = series.str.contains(re.escape(key_noext), case=False, na=False)

    if mask.any():
        row = df[mask].iloc[0]
        return str(row[COL_TEXTO])
    return None


# ========= INTENT DETECTION =========
def is_structured_query(q:str)->bool:
    ql = q.lower()
    return any(k in ql for k in DATA_KWS)

def respuesta_saludo(q:str) -> str:
    ql = q.strip().lower()
    if any(ql == s or ql.startswith(s) for s in SALUDOS):
        return "¡Hola! Soy LAIM. ¿En qué puedo ayudarte con tus facturas?"
    return ""

def _norm(s: str) -> str:
    return " ".join(str(s).strip().lower().split())

def responder_pregunta_basica(q: str, respuestas_basicas: dict, strict_no_direct: bool) -> str:
    if not respuestas_basicas:
        return ""
    qn = _norm(q)
    if strict_no_direct or any(k in qn for k in DATA_KWS):
        return ""
    for k, v in respuestas_basicas.items():
        if _norm(k) == qn:
            return v
    return ""

# ========= ESTADO =========
st.session_state.setdefault("is_busy", False)
st.session_state.setdefault("pending_question", "")
st.session_state.setdefault("last_hash", None)
st.session_state.setdefault("last_hash_time", 0.0)
st.session_state.setdefault("ultima_respuesta", "")
st.session_state.setdefault("last_tts_hash", "")
st.session_state.setdefault("audio_ok", False)  # 🔓 desbloqueo de audio (autoplay)

# ========= SIDEBAR (todo en un solo desplegable) =========
with st.sidebar.expander("⚙️ Ajustes (Modelo, Modo de respuestas y TTS)", expanded=False):
    # --- Ajustes del modelo ---
    st.subheader("🧠 Modelo local (GGUF)")
    default_threads = max(4, min(8, (os.cpu_count() or 8)))
    n_threads = st.slider("Hilos CPU", 2, (os.cpu_count() or 16), default_threads, step=1)
    n_ctx     = st.select_slider("Contexto (tokens)", options=[1024, 1536, 2048, 3072, 4096], value=4096)
    temp      = st.slider("Temperatura", 0.0, 1.2, 0.2, 0.05)
    max_new   = st.slider("Máx. tokens de salida", 64, 512, 220, 16)

    st.subheader("🎬 Animación")
    use_typewriter = st.toggle("Animación tipo máquina", value=True)
    type_speed     = st.slider("Velocidad (seg/caracter)", 0.001, 0.05, 0.015, 0.001)

    st.subheader("🛡️ Modo respuestas")
    strict_no_direct = st.toggle("Evitar respuestas enlatadas (recomendado)", value=True)

    st.subheader("🔊 Piper (TTS local)")
    piper_exe_path   = st.text_input("Ruta piper.exe", value=r"C:\\piper\\piper.exe")
    piper_model_path = st.text_input("Ruta voz .onnx", value=r"C:\\piper\\voices\\es_MX-claude-high.onnx")
    sentence_sil     = st.slider("Pausa entre frases (seg)", 0.0, 1.0, 0.35, 0.05)
    voz_speed        = st.slider("Velocidad de voz (×)", 0.7, 1.6, 1.25, 0.05)
    length_scale     = max(0.6, min(1.4, 1.0 / max(0.1, voz_speed)))
    auto_tts         = st.toggle("Hablar respuestas automáticamente", value=True)

    # Diagnóstico rápido de rutas
    if not os.path.exists(piper_exe_path):
        st.warning(f"No encuentro Piper: {piper_exe_path} (ajusta la ruta).")
    if not os.path.exists(piper_model_path):
        st.warning(f"No encuentro el modelo .onnx: {piper_model_path} (ajusta la ruta).")

    # Botón Probar voz
    if st.button("🗣️ Probar voz"):
        try:
            wav_bytes = tts_with_piper_cli(
                "Hola, soy LAIM. ¿En qué te ayudo hoy?",
                piper_exe_path, piper_model_path,
                sentence_silence=sentence_sil,
                length_scale=length_scale
            )
            st.audio(wav_bytes, format="audio/wav")
            st.success("¡Piper OK!")
        except Exception as e:
            st.error(f"TTS falló: {e}")

# 🔊 Voz (autoplay on/off en la pestaña)
st.sidebar.markdown("### 🔊 Voz")
colv1, colv2 = st.sidebar.columns(2)
with colv1:
    if st.sidebar.button("🔓 Habilitar voz (una vez)", key="btn_enable_voice"):
        st.session_state["audio_ok"] = True
        st.sidebar.success("Voz habilitada. A partir de ahora debería sonar automáticamente.")
with colv2:
    if st.sidebar.button("🔒 Deshabilitar voz", key="btn_disable_voice"):
        st.session_state["audio_ok"] = False
        st.sidebar.info("Voz deshabilitada. No se reproducirá automáticamente.")
# Estado visible
if st.session_state.get("audio_ok", False):
    st.sidebar.caption("✅ Voz habilitada para autoplay")
else:
    st.sidebar.caption("🔇 Voz deshabilitada (sin autoplay)")

# ========= MODELO =========
try:
    llm = cargar_llm(GGUF_PATH, n_threads=n_threads, n_ctx=n_ctx)
except Exception as e:
    st.error(f"Error cargando el modelo GGUF: {e}")
    st.stop()
respuestas_basicas = cargar_respuestas_basicas(RESP_BAS_PATH)

# ========= FILTROS =========
st.sidebar.header("🧰 Filtros de facturas")
if COL_FECHA in df.columns and df[COL_FECHA].notna().any():
    st.sidebar.caption(
        f"Min fecha en datos: {pd.to_datetime(df[COL_FECHA]).min().date()} | "
        f"Max: {pd.to_datetime(df[COL_FECHA]).max().date()}"
    )

W_MIN = date(2000, 1, 1)
W_MAX = date.today() + timedelta(days=365)

if "fecha_inicio" not in st.session_state or "fecha_fin" not in st.session_state:
    if COL_FECHA in df.columns and df[COL_FECHA].notna().any():
        fmin_real = pd.to_datetime(df[COL_FECHA]).min().date()
        fmax_real = pd.to_datetime(df[COL_FECHA]).max().date()
    else:
        fmin_real = W_MIN
        fmax_real = W_MAX
    st.session_state.fecha_inicio, st.session_state.fecha_fin = fmin_real, fmax_real

st.session_state.fecha_inicio = st.sidebar.date_input(
    "Fecha inicio",
    value=st.session_state.fecha_inicio,
    min_value=W_MIN, max_value=W_MAX
)
st.session_state.fecha_fin = st.sidebar.date_input(
    "Fecha fin",
    value=st.session_state.fecha_fin,
    min_value=W_MIN, max_value=W_MAX
)

# --- Filtro por ID_PROVEEDOR / CIF / Nombre proveedor (explícito) ---
st.sidebar.subheader("🔎 Filtro de proveedor")
if "filtro_id" not in st.session_state:
    st.session_state.filtro_id = ""
if "filtro_tipo" not in st.session_state:
    st.session_state.filtro_tipo = "Auto (CIF o ID_PROVEEDOR)"

filtro_tipo = st.sidebar.selectbox(
    "Filtrar por:",
    ["Auto (CIF o ID_PROVEEDOR)", "ID_PROVEEDOR", "CIF/NIF/NIE", "Nombre proveedor"],
    index=["Auto (CIF o ID_PROVEEDOR)", "ID_PROVEEDOR", "CIF/NIF/NIE", "Nombre proveedor"].index(st.session_state.filtro_tipo),
)
st.session_state.filtro_tipo = filtro_tipo

st.session_state.filtro_id = st.sidebar.text_input(
    "Valor del filtro (ej. B66766866)",
    value=st.session_state.filtro_id
)


colB1, colB2 = st.sidebar.columns(2)
if colB1.button("🧹 Borrar filtros"):
    
    if COL_FECHA in df.columns and df[COL_FECHA].notna().any():
        st.session_state.fecha_inicio = pd.to_datetime(df[COL_FECHA]).min().date()
        st.session_state.fecha_fin    = pd.to_datetime(df[COL_FECHA]).max().date()
    else:
        st.session_state.fecha_inicio = W_MIN
        st.session_state.fecha_fin    = W_MAX
    st.session_state.filtro_id = ""
    
if colB2.button("🔁 Restablecer todo"):
    # Limpia estado de sesión clave
    for k in ["filtro_id", "filtro_tipo", "fecha_inicio", "fecha_fin", "historial", "ultima_respuesta"]:
        if k in st.session_state: del st.session_state[k]
    # Limpia cachés
    try:
        st.cache_data.clear()
        st.cache_resource.clear()
    except Exception:
        pass
    st.success("Sesión y cachés restablecidas. Vuelve a cargar si es necesario.")
    

# Super poderes (Wikipedia)
if "activar_web" not in st.session_state:
    st.session_state.activar_web=False
clave_correcta = "rsquare2025"
clave_usuario  = st.sidebar.text_input("🔐 Clave Super Poderes", type="password", key="clave_temp")
c1, c2 = st.sidebar.columns(2)
if c1.button("✅ Activar web"):
    if clave_usuario == clave_correcta:
        st.session_state.activar_web=True; st.success("🟢 Super power activado")
    else:
        st.warning("Clave incorrecta, bro.")
if c2.button("❌ Desactivar web"):
    st.session_state.activar_web=False; st.info("🔴 Super power desactivada")

# ========= APLICAR FILTROS =========
df_filtrado = df.copy()
if COL_FECHA in df.columns:
    fi = pd.to_datetime(st.session_state.fecha_inicio)
    ff = pd.to_datetime(st.session_state.fecha_fin)
    df_filtrado = df_filtrado[(df_filtrado[COL_FECHA] >= fi) & (df_filtrado[COL_FECHA] <= ff)]

if st.session_state.filtro_id:
    val = st.session_state.filtro_id.strip()
    is_cif = re.fullmatch(CIF_PATTERN, val) is not None

    if st.session_state.filtro_tipo == "ID_PROVEEDOR" and COL_PROV_ID:
        df_filtrado = df_filtrado[df_filtrado[COL_PROV_ID].astype(str).str.contains(val, case=False, na=False)]
    elif st.session_state.filtro_tipo == "CIF/NIF/NIE" and COL_CIF:
        df_filtrado = df_filtrado[df_filtrado[COL_CIF].astype(str).str.contains(val, case=False, na=False)]
    elif st.session_state.filtro_tipo == "Nombre proveedor" and COL_PROV:
        df_filtrado = df_filtrado[df_filtrado[COL_PROV].astype(str).str.contains(val, case=False, na=False)]
    else:
        # Auto (compatibilidad): si parece CIF → CIFPRA, si no → ID_PROVEEDOR, luego nombre
        if is_cif and COL_CIF:
            df_filtrado = df_filtrado[df_filtrado[COL_CIF].astype(str).str.contains(val, case=False, na=False)]
        elif COL_PROV_ID:
            df_filtrado = df_filtrado[df_filtrado[COL_PROV_ID].astype(str).str.contains(val, case=False, na=False)]
        elif COL_PROV:
            df_filtrado = df_filtrado[df_filtrado[COL_PROV].astype(str).str.contains(val, case=False, na=False)]
        else:
            st.warning("No existe columna adecuada para filtrar por ese valor.")

# ========= CONTROLES DE TABLA =========
# st.sidebar.subheader("🗂️ Tabla")
# tabla_fuente = st.sidebar.radio(
#     "¿Qué mostrar?",
#     ["Filtro actual", "Todo el Excel"],
#     index=0,
# )

# # Armar lista de columnas
# cols_all = list(df.columns)
# # columnas útiles por defecto, si existen
# default_cols = [c for c in [
#     COL_FECHA, COL_PROV_ID or COL_CIF, COL_NUM, COL_TOTAL, COL_IVA, COL_IRPF, COL_IGIC, COL_ARCHIVO
# ] if c and c in cols_all]

# # toggle para incluir TextoExtraido (pesado y largo)
# incluir_texto = st.sidebar.toggle("Incluir columna TextoExtraido", value=False)
# if incluir_texto and COL_TEXTO and COL_TEXTO not in default_cols and COL_TEXTO in cols_all:
#     default_cols.append(COL_TEXTO)

# cols_sel = st.sidebar.multiselect(
#     "Columnas a mostrar",
#     options=cols_all,
#     default=default_cols if default_cols else cols_all
# )

# ========= CONTROLES DE TABLA =========
st.sidebar.subheader("🗂️ Tabla")

# ¿Qué dataset quieres ver?  👉 key único
tabla_fuente = st.sidebar.radio(
    "¿Qué mostrar?",
    ["Filtro actual", "Todo el Excel"],
    index=0,
    key="tabla_principal_fuente",
)

# Armar lista de columnas disponibles
cols_all = list(df.columns)

# columnas útiles por defecto (si existen)
default_cols = [c for c in [
    COL_FECHA, (COL_PROV_ID or COL_CIF), COL_NUM, COL_TOTAL, COL_IVA, COL_IRPF, COL_IGIC, COL_ARCHIVO
] if c and c in cols_all]

# Incluir/ocultar TextoExtraido  👉 key único
incluir_texto = st.sidebar.toggle(
    "Incluir columna TextoExtraido",
    value=False,
    key="tabla_principal_incluir_texto"
)
if incluir_texto and COL_TEXTO and COL_TEXTO not in default_cols and COL_TEXTO in cols_all:
    default_cols.append(COL_TEXTO)

# Selección de columnas  👉 key único
cols_sel = st.sidebar.multiselect(
    "Columnas a mostrar",
    options=cols_all,
    default=default_cols if default_cols else cols_all,
    key="tabla_principal_cols_sel"
)

# Filas visibles (para calcular altura de la tabla)  👉 key único
n_filas_sidebar = st.sidebar.slider(
    "Filas visibles en tabla",
    5, 10, 5, 1,
    key="tabla_principal_nfilas"
)

# ========= TABLA =========
# Fuente de datos
tabla_df = df_filtrado if tabla_fuente == "Filtro actual" else df

# Si el usuario NO quiere TextoExtraido, quítalo si se coló
if not incluir_texto and COL_TEXTO and COL_TEXTO in cols_sel:
    cols_sel = [c for c in cols_sel if c != COL_TEXTO]

# Asegurar que todas existen en el df seleccionado
cols_sel = [c for c in cols_sel if c in tabla_df.columns]

# Calcular altura aproximada para ~n_filas_sidebar renglones
visible_rows = min(n_filas_sidebar, len(tabla_df))
row_px = 36      # alto aprox. por fila
header_px = 42   # alto aprox. del encabezado
height_px = header_px + visible_rows * row_px

# Pintar tabla
st.dataframe(
    tabla_df[cols_sel] if cols_sel else tabla_df,
    use_container_width=True,
    height=height_px
)

# Botón de descarga de lo visible
csv_df = (tabla_df[cols_sel] if cols_sel else tabla_df).to_csv(index=False).encode("utf-8")
st.download_button(
    "📥 Descargar tabla mostrada (CSV)",
    csv_df,
    file_name=f"tabla_{'filtro' if tabla_fuente=='Filtro actual' else 'global'}.csv"
)

# Pie con información de lo mostrado
st.caption(
    f"Mostrando **{len(tabla_df)}** filas · **{len(cols_sel) if cols_sel else len(tabla_df.columns)}** columnas — "
    f"Fuente: **{tabla_fuente}** — Filas visibles: **{visible_rows}**"
)

# Mensaje si no hay filas
if tabla_df.empty:
    st.info("📋 No hay filas para mostrar con los filtros actuales.")



# ========= KPIs y tabla =========
# KPIs (filtro)
k1, k2, k3 = st.columns(3)
k1.metric("🧾 Facturas (filtro)", len(df_filtrado))
s_total_f, _ = safe_sum(df_filtrado, COL_TOTAL)
k2.metric("💶 Suma TOTAL (filtro)", fmt_eur(s_total_f) if s_total_f is not None else "—")

# KPIs (global)
s_total_g, _ = safe_sum(df, COL_TOTAL)
k3.metric("🌍 Suma TOTAL (global)", fmt_eur(s_total_g) if s_total_g is not None else "—")
st.caption(f"📦 Total registros en Excel (global): **{len(df)}**")

# Badge de filtros activos
activos = []
if COL_FECHA in df.columns:
    activos.append(f"Fecha: {st.session_state.fecha_inicio:%d/%m/%Y} → {st.session_state.fecha_fin:%d/%m/%Y}")
if st.session_state.filtro_id:
    activos.append(f"{st.session_state.filtro_tipo}: '{st.session_state.filtro_id}'")
if activos:
    st.info("🧰 Filtros activos → " + " | ".join(activos))

st.markdown("## -LAIM Local AI")
  
# --- Badge "Super poderes" debajo del título ---
def render_web_badge():
    on = st.session_state.get("activar_web", False)
    st.markdown(
        f"""
        <div style="
            display:flex; justify-content:flex-end; 
            margin-top:-6px; margin-bottom:8px;
        ">
          <span style="
            font-size:13px; font-weight:600;
            padding:4px 10px; border-radius:8px;
            background:rgba(255,255,255,.06);
            border:1px solid rgba(255,255,255,.12);
            color:{'#0f9d58' if on else '#d93025'};
          ">
            {'🟢 Con Wikipedia' if on else '🔴 Sin Super Poderes'}
          </span>
        </div>
        """,
        unsafe_allow_html=True
    )


# ========= AGREGADOS & CORE =========
def count_unique_providers(df_view: pd.DataFrame) -> Tuple[int, str]:
    """Devuelve (n_unicos, columna_usada). Prioriza ID_PROVEEDOR, luego CIFPRA,
    y finalmente extracción regex de TextoExtraido."""
    if COL_PROV_ID and COL_PROV_ID in df_view.columns:
        n = df_view[COL_PROV_ID].dropna().astype(str).str.strip().nunique()
        return int(n), COL_PROV_ID
    if COL_CIF in df_view.columns:
        n = df_view[COL_CIF].dropna().astype(str).str.strip().nunique()
        return int(n), COL_CIF
    if COL_TEXTO and COL_TEXTO in df_view.columns:
        cifs = df_view[COL_TEXTO].dropna().astype(str).map(extract_first_cif_from_text)
        n = cifs.dropna().nunique()
        return int(n), f"regex@{COL_TEXTO}"
    return 0, ""


def aggregate_answer(q: str, df_view: pd.DataFrame) -> Optional[str]:
    ql = q.lower().strip()
    if any(kw in ql for kw in AGG_COUNT_KWS):
        return f"Hay {len(df_view)} facturas en el filtro actual."

    field = None
    label = None
    if any(kw in ql for kw in AGG_SUM_TOTAL_KWS):
        field, label = COL_TOTAL, "TOTAL_FACTURA"
    elif "igic" in ql:
        field, label = COL_IGIC, "IGIC"
    elif "irpf" in ql:
        field, label = COL_IRPF, "IRPF"
    elif "iva" in ql:
        field, label = COL_IVA, "IVA"

    if field:
        if field not in df_view.columns:
            return f"No encuentro la columna {field} en el Excel. {available_cols_msg()}"
        s, n = safe_sum(df_view, field)
        return f"La suma de {label} en el filtro actual es {fmt_eur(s)} (sobre {n} registros válidos)."

    if any(w in ql for w in ["proveedor","proveedores","cliente","clientes","únicos","unicos","distintos","diferentes"]):
        n_unique, col_used = count_unique_providers(df_view)
        if n_unique == 0:
            return ("No puedo calcular proveedores únicos: faltan columnas CIF/ID_PROVEEDOR/TextoExtraido. "
                    + available_cols_msg())
        return f"Hay **{n_unique}** proveedores únicos (base: **{col_used}**, filtro actual)."

    # Top proveedor por número de facturas (si hay columna texto de nombre)
    if any(kw in ql for kw in AGG_PROV_TOP_KWS):
        if not COL_PROV or COL_PROV not in df_view.columns:
            return ("No puedo calcular 'top proveedor' por nombre: falta columna de nombre de proveedor. "
                    + available_cols_msg())
        counts = df_view[COL_PROV].dropna().astype(str).str.strip().value_counts()
        if counts.empty:
            return "No hay datos de proveedor/cliente en el filtro actual."
        top_name = counts.index[0]
        top_n = int(counts.iloc[0])
        return (f"Top por número de facturas → **{top_name}** con **{top_n}** facturas "
                f"(columna usada: **{COL_PROV}**, filtro actual).")
    return None


def sanitize_tags(text: str) -> str:
    s = str(text).strip()
    if s.endswith("]") and "[" in s:
        i = s.rfind("[")
        if i != -1 and i < len(s):
            return s[:i].rstrip()
    return s

# ========= PIPELINE DE RESPUESTA =========
def procesar_pregunta(pregunta: str):
    st.session_state.historial = st.session_state.get("historial", [])
    st.session_state.historial.append(("🧑‍💻 Tú", f"{now_str()} - {pregunta}"))
    respuesta = respuesta_saludo(pregunta)

    # 1) Lectura por archivo → TextoExtraido
    arch_key = None
    if (
        not respuesta
        and COL_ARCHIVO and COL_TEXTO
        and any(w in pregunta.lower() for w in ["lee","leer","archivo","texto","contenido","factura"])
    ):
        arch_key = extract_archivo_key(pregunta)
        if arch_key:
            txt = read_texto_from_archivo(arch_key)
            if txt:
                # preview + texto completo
                preview = txt[:800] + ("..." if len(txt) > 800 else "")
                st.caption(f"📎 Coincidencia por archivo: {arch_key}")
                st.code(preview)
                with st.expander("Ver TextoExtraido completo", expanded=False):
                    st.text_area("TextoExtraido", txt, height=300)

                respuesta = f"He leído el archivo **{arch_key}** y he mostrado su **TextoExtraido**."

                # prepara payload para TTS (que lea el contenido)
                to_say = txt
                max_chars_tts = 1800  # evita audios eternos
                if len(to_say) > max_chars_tts:
                    to_say = to_say[:max_chars_tts] + " ... [texto recortado para voz]"
                st.session_state["tts_payload"] = to_say

    # 2) Consulta por factura (NUM_FACTURA)
    if not respuesta and is_structured_query(pregunta):
        inv = extract_invoice_id(pregunta)
        if inv:
            respuesta = answer_from_df(pregunta, inv)
            row = find_row_by_invoice(inv)
            if row is not None:
                st.caption("📎 Coincidencia por NUM_FACTURA:")
                st.dataframe(pd.DataFrame([row]), use_container_width=True, height=120)

    # 3) Agregados
    if not respuesta:
        agg = aggregate_answer(pregunta, df_filtrado)
        if agg:
            respuesta = agg

    # 4) Si pregunta de proveedor/cliente sin poder resolver
    if not respuesta and any(w in pregunta.lower() for w in ["proveedor","proveedores","cliente","clientes"]):
        n_unique, col_used = count_unique_providers(df_filtrado)
        if n_unique:
            respuesta = f"En el filtro actual hay **{n_unique}** proveedores únicos (base: **{col_used}**)."
        else:
            respuesta = ("No encuentro columnas suficientes para proveedores. " + available_cols_msg())

    # 5) LLM
    if not respuesta:
        contexto = ""
        q_simple = len(pregunta.strip()) < 40
        if (not q_simple) and (COL_FECHA in df_filtrado.columns):
            fechas = f"{st.session_state.fecha_inicio:%d/%m/%Y} → {st.session_state.fecha_fin:%d/%m/%Y}"
            columnas = df_filtrado.columns.tolist()
            muestra = df_filtrado.head(5).to_dict(orient="records")
            contexto = f"Cols:{columnas}\nFechas:{fechas}\nReg:{len(df_filtrado)}\nMuestra:{muestra}\n\n"
        with st.spinner("Pensando con el LLM…"):
            respuesta = llm_answer(llm, (contexto + pregunta).strip(), max_tokens=max_new, temperature=temp)

    # 6) Wikipedia (si activada)
    if not respuesta and st.session_state.activar_web:
        with st.spinner("Buscando en Wikipedia…"):
            try:
                wikipedia.set_lang("es")
                respuesta = wikipedia.summary(pregunta, sentences=2)
            except Exception:
                respuesta = ""

    # 7) Respuestas básicas
    if not respuesta:
        respuesta = responder_pregunta_basica(pregunta, respuestas_basicas, strict_no_direct)

    # 8) Fallback final
    if not respuesta:
        respuesta = ("Aún no tengo esa respuesta y sin poderes web no puedo buscar más. "
                     "🛠️ Habla con mi creador, bro 😉")

    # Registrar respuesta
    respuesta = sanitize_tags(respuesta)
    st.session_state.historial.append(("🧠 LAIM IA", f"{now_str()} - {respuesta}"))
    st.session_state["ultima_respuesta"] = respuesta

    # ========= TTS de la respuesta =========
    # Si hay tts_payload (por lectura de archivo), úsalo; si no, usa la última respuesta
    to_say = st.session_state.pop("tts_payload", None) or st.session_state["ultima_respuesta"]
    speak_response_if_needed(
        to_say,
        piper_exe=piper_exe_path,
        piper_model=piper_model_path,
        sentence_sil=sentence_sil,
        length_scale=length_scale,
        auto_tts=auto_tts
    )


# ========= Entrada de pregunta (formulario) =========
with st.form(key="form_pregunta", clear_on_submit=False):
    pregunta_text = st.text_input(
        "💬 Pregúntame algo (p.ej. 'Total de la factura 2023-001', '¿Qué es el IGIC?', 'suma total', 'proveedores únicos', 'lee G_7708_2024411')",
        value="",
        key="input_pregunta"
    )
    enviar = st.form_submit_button("Enviar")

if enviar and pregunta_text.strip():
    st.session_state["pending_question"] = pregunta_text.strip()

# ========= Disparador central (para teclado) =========
pq = st.session_state.get("pending_question", "").strip()
if pq and _should_process(pq):
    if not st.session_state["is_busy"]:
        st.session_state["is_busy"] = True
        try:
            procesar_pregunta(pq)
        finally:
            st.session_state["is_busy"] = False
            st.session_state["pending_question"] = ""  # limpia tras procesar

# ========= HISTORIAL =========
if st.session_state.get("historial"):
    autor, msg = st.session_state.historial[-1]
    if autor.startswith("🧠") and st.session_state.get("ultima_respuesta"):
        if 'use_typewriter' in globals() and use_typewriter:
            ph = st.empty()
            acc = ""
            cursor = "▌"
            for ch in msg:
                acc += ch
                ph.markdown(acc + cursor)
                time.sleep(type_speed)
            st.success(acc)
            ph.empty()
        else:
            st.success(f"**{autor}:** {msg}")
    else:
        st.success(f"**{autor}:** {msg}")

    for a, m in reversed(st.session_state.historial[:-1]):
        st.markdown(f"**{a}:** {m}")
