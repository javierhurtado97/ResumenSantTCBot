import os
import re
import numpy as np
import pandas as pd
import pdfplumber
from fpdf import FPDF
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
import nest_asyncio
nest_asyncio.apply()  # Para entornos interactivos

#########################
# CLASE PDFTableExtractor
#########################
class PDFTableExtractor:
    def __new__(cls, pdf_path, group_dict):
        # Crea una instancia y realiza la extracción inmediata
        self = super().__new__(cls)
        self.pdf_path = pdf_path
        self.group_dict = group_dict
        self.text = ""
        self.transactions = []
        self.df_o = pd.DataFrame()
        self.df_resume = pd.DataFrame()
        # Extrae el texto y estructura las transacciones
        self.extract_text()
        return self.struct_text(), self.transaction_resume()
    
    @staticmethod
    def convert_amount(val_str):
        """
        Convierte el valor en formato string a formato numérico (float).
        Entrada: str (por ejemplo, "13.859,00")
        Salida: float (por ejemplo, 13859.00)
        """
        if "," in val_str:
            val_str = val_str.replace(".", "").replace(",", ".")
        else:
            val_str = val_str.replace(".", "")
        try:
            return float(val_str)
        except Exception:
            return None

    @staticmethod
    def transaction_validator(row):
        """
        Valida una fila (transacción) para filtrar aquellas que no son operaciones reales.
        Entrada: row (una serie de pandas)
        Salida: bool (True si es una transacción válida)
        """
        date_pattern = re.compile(r'\d{2}/\d{2}/\d{2,4}')
        # Si LUGAR está vacío, se asigna "SIN LUGAR"
        lugar = row["LUGAR"] if row["LUGAR"] and row["LUGAR"].strip() != "" else "SIN LUGAR"
        combined = (lugar + " " + row["DETALLE"]).upper()
        exclude_keywords = ["TOTAL OPERACIONES", "MONTO CANCELADO", "MOVIMIENTOS TARJETA", "PAGAR HASTA", "FACTURADO"]
        if any(kw in combined for kw in exclude_keywords):
            return False
        if date_pattern.search(row["DETALLE"]):
            return False
        return True

    @staticmethod
    def struct_cuotas(text):
        """
        Busca un patrón con uno o dos dígitos, una barra y uno o dos dígitos 
        (estructura de cuotas en el PDF).
        Entrada: text (string)
        Salida: (n_cuota, cuota_total) o (np.nan, np.nan) si no se encuentra
        """
        match = re.search(r'(\d{1,2})/(\d{1,2})\s*$', text)
        if match:
            n_cuota = int(match.group(1))
            cuota_total = int(match.group(2))
            return n_cuota, cuota_total
        else:
            return np.nan, np.nan

    def assign_group(self, detalle):
        """
        Asigna un grupo a la transacción en función del texto del detalle.
        Entrada: detalle (string)
        Salida: grupo (string)
        """
        detalle_upper = detalle.upper()
        for keyword, group in self.group_dict.items():
            if keyword in detalle_upper:
                return group
        return "CONSUMO"

    def extract_text(self):
        """
        Lee el archivo PDF y lo convierte en texto, preservando los saltos de línea.
        Resultado: guarda el texto completo en self.text.
        """
        text = ""
        with pdfplumber.open(self.pdf_path) as pdf:
            for page in pdf.pages:
                extracted = page.extract_text()
                if extracted:
                    text += extracted + "\n"
        self.text = text

    def struct_text(self):
        """
        Procesa el texto extraído del PDF y lo estructura en un DataFrame.
        Salida: self.df_o (DataFrame estructurado)
        """
        lines = self.text.splitlines()
        date_pattern = re.compile(r'\d{2}/\d{2}/\d{2,4}')
        amount_pattern = re.compile(r'\$\s*([-–]?\d{1,3}(?:\.\d{3})*(?:[.,]\d{2})?)')
        
        for line in lines:
            if "$" in line and date_pattern.search(line):
                date_match = date_pattern.search(line)
                if not date_match:
                    continue
                fecha = date_match.group(0)
                date_start = date_match.start()
                # Si no hay texto antes de la fecha, se asigna "SIN LUGAR"
                lugar = line[:date_start].strip() if date_start > 0 else "SIN LUGAR"
                amounts = amount_pattern.findall(line)
                if not amounts:
                    continue
                valor_str = amounts[-1]
                last_dollar_index = line.rfind('$')
                detalle = line[date_match.end():last_dollar_index].strip()
                self.transactions.append((fecha, lugar, detalle, valor_str))
        
        self.df_o = pd.DataFrame(self.transactions, columns=["FECHA", "LUGAR", "DETALLE", "VALOR"])
        self.df_o["VALOR"] = self.df_o["VALOR"].apply(PDFTableExtractor.convert_amount)
        self.df_o = self.df_o[self.df_o.apply(PDFTableExtractor.transaction_validator, axis=1)]
        self.df_o["FG_CUOTA"] = self.df_o["DETALLE"].str.contains("CUOTA", case=False, na=False).astype(int)
        self.df_o[['N_CUOTA', 'CUOTAS_TOT']] = self.df_o.apply(
            lambda row: PDFTableExtractor.struct_cuotas(row['DETALLE']) if row['FG_CUOTA'] else (np.nan, np.nan),
            axis=1,
            result_type='expand'
        )
        self.df_o['GRUPO'] = self.df_o['DETALLE'].apply(self.assign_group)
        self.df_o = self.df_o.reset_index(drop=True)
        return self.df_o

    @staticmethod
    def resume_currency_format(num):
        """
        Formatea el número sin decimales, por ejemplo, 200000.0 -> "200,000",
        intercambiando comas y puntos para obtener el formato deseado: "200.000".
        """
        s = f"{num:,.0f}"
        s = s.replace(",", "X").replace(".", ",").replace("X", ".")
        return f"${s}"

    def transaction_resume(self):
        """
        Arma un resumen agrupado por GRUPO a partir del DataFrame de transacciones.
        Salida: DataFrame resumen con columnas GRUPO, VALOR y PERCENTAGE.
        """
        self.df_resume = self.df_o[['GRUPO', 'VALOR']].groupby('GRUPO').sum().sort_values('VALOR', ascending=False)
        self.df_resume['PERCENTAGE'] = self.df_resume['VALOR'] / self.df_resume['VALOR'].sum()
        self.df_resume['PERCENTAGE'] = self.df_resume['PERCENTAGE'].round(2)
        self.df_resume['VALOR'] = self.df_resume['VALOR'].apply(PDFTableExtractor.resume_currency_format)
        # Resetear el índice para que GRUPO quede como columna
        self.df_resume = self.df_resume.reset_index()
        return self.df_resume

#########################
# BOT DE TELEGRAM
#########################
from telegram.ext import ContextTypes  # Asegurarse de importar ContextTypes

def generate_summary_pdf(df_resume, output_path="summary.pdf"):
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    
    # Título del resumen
    pdf.set_font("Arial", "B", 16)
    pdf.set_text_color(0, 51, 102)  # Azul oscuro
    pdf.cell(0, 10, txt="Resumen de Transacciones", ln=True, align="C")
    pdf.ln(5)
    
    # Configuración de los encabezados de la tabla
    pdf.set_font("Arial", "B", 12)
    pdf.set_fill_color(0, 102, 204)  # Fondo azul para encabezados
    pdf.set_text_color(255, 255, 255)  # Texto blanco en encabezados
    
    # Calcula el ancho disponible y el ancho de cada columna
    page_width = pdf.w - 2 * pdf.l_margin
    n_cols = len(df_resume.columns)
    col_width = page_width / n_cols
    row_height = 10
    
    # Dibuja los encabezados
    for col in df_resume.columns:
        pdf.cell(col_width, row_height, str(col), border=1, fill=True, align="C")
    pdf.ln(row_height)
    
    # Configura el cuerpo de la tabla
    pdf.set_font("Arial", "", 10)
    pdf.set_text_color(0, 0, 0)  # Texto negro
    fill = False  # Para alternar colores de fondo en las filas
    
    for index, row in df_resume.iterrows():
        # Si se supera el límite vertical, se agrega una nueva página y se redibujan los encabezados
        if pdf.get_y() > 250:
            pdf.add_page()
            pdf.set_font("Arial", "B", 12)
            pdf.set_fill_color(0, 102, 204)
            pdf.set_text_color(255, 255, 255)
            for col in df_resume.columns:
                pdf.cell(col_width, row_height, str(col), border=1, fill=True, align="C")
            pdf.ln(row_height)
            pdf.set_font("Arial", "", 10)
            pdf.set_text_color(0, 0, 0)
        # Asigna un color de fondo alternado para cada fila
        if fill:
            pdf.set_fill_color(230, 230, 230)  # Gris claro
        else:
            pdf.set_fill_color(255, 255, 255)  # Blanco
        for col in df_resume.columns:
            pdf.cell(col_width, row_height, str(row[col]), border=1, fill=True, align="C")
        pdf.ln(row_height)
        fill = not fill  # Alterna el fondo para la siguiente fila
    
    # Opcional: se puede agregar número de página aquí
    pdf.output(output_path)
    return output_path

# Handlers asíncronos para el bot (versión 20+)
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Hola! Envíame un archivo PDF para procesarlo.")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    document = update.message.document
    file = await context.bot.get_file(document.file_id)
    file_path = f"temp_{document.file_name}"
    await file.download_to_drive(file_path)
    await update.message.reply_text("Procesando el PDF, por favor espera...")
    
    # Diccionario de grupos (ajústalo según tus necesidades)
    group_dict = {
        'COMERCIAL DECOSTORE': 'DECORACION',
        'MERCADO PAGO 4 TCOM': 'GYM',
        'UBER EATS': 'DELIVERY',
        'UBER TRIP': 'MOVILIDAD',
        'UBER': 'MOVILIDAD',
        'TAXI': 'MOVILIDAD',
        'RAPPI': 'DELIVERY',
        'NIU': 'DELIVERY',
        'MED': 'MEDICO',
        'CRUZ VERDE': 'MEDICO',
        'AHUM': 'MEDICO',
        'SII': 'SII',
        'SODIMAC': 'DECORACION',
        'TOTTUS': 'SUPERMERCADO',
        'JUMBO': 'SUPERMERCADO',
        'BIRRA': 'SALIDA/BAR',
        'BAR': 'SALIDA/BAR',
        'ENTEL': 'SERVICIOS',
        'AGUAS CORDILLERA': 'SUPERMERCADO',
        'PLAYSTATION': 'SUBSCRIPCION',
        'BOCAJUNIORS': 'SUBSCRIPCION',
        'NETFLIX': 'SUBSCRIPCION',
        'AMAZON': 'SUBSCRIPCION',
        'APPLE': 'SUBSCRIPCION',
        'GUACAMOLE': 'DELIVERY',
        'FANTASILANDIA': 'SALIDA/BAR',
        'MACONLINE': 'SEGURO',
        'LATAM': 'VUELOS',
        'COMUNIDAD FELIZ': 'SERVICIOS',
        'TICKET MASTER': 'SALIDAS/BAR',
        'NUI SUSHI': 'DELIVERY'
    }
    
    csv_path = "transactions.csv"
    summary_pdf_path = "summary.pdf"
    
    try:
        # Procesa el PDF usando la clase y obtiene los DataFrames
        df_output, resumen = PDFTableExtractor(file_path, group_dict)
        # Guarda el CSV completo con codificación adecuada
        df_output.to_csv(csv_path, index=False, encoding='utf-8-sig')
        # Genera el PDF resumen con paginación
        summary_pdf_path = generate_summary_pdf(resumen, "summary.pdf")
        # Envía los archivos al usuario
        await update.message.reply_document(document=open(csv_path, "rb"), filename="transactions.csv")
        await update.message.reply_document(document=open(summary_pdf_path, "rb"), filename="summary.pdf")
        await update.message.reply_text("¡Proceso completado!")
    except Exception as e:
        await update.message.reply_text(f"Error al procesar el PDF: {e}")
    finally:
        # Elimina archivos temporales
        if os.path.exists(file_path):
            os.remove(file_path)
        if os.path.exists(csv_path):
            os.remove(csv_path)
        if os.path.exists(summary_pdf_path):
            os.remove(summary_pdf_path)

def main():
    # Reemplaza 'YOUR_BOT_TOKEN' con el token real de tu bot
    application = Application.builder().token("7945679312:AAG5rNx7VUMJiHfiqCKMtbZL4Gof9AOK4j0").build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.Document.PDF, handle_document))
    application.run_polling(close_loop=False)

if __name__ == '__main__':
    main()