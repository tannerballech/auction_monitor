# debug_madison_pdf.py
import io, requests
from pdfminer.high_level import extract_text

PDF_URL = "https://www.madisonkymastercommissioner.com/_files/ugd/2d9678_4599a86d727940a08dd1b0964fc0b453.pdf"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

resp = requests.get(PDF_URL, headers={"User-Agent": UA}, timeout=30)
text = extract_text(io.BytesIO(resp.content))

print(repr(text))   # repr shows exact whitespace, newlines, etc.