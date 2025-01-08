import io
import logging
from typing import List, Optional, Dict
from dataclasses import dataclass
from functools import wraps
import json

# Third-party imports
import pdfplumber
# import textract
# from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
# from pdfminer.converter import TextConverter
# from pdfminer.layout import LAParams
# from pdfminer.pdfpage import PDFPage

try:
    from docx import Document
    PYTHON_DOCX_INSTALLED = True
except ImportError:
    PYTHON_DOCX_INSTALLED = False

@dataclass
class PageContent:
    text: str
    page_number: int

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def exception_handler(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            return None
    return wrapper

class DocumentExtractor:
    @staticmethod
    @exception_handler
    def extract_from_pdf(pdf_path: str) -> List[PageContent]:
        """Extract text and page numbers from PDF using pdfplumber."""
        pages = []
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                text = page.extract_text()
                if text.strip():
                    pages.append(PageContent(
                        text=text.strip(),
                        page_number=page_num
                    ))
        return pages

    @staticmethod
    @exception_handler
    def extract_from_doc(doc_path: str) -> List[PageContent]:
        """Extract text and page numbers from DOCX."""
        if not PYTHON_DOCX_INSTALLED:
            return []

        pages = []
        doc = Document(doc_path)
        current_page = 1
        current_text = []
        
        for para in doc.paragraphs:
            if para.text.strip():
                current_text.append(para.text.strip())
                if len(current_text) >= 3:  # Arbitrary page break after 3 paragraphs
                    pages.append(PageContent(
                        text='\n'.join(current_text),
                        page_number=current_page
                    ))
                    current_page += 1
                    current_text = []
        
        if current_text:
            pages.append(PageContent(
                text='\n'.join(current_text),
                page_number=current_page
            ))
        
        return pages

def save_to_json(pages: List[PageContent], output_path: str):
    """Save extracted pages to JSON file."""
    output = [{
        'text': p.text,
        'page_number': p.page_number
    } for p in pages]
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    pdf_path = "Shashi_Resume.pdf"
    docx_path = "document.docx"
    
    pdf_pages = DocumentExtractor.extract_from_pdf(pdf_path)
    for page_info in pdf_pages:
        print(page_info.text)
        print("Page Number: ", page_info.page_number)

    # save_to_json(pdf_pages, "pdf_output.json")
    
    # doc_pages = DocumentExtractor.extract_from_doc(docx_path)
    # save_to_json(doc_pages, "doc_output.json")