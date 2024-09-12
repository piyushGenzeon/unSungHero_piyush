import csv
from sqlalchemy import create_engine, Column, Integer, String, Text, ForeignKey, DateTime, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
import os

# Define the base class
Base = declarative_base()

# Define the Page, Document, DocumentField, and File models
class Page(Base):
    __tablename__ = "page"
    pageId = Column(Integer, primary_key=True, autoincrement=True)
    fileId = Column(Integer, ForeignKey("file.fileId"), nullable=True)
    documentId = Column(Integer, ForeignKey("document.documentId"), nullable=True)
    pageNumber = Column(Integer, nullable=False)
    pageLocation = Column(Text, nullable=False)
    pageUrl = Column(Text, nullable=True)
    cleanOcrData = Column(Text, nullable=True)
    ocrStatus = Column(String(50), nullable=True)
    processStatus = Column(String(50), nullable=True)
    isBlank = Column(Boolean, default=False)
    dateOfService = Column(DateTime, nullable=True)
    MD5 = Column(String(255), nullable=True)

class Document(Base):
    __tablename__ = "document"
    documentId = Column(Integer, primary_key=True, autoincrement=True)
    documentName = Column(String(255), nullable=False)

class DocumentField(Base):
    __tablename__ = "documentField"
    fieldId = Column(Integer, primary_key=True, autoincrement=True)
    documentId = Column(Integer, ForeignKey("document.documentId"), nullable=True)
    fieldName = Column(String(255), nullable=False)
    fieldValue = Column(Text, nullable=True)
    confidenceScore = Column(Integer, nullable=True)
    pageId = Column(Integer, ForeignKey("page.pageId"), nullable=True)

class File(Base):
    __tablename__ = "file"
    fileId = Column(Integer, primary_key=True, autoincrement=True)
    fileName = Column(String(255), nullable=True)

# Database connection setup (replace the below connection string with your actual database URL)
DATABASE_URL = os.getenv('DATABASE_URL')
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

def format_large_numbers(value):
    """Formats large numbers to avoid scientific notation (if necessary)"""
    try:
        # Attempt to convert to float and return the value as a number (no scientific notation)
        value = float(value)
        return '{:.0f}'.format(value) if value == int(value) else str(value)
    except (ValueError, TypeError):
        # If the value is not a number, return it as is
        return value

def fetch_pages_with_document_and_fields(file_id):
    # Fetch all pages for the given file_id and order them by pageNumber
    pages = session.query(Page).filter(Page.fileId == file_id).order_by(Page.pageNumber).all()

    if not pages:
        print(f"No pages found for fileId {file_id}")
        return

    # Fetch the file name using the fileId
    file = session.query(File).filter(File.fileId == file_id).first()
    file_name = file.fileName if file else "Unknown"

    output_csv = f"pages_for_file_{file_id}.csv"
    with open(output_csv, mode="w", newline="") as file:
        writer = csv.writer(file)

        # Write headers including fileName
        writer.writerow([
            "fileId", "fileName", "pageId", "pageNumber", "documentName", 
            "fieldName", "fieldValue", "confidenceScore"
        ])

        # Iterate over each page and fetch associated document and field details
        for page in pages:
            # Fetch the document based on documentId
            document = session.query(Document).filter(Document.documentId == page.documentId).first()
            document_name = document.documentName if document else "Unknown"

            # Fetch all fields associated with the page
            fields = session.query(DocumentField).filter(DocumentField.pageId == page.pageId).all()

            # If there are no fields, write page info without field details
            if not fields:
                writer.writerow([
                    page.fileId, file_name, page.pageId, page.pageNumber, document_name, 
                    "No field", "No value", "No confidence"
                ])
            else:
                # Write page and field details for each field, applying formatting for large numbers
                for field in fields:
                    field_value_formatted = format_large_numbers(field.fieldValue)
                    confidence_score_formatted = format_large_numbers(field.confidenceScore)

                    writer.writerow([
                        page.fileId, file_name, page.pageId, page.pageNumber, document_name, 
                        field.fieldName, field_value_formatted, confidence_score_formatted
                    ])

    print(f"CSV file created: {output_csv}")

def process_multiple_file_ids(file_ids):
    # Iterate over each file_id and generate the CSV
    for file_id in file_ids:
        try:
            file_id_int = int(file_id.strip())  # Ensure each input is a valid integer and strip any extra spaces
            fetch_pages_with_document_and_fields(file_id_int)
        except ValueError:
            print(f"Invalid fileId: {file_id}")

if __name__ == "__main__":
    # Input multiple fileIds as a comma-separated list
    file_ids_input = input("Enter the fileIds (comma-separated): ")
    
    # Split the input by commas and process each fileId
    file_ids = file_ids_input.split(",")
    process_multiple_file_ids(file_ids)
