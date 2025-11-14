import os
import json
import datetime
from pathlib import Path

from pypdf import PdfReader

import chromadb
# from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction

# Add logging to improve visiblity

client = chromadb.HttpClient(
    host='chromadb_server', 
    port=8000,
    settings=chromadb.config.Settings(
        allow_reset=True, 
        anonymized_telemetry=False
    )
)

collection = client.get_or_create_collection(
    name="vehicle-docs",
    metadata={
        "description": "Storage for indexing vehicle pdf files"
    }
    # embedding_function=OpenAIEmbeddingFunction(
    #     api_key=os.getenv("OPENAI_API_KEY"),
    #     model_name="text-embedding-3-small"
    # )
)

docs_path = Path("/app/docs")

with open("/data/doc_map.json") as file:
    META = json.load(file)

if not docs_path.exists():
    exit(0)

for pdf_file in docs_path.glob("*.pdf"):
    modified_time = datetime.datetime.fromtimestamp(pdf_file.stat().st_mtime)

    reader = PdfReader(pdf_file)
    # text = "\n".join([page.extract_text() or "" for page in reader.pages])

    for page in reader.pages:
        if not page.extract_text().strip():
            continue

        doc_id = pdf_file.stem

        try:
            client.get_collection(
                "vehicle-docs"
            ).delete(
                ids=[doc_id]
            )
        except Exception:
            pass

        file_meta = list(filter(lambda x: x.get("file") == pdf_file.name, META))[0]

        collection.add(
            ids=[doc_id],
            documents=[page.extract_text()],
            metadatas=[{
                "filename": pdf_file.name,
                "vehicle": file_meta.get("vehicle"),
                "model": file_meta.get("model", "Unknown"),
                "year": file_meta.get("year", "Unknown"),
                "type": file_meta.get("type"),
                "page": page.page_number,
                "modified": modified_time.isoformat()
            }]
        )
