import azure.functions as func
import logging
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
import requests
from azure.storage.blob import ContainerClient
import pandas as pd
from io import BytesIO
from xlsxwriter import Workbook

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="input",
                               connection="04f823_STORAGE") 
def blob_trigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    
    #end point for Document Intelligence
    endpoint = ""
    key = ""

    document_analysis_client = DocumentAnalysisClient(
        endpoint=endpoint, credential=AzureKeyCredential(key)
    )

    #setting up functions
    def analyze_pdf(blob):
        pdf_content = blob.read()

        poller = document_analysis_client.begin_analyze_document("prebuilt-invoice", pdf_content)

        return poller.result()

    def extract_table_data(analyze_result):
        tables = []
        for table in analyze_result.tables:
            table_data = [["" for _ in range(table.row_count)] for _ in range(table.column_count)]
            
            for cell in table.cells:
                row_index = cell.row_index
                col_index = cell.column_index
                text = cell.content
                table_data[row_index][col_index] = text

            tables.append(pd.DataFrame(table_data))

        return tables

    # Azure Container details
    output_container_sas_url = ""

    output_container_client = ContainerClient.from_container_url(output_container_sas_url)

    def save_to_blob(output_container_client, blob_name, excel_data):
    # Create a blob client for the output file
        output_blob_client = output_container_client.get_blob_client(blob_name)

        # Upload the Excel data
        output_blob_client.upload_blob(excel_data, blob_type="BlockBlob", overwrite=True)

    analyze_result = analyze_pdf(myblob)

    if analyze_result:
        tables = extract_table_data(analyze_result)

        # Save the Excel file to a BytesIO object
        excel_stream = BytesIO()
        with pd.ExcelWriter(excel_stream, engine='xlsxwriter') as writer:
            for i, table in enumerate(tables):
                table.to_excel(writer, sheet_name=f"Table {i+1}", index=False)
        excel_stream.seek(0)

        segments = myblob.name.split('/')

        # Construct output blob name
        output_blob_name = f"{segments[len(segments) - 1].split('.')[0]}_output.xlsx"

        # Save the Excel file to Blob Storage
        save_to_blob(output_container_client, output_blob_name, excel_stream)

        print(f"{myblob.name} exported to Azure Blob Storage successfully.")
    else:
        print(f"Failed to analyze {myblob.name}.")


    

    

