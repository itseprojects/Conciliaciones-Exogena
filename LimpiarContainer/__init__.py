import re
import os
import logging
import azure.functions as func
from tabula.io import read_pdf
import pandas as pd
from azure.storage.blob import BlobServiceClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    Cliente = req.get_json().get('Cliente')
    account_name = 'itseblobdev' #'probepython'
    account_key = 'd9sOh0WeqvVF66NQnyWKZWFL/KDje0LizX8UyFWpWX39lLX2C8fxnqRtYD2lOFvNp6aaayQsAq7T+AStvsHyew==' #Zas0npJX9ryEm4hmW/gatWr8aI91oOCvt+qbQKqWrZJCmhv5qh6S/w6ittYYaDBRjRnoxa0h+A8H+ASttcvrrQ=='
    blob_service_client = BlobServiceClient(account_url = f'https://{account_name }.blob.core.windows.net/', credential = account_key)
    container_client = blob_service_client.get_container_client(container = Cliente)
    # Enumerar todos los blobs en el contenedor
    blobs = container_client.list_blobs()

    # Eliminar los blobs a excepción de los que contengan las palabras
    for blob in blobs:
        if (blob.name.__str__().lower().__contains__('renta')) | (blob.name.__str__().lower().__contains__('balance')) | (
            blob.name.__str__().lower().__contains__('formato') | (blob.name.__str__().lower().__contains__('base'))) | (
            blob.name.__str__().lower().__contains__('municipio')| (blob.name.__str__().lower().__contains__('gif'))):
            pass
        else: container_client.delete_blob(blob.name)

    return func.HttpResponse("Contenedor limpio más no vacío")

