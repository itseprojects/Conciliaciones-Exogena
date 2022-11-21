from cmath import nan
import logging
from operator import index

import azure.functions as func

from datetime import datetime, timedelta
import pandas as pd
from azure.storage.blob import BlobSasPermissions, generate_blob_sas, BlobServiceClient
import urllib.parse
from math import ceil
from io import BytesIO

account_name = 'probepython'
account_key = 'Zas0npJX9ryEm4hmW/gatWr8aI91oOCvt+qbQKqWrZJCmhv5qh6S/w6ittYYaDBRjRnoxa0h+A8H+ASttcvrrQ=='
container_name = 'test'

blob_name_DB = 'BASE DE DATOS EXELTIS.xls'
HeaderHojaDB = 0
nombreHojaDB="Sheet1"

def main(req: func.HttpRequest) -> func.HttpResponse:
    blob_name = req.get_json().get('Balance')
    HeaderHojaBalance=11
    nombreHojaBalance= "Hoja 1"
    ColumnaValorIngreso = "2022/08" if blob_name=='BALANCE ENERO A AGOSTO 2022.xlsx' else "2021/12"
    blob_name_to_save = 'HG_'+blob_name
    logging.info('Python HTTP trigger function processed a request.')
    message = req.get_json().get('Balance')
    try:
        blob_service_client = BlobServiceClient(account_url = f'https://{account_name }.blob.core.windows.net/', credential = account_key)
        try:
            blob_client = blob_service_client.get_blob_client(container = container_name, blob = blob_name_to_save)
            blob_client.delete_blob()
        except:
            pass
        blob_client = blob_service_client.get_blob_client(container = container_name, blob = blob_name)
        downloader = blob_client.download_blob()
        Datos = pd.read_excel(downloader.readall(), sheet_name=nombreHojaBalance, header=HeaderHojaBalance)
        Datos = Datos[~Datos['Cuentas'].isnull()]
        DatosSeparados = separarCuentas(Datos)
        Totales = ObtenerIngresos(DatosSeparados,ColumnaValorIngreso)
        IngresosClientes4135 = UnificarClientesPorCuenta(DatosSeparados,413500,417000,"Ingresos Brutos Recibidos",ColumnaValorIngreso)
        IngresosClientes4175 = UnificarClientesPorCuenta(DatosSeparados,417500,417600,"Devoluciones",ColumnaValorIngreso)
        IngresosClientes4001 = pd.merge(IngresosClientes4135,IngresosClientes4175,on=['Numero de identificacion','Razón social'],how='outer').fillna(0)
        IngresosClientes4001.insert(0,'Concepto', 4001)
        IngresosClientes4002 = UnificarClientesPorCuenta(DatosSeparados,420000,430000,"Ingresos Brutos Recibidos",ColumnaValorIngreso).fillna(0)
        IngresosClientes4002.insert(0,'Concepto', 4002)
        IngresosClientes4002.insert(2,'Devoluciones', 0)
        Ingresos = pd.concat([IngresosClientes4001,IngresosClientes4002])
        Ingresos = Ingresos.sort_values(by = ['Concepto','Ingresos Brutos Recibidos'], ascending = [True, True], na_position = 'last',ignore_index=True)
       
        ValorTotalIngresos= Ingresos["Ingresos Brutos Recibidos"].sum()+Ingresos["Devoluciones"].sum()
        
        blob_client = blob_service_client.get_blob_client(container = container_name, blob = blob_name_DB)
        downloader = blob_client.download_blob()
        BD = pd.read_excel(downloader.readall(), sheet_name=nombreHojaDB, header=HeaderHojaDB)
        
        Formato1007 = BuscarId(Ingresos,BD)
        abs_container_client = blob_service_client.get_container_client(container=container_name)
        buffer = BytesIO()
        excel_buf = Formato1007.to_excel(sheet_name="Formato1007",excel_writer=buffer,index=False)

        blockBlob = abs_container_client.upload_blob(name=blob_name_to_save,data=buffer.getvalue())

    except Exception as e:
        logging.info(e)

        return func.HttpResponse(
                f"!! Ocurrió un error en la ejecución. \n\t {e} ",
                status_code=200
        )
    return func.HttpResponse(str(ValorTotalIngresos))

def BuscarId(dfBalance,bd):
    TiposDoc={'C':13,'E':31,'N':31,'O':43,'X':31}
    ListadoIds = dfBalance["Numero de identificacion"].tolist()
    vectorCoincidencias = []
    vectorPrimerApellido = []
    vectorSegundoApellido = []
    vectorNombre = []
    vectorOtrosNombres = []
    bd['Código']=bd['Código'].apply(lambda x: x.strip())
    for Id in ListadoIds:
        try:
            tipoId = bd[(bd['Código']==str(Id))]['Tipo de identificación'].iloc[0]
            NumeroTipoID = TiposDoc[tipoId]
            if NumeroTipoID == 13:
                nombrecompleto = dfBalance[(dfBalance["Numero de identificacion"]==str(Id))]['Razón social'].iloc[0]
                if len(nombrecompleto.split())==1:
                    vectorPrimerApellido.append(nombrecompleto)
                    vectorSegundoApellido.append("")
                    vectorNombre.append("")
                    vectorOtrosNombres("")
                elif len(nombrecompleto.split())==2:
                    vectorPrimerApellido.append(nombrecompleto.split()[0])
                    vectorSegundoApellido.append(nombrecompleto.split()[1])
                    vectorNombre.append("")
                    vectorOtrosNombres("")
                elif len(nombrecompleto.split())==3:
                    vectorPrimerApellido.append(nombrecompleto.split()[0])
                    vectorSegundoApellido.append(nombrecompleto.split()[1])
                    vectorNombre.append(nombrecompleto.split()[2])
                    vectorOtrosNombres.append("")
                elif len(nombrecompleto.split())>3:
                    vectorPrimerApellido.append(nombrecompleto.split()[0])
                    vectorSegundoApellido.append(nombrecompleto.split()[1])
                    vectorNombre.append(nombrecompleto.split()[2])
                    vectorOtrosNombres.append(nombrecompleto.split(maxsplit=3)[3])
            else:
                vectorPrimerApellido.append("")
                vectorSegundoApellido.append("")
                vectorNombre.append("")
                vectorOtrosNombres.append("")
            vectorCoincidencias.append(TiposDoc[tipoId])
        except Exception:
            vectorCoincidencias.append("NE")  
            vectorPrimerApellido.append("")
            vectorSegundoApellido.append("")
            vectorNombre.append("")
            vectorOtrosNombres.append("")
    dfBalance.insert(1,'Tipo de documento', vectorCoincidencias)
    dfBalance.insert(4,'Pais', 169)
    dfBalance.insert(3,'Otros nombres', vectorOtrosNombres)
    dfBalance.insert(3,'Primer nombre', vectorNombre)
    dfBalance.insert(3,'Segundo apellido', vectorSegundoApellido)
    dfBalance.insert(3,'Primer apellido', vectorPrimerApellido)
    return dfBalance

def separarCuentas(df):
    cuentas=df["Cuentas"]
    VectorCuentas = []
    VectorNits = []
    VectorNombres = []
    for cuenta in cuentas:  
        try:
            nit,name = cuenta.split(maxsplit=1)
        except Exception:
            try:
                cta = int(str(cuenta).strip().replace('.', ''))
            except Exception:
                cta = 0
            name = ""
            nit = ""
        VectorNombres.append(name)
        VectorCuentas.append(cta)
        VectorNits.append(nit)
    df.insert(1,"Razón social",VectorNombres)
    df.insert(1,"NIT",VectorNits)
    df.insert(1,"NumeroCuenta",VectorCuentas)
    return df

def ObtenerIngresos(df,ColumnaValorIngreso):
    ingresosAO = df[(df['NumeroCuenta']>413499)&(df['NumeroCuenta']<417000)&(df['NIT']!="")][ColumnaValorIngreso].sum()
    print('4135+55',': ',ingresosAO)#,df[(df['NumeroCuenta']==4135)].iloc[0]['             Descripción                                '].strip()
    devoluciones= df[(df['NumeroCuenta']<417600)&(df['NumeroCuenta']>417499)&(df['NIT']!="")][ColumnaValorIngreso].sum()
    print('4175',': ',devoluciones)#df[(df['NumeroCuenta']==4175)].iloc[0]['             Descripción                                '].strip(),
    ingresosO = df[(df['NumeroCuenta']<430000)&(df['NumeroCuenta']>420999)&(df['NIT']!="")][ColumnaValorIngreso].sum()
    print('42',': ',ingresosO)#df[(df['NumeroCuenta']==42)].iloc[0]['             Descripción                                '].strip(),
    print('TOTAL INGRESOS :',ingresosAO+devoluciones+ingresosO)
    return ingresosAO+devoluciones+ingresosO

def UnificarClientesPorCuenta(df,limiteInferiorCta,LimiteSuperiorCta,nombreDatoColumna,ColumnaValorIngreso):
    datosPorCliente = pd.DataFrame()
    listaSoloClientesIngreso = df[(df['NIT']!="")&(df['NumeroCuenta']>limiteInferiorCta)&(df['NumeroCuenta']<LimiteSuperiorCta)]
    listaUnica = listaSoloClientesIngreso["Razón social"].unique().tolist()
    VectorNombres = []
    VectorIdentificacion = []
    VectorTotales = []
    for cliente in listaUnica:
        total = listaSoloClientesIngreso[(listaSoloClientesIngreso['Razón social']==cliente)][ColumnaValorIngreso].sum()
        VectorIdentificacion.append(df[df['Razón social']==cliente]['NIT'].iloc[0])
        VectorNombres.append(cliente)
        VectorTotales.append(total)
    datosPorCliente.insert(0,"Razón social",VectorNombres)
    datosPorCliente.insert(0,"Numero de identificacion",VectorIdentificacion)
    datosPorCliente.insert(2,nombreDatoColumna,VectorTotales)
    return datosPorCliente

   


