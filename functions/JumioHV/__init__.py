"""
Created on Wed Oct 19 11:08:32 2022

@author: HaroldFerneyGomez
"""
import logging
import azure.functions as func
from azure.storage.blob import BlobSasPermissions, generate_blob_sas, BlobServiceClient
import requests
from requests.auth import HTTPDigestAuth

def main(req: func.HttpRequest) -> func.HttpResponse:
    name = req.get_json().get('name')
    dateFly = req.get_json().get('dateFly')#"2022-10-01T17:30:08"
    endFly = req.get_json().get('endFly')
    gender = req.get_json().get('gender')#"male"
    image_url = req.get_json().get('image_url')
    
    newEmployee = searchLastEmployee()
    addPerson(newEmployee,name,dateFly,endFly,gender)
    agregarFaceData(newEmployee,name,image_url)
    return func.HttpResponse("Good")

def searchLastEmployee():
    url = 'http://190.145.128.77/ISAPI/AccessControl/UserInfo/Search?format=json'
    data = {
    "UserInfoSearchCond":{
        "searchID":"3",
        "searchResultPosition": 0,
        "maxResults": 20
    }
    }
    employees =requests.post(url, auth=HTTPDigestAuth('admin', 'z8Vh-_6K4M-qwH6'),json=data).json()
    Listemployees = employees['UserInfoSearch']['UserInfo']
    compara = 0
    for NEMpleado in Listemployees:
        if int(NEMpleado['employeeNo'])>compara:
            compara = int(NEMpleado['employeeNo'])          
    newEmployee=compara+1
    return(newEmployee)

def addPerson(newEmployee,name,dateFly,endFly,gender):
    url = 'http://190.145.128.77/ISAPI/AccessControl/UserInfo/Record?format=json'
    data = {
    "UserInfo": {
        "employeeNo": str(newEmployee),
        "name": name,
        "userType": "visitor",
        "Valid": {
            "enable": True,
            "beginTime": dateFly,
            "endTime": endFly,
            "timeType": "local"
        },
        "RightPlan": [
            {
                "doorNo": 1,
                "planTemplateNo": "1"
            }
        ],
        "password": "12345",
        "doorRight": "1",
        "userVerifyMode": "face",
        "addUser": True,
        "gender": gender,
        "PersonInfoExtends": [
        {
            "name": "user",
            "value": "Creado por Function"
        }
        ]
    }
    }
    print(requests.post(url, auth=HTTPDigestAuth('admin', 'z8Vh-_6K4M-qwH6'),json=data).json())

def agregarFaceData(newEmployee,name,image_url):
    url = "http://190.145.128.77/ISAPI/Intelligent/FDLib/FaceDataRecord?format=json"
    data = {
        "faceURL":image_url,
        "faceLibType": "blackFD",
        "FDID": "1",
        "FPID":str(newEmployee),
        "name": name,
        "bornTime": "2004-05-03"
    }
    print(requests.post(url, auth=HTTPDigestAuth('admin', 'z8Vh-_6K4M-qwH6'),json=data).json())