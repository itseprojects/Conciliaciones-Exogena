"""
Created on Wed Oct 19 11:08:32 2022

@author: HaroldFerneyGomez
"""
from asyncore import read
from cmath import nan
import logging
import azure.functions as func

from datetime import datetime, timedelta
import urllib.parse
import requests
from requests.auth import HTTPDigestAuth

def main(req: func.HttpRequest) -> func.HttpResponse:
    name = req.get_json().get('name')
    dateFly = req.get_json().get('dateFly')#"2022-10-01T17:30:08"
    endFly = req.get_json().get('endFly')
    gender = req.get_json().get('gender')#"male"
    image_url = req.get_json().get('image_url')
    disp_url = req.get_json().get('disp_url')#"male"
    user = req.get_json().get('user')
    passw = req.get_json().get('passw')
    
    newEmployee = searchLastEmployee(disp_url,user,passw)
    addPerson(newEmployee,name,dateFly,endFly,gender,disp_url,user,passw)
    agregarFaceData(newEmployee,name,image_url,disp_url,user,passw)
    return func.HttpResponse("pasó bien")

def searchLastEmployee(disp_url,user,passw):
    url = disp_url + '/ISAPI/AccessControl/UserInfo/Search?format=json'
    data = {
    "UserInfoSearchCond":{
        "searchID":"3",
        "searchResultPosition": 0,
        "maxResults": 20
        }
    }
    employees =requests.post(url, auth=HTTPDigestAuth(user, passw),json=data).json()
    Listemployees = employees['UserInfoSearch']['UserInfo']
    compara = 0
    if bool(Listemployees):
        for NEMpleado in Listemployees:
            if int(NEMpleado['employeeNo'])>compara:
                compara = int(NEMpleado['employeeNo'])          
        newEmployee=compara+1
    else: newEmployee = 1
    
    return(newEmployee)

def addPerson(newEmployee,name,dateFly,endFly,gender,disp_url,user,passw):
    url = disp_url + '/ISAPI/AccessControl/UserInfo/Record?format=json'
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
    print(requests.post(url, auth=HTTPDigestAuth(user, passw),json=data).json())

def agregarFaceData(newEmployee,name,image_url,disp_url,user,passw):
    url = disp_url + "/ISAPI/Intelligent/FDLib/FaceDataRecord?format=json"
    data = {
        "faceURL":image_url,
        "faceLibType": "blackFD",
        "FDID": "1",
        "FPID":str(newEmployee),
        "name": name,
        "bornTime": "2004-05-03"
    }
    print(requests.post(url, auth=HTTPDigestAuth(user, passw),json=data).json())