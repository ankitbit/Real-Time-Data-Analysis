
import win32com.client as win32
import time
import random

from os import listdir
from os.path import isfile, join

emailspath = "C:/Users/joangj/OneDrive - NETMIND/BTS - PySpark/2020/RealTime/Example1/Users"
attachmentspath = "C:/Users/joangj/OneDrive - NETMIND/BTS - PySpark/2020/RealTime/Example1/Attachments"



def sendemail(email):
    outlook = win32.Dispatch("outlook.application")
    mail = outlook.CreateItem(0)
    mail.To = email
    mail.Subject = "Hello! New offer from BTS"
    mail.Body = "Generic Body template"
    mail.Attachments.Add( attachmentspath + "/attach" + str(random.randint(1,3)) + ".jpg")
    mail.Send()
    print("Email sent to: " + email)
    
listoffiles = set()
listofemails = set()


while True:
    onlyfiles = [f for f in listdir(emailspath) if isfile(join(emailspath,f))]
    for file in onlyfiles:
        if file not in listoffiles:
            f = open(join(emailspath, file), "r")
            emails = f.readlines() # ["asdf@gmail.com", "asdfasdf2@gmail.com"]
            for email in emails:
                if email not in listofemails:
                    sendemail(email = email)
                    listofemails.add(email)
            listoffiles.add(file)
            f.close()
    time.sleep(5)
    

# https://www.youtube.com/watch?v=lOIJIk_maO4
# pyinstaller







