#1.Downloading dataset which is been automated using the code
import time
import datetime
import os
import pandas as pd
import sqlite3
from configparser import ConfigParser
config=ConfigParser()
config.read(r".\config.ini")
noofcomp=int(config['number_of_company']['numberofcomp'])
#we are defaultly setting the time period from 21-03-2021 to 21-04-2023
period_start=int(time.mktime(datetime.datetime(2021,3,21,23,59).timetuple()))
period_stop=int(time.mktime(datetime.datetime(2023,4,21,23,59).timetuple()))
directory="Data"
parent_dir="./"
path=os.path.join(parent_dir,directory)
os.mkdir(path)
for i in range(1,noofcomp+1):
    comp=config['company'][str(i)]
    query_string=f'https://query1.finance.yahoo.com/v7/finance/download/{comp}?period1={period_start}&period2={period_stop}&interval=1d&events=history&includeAdjustedClose=true'
    df=pd.read_csv(query_string)
    df['Company_Name']=comp
    df.to_csv(fr'.\Data\{comp}.csv',index=False)

#loading all the downloaded csv file to a single dataframe
finaldf=pd.DataFrame(columns=['Date','Open','High','Low','Close','Adj Close','Volume','Company_Name'])
path_dir=r".\Data"
for files in os.listdir(path_dir):
    df=pd.read_csv(path_dir+'\\'+files)
    finaldf=pd.concat([finaldf,df])
finaldf['Date']=finaldf['Date'].astype(str)
finaldf['Date']=pd.to_datetime(finaldf['Date'])

#Loading data all to a single DataBase
conn=sqlite3.connect(r'.\finance.db')
createtable="CREATE TABLE IF NOT EXISTS Stocks (Date Date,Open FLOAT, High FLOAT, Low FLOAT, Close FLOAT, Adj_Close FLOAT, Volumne INTEGER,Company_Name TEXT)"
cursor=conn.cursor()
finaldf.to_sql('Stocks',con=conn,index=False)
cursor.execute(createtable)

#importing flask for api creation
from flask import Flask,request,json
app=Flask(__name__)
@app.route('/')
def index():
    return 'Home Page'

#Get all companies stock data for a particular day
@app.route('/filter/<Date>')
#date reciving format YYYY-MM-DD
def filter_day(Date):
    conn=sqlite3.connect(r'.\finance.db')
    cursor=conn.cursor()
    selectquery=f"SELECT * FROM Stocks WHERE Date='{Date} 00:00:00'"
    cursor.execute(selectquery)
    result=cursor.fetchall()
    tempdf=pd.DataFrame(result,columns=['Date','Open','High','Low','Close','Adj Close','Volume','Company_Name'])
    table=tempdf.to_html()
    return table

#Get all companies stock data for a particular company and a particular day
#Date=YYYY-MM-DD(2022-03-21)
#Company="IBM"
@app.route('/filter/<Date>/compname/<Company>')
def filter_day_Company(Date,Company):
    conn=sqlite3.connect(r'.\finance.db')
    cursor=conn.cursor()
    selectquery=f"SELECT * FROM Stocks WHERE Date='{Date} 00:00:00' AND Company_Name='{Company}'"
    cursor.execute(selectquery)
    result=cursor.fetchall()
    tempdf=pd.DataFrame(result,columns=['Date','Open','High','Low','Close','Adj Close','Volume','Company_Name'])
    table=tempdf.to_html()
    return table

#Get all companies stock data for a particular company 
#Company="IBM"
@app.route('/filter/compname/<Company>')
def filter_Company(Company):
    conn=sqlite3.connect(r'.\finance.db')
    cursor=conn.cursor()
    selectquery=f"SELECT * FROM Stocks WHERE Company_Name=='{Company}'"
    cursor.execute(selectquery)
    result=cursor.fetchall()
    tempdf=pd.DataFrame(result,columns=['Date','Open','High','Low','Close','Adj Close','Volume','Company_Name'])
    table=tempdf.to_html()
    return table     

#POST/Patch API to update stock data for a company by date.
#postman is used to pass value to the API
@app.route('/update/<Date>/company/<Company>',methods=['POST'])
def update(Date,Company):
    conn=sqlite3.connect(r'.\finance.db')
    cursor=conn.cursor()
    data=json.loads(request.data)
    Open=data['Open']
    High=data['High']
    Low=data['Low']
    Close=data['Close']
    Adj_Close=data['Adj_Close']
    Volume=data['Volume']
    updatesquery=f"UPDATE Stocks SET Open={Open},High={High},Low={Low},Close={Close} WHERE Date='{Date} 00:00:00' AND Company_Name='{Company}'"
    cursor.execute(updatesquery)
    selectquery_update=f"SELECT * FROM Stocks WHERE Date='{Date} 00:00:00' and Company_Name='{Company}'"
    cursor.execute(selectquery_update)
    result=cursor.fetchall()
    tempdf=pd.DataFrame(result,columns=['Date','Open','High','Low','Close','Adj Close','Volume','Company_Name'])
    table=tempdf.to_html()
    return table 

if __name__=='__main__':
    app.run()