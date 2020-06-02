"""
This program is to do the ETL process for the data on the stock price. 
In this program, we target the stock of the Apple company.
Thus, one might modify this configuration for his/her own need.
Programmer: Phuong Van Nguyen
phuong.nguyen@economics.uni-kiel.de
"""

import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import time
import sqlite3

api_key = 'EQ66HWXJM8SYO3XY'
name_stock='MSFT'

class stock_extract():
    
    def __init__(self):
        print('1. Extract data on ' +name_stock+ ' from the Internet....')
        self.api_key=api_key
        self.name_stock=name_stock
        self.data=self.extract(self.api_key,self.name_stock)
        print('You have just successfully extracted data on ' +name_stock+ ' from the Internet.\nLooking at them below.')
        display(self.data.head(5))
          
    def extract(self,api_key,name_stock):
        ts = TimeSeries(key=api_key, output_format='pandas')
        self.data, self.meta_data = ts.get_intraday(symbol=self.name_stock, 
                                                    interval = '1min', outputsize = 'full')
        return self.data
    
class transform():
   
    def __init__(self,stock_extract):
        print('2. Transform the extracted data on ' +name_stock+' ....')
        self.data=stock_extract.data
        
        print('The data on the stock ' +name_stock +' after renaming columns:')
        self.renam_data=self.rename(self.data)
        display(self.renam_data.head(5))
        
        print('The data on the stock ' +name_stock +' after reseting index:')
        self.resetid_data=self.reset_index(self.renam_data)
        display(self.resetid_data.head(5))
        
        print('The data on the stock ' +name_stock +' after turing values into the string format:')
        self.string_data=self.to_string(self.resetid_data)
        display(self.string_date.head(5))
        
        print('The prepared data on the stock '+name_stock+' for loading to the SQL database')
        self.prepared_data=self.convert_list(self.string_data)
        display(self.prepared_data[0:5])
        
    def rename(self,data):
        self.renam_data=data.rename(columns={'1. open': 'open', '2. high':'high',
                    '3. low':'low','4. close': 'close','5. volume':'volume'})
        return self.renam_data
    
    def reset_index(self,data):
        self.resetid_data=data.reset_index()
        return self.resetid_data
    
    def to_string(self,data):
        self.string_date=data.astype(str)
        return self.string_date
    
    def convert_list(self,data):
        self.prepared_data=data.values.tolist()
        return self.prepared_data
        
class load():
    
    def __init__(self,transform):
        print('3. Load the transformed data on ' +name_stock+ ' to the SQL database....')
        self.tolist=transform.prepared_data
        self.connect = sqlite3.connect('Phuong_session.db')
        self.mycursor = self.connect.cursor() 
        print('You have just opened the database successfully:')
        print(self.mycursor )
        print('The existing Tables in this database:');
        self.mycursor.execute('''SELECT name FROM sqlite_master WHERE type='table' ''')
        print(self.mycursor.fetchall())
        
        
        self.mycursor.execute('''DROP TABLE IF EXISTS MSFT''')
        self.mycursor.execute(''' CREATE TABLE MSFT
         (Time TEXT  NOT NULL,
         Open TEXT  NOT NULL,
         High TEXT  NOT NULL,
         Low TEXT  NOT NULL,
         Close TEXT  NOT NULL,
         Volumne TEXT  NOT NULL);''')      
        print('You have just successfully created a new '+ name_stock+ ' table')
        
        
        self.mycursor.executemany("INSERT INTO MSFT(Time, Open, High, Low, Close, Volumne) VALUES (?,?,?,?,?,?)",self.tolist)
        print('You have just inserted the transformed data into the new '+ name_stock+ ' table successfully.')
        print('Check the first 5 observations in the new '+ name_stock+ ' table')
        self.rows = self.mycursor.execute('SELECT * from MSFT LIMIT 5')
        for row in self.rows:
            print(row)

if __name__=='__main__':
    load(transform(stock_extract()))
   
