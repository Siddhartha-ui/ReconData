import pandas as pd
import HDOperation as hd
from dboperation import dboperation
import vaex
import streamlit as st

class Upload(object) :
    
    def __init__(self, FileObj = "", user ="", date = "", customfilename = "") -> None:
         
         __dbop = dboperation(st.session_state.user_name)
         self.dbop = __dbop
         self.FileObj = FileObj
         self.user = user
         self.date = date
         self.customfilename = customfilename

         if "df" not in st.session_state :
             st.session_state.df = None
          
    
    def DUCKDB_to_TBL(self) -> pd.DataFrame :
        df = self.dbop.showalltbl()
        return df 

    def File_to_Dataframe(self) -> pd.DataFrame :
        
        
        def load_data(fileobj):
            
            file = str(fileobj.name)
            fileExt = str(file[-3:])

            
            if fileExt.upper() == "LSX" :
                
                df = pd.read_excel(fileobj, index_col= False )
                df = df.replace(',', '', regex=True) 
                df = df.applymap(str)
                
            elif fileExt.upper() == "CSV" :
                
                df = pd.read_csv(fileobj, index_col= False)
                df = df.replace(',', '', regex=True)
                df = df.applymap(str)
                
            return df

        
        df = load_data(self.FileObj)
        st.session_state.df = df
        return st.session_state.df         
        
    

    def CSV_to_DUCKDB(self) :

        if len(self.customfilename.strip()) == 0 :
            fileNM = str(self.FileObj.name)
        else :
           fileNM = self.customfilename.strip()      

        def tbl_nm_prpare(tbl : str) -> str :
            tbl = tbl.replace("\\", '').replace(".", '').replace("-",'').replace(" ",'').replace("_",'').replace("(" , '').replace(")",'').replace("&",'') 
            return str(tbl) 
        
        @st.cache_data
        def load_data(fileobj):
            
            file = str(fileobj.name)
            fileExt = str(file[-3:])
            
            
            if fileExt.upper() == "LSX" :
                
                df = pd.read_excel(fileobj, index_col= False )
                df = df.replace(',', '', regex=True) 
                # cols = list(df.columns)
                # df = df.applymap(str)
                
                # df[cols] = pd.to_numeric(df[cols].values.tolist(), errors= 'ignore')

            elif fileExt.upper() == "CSV" :
                
                df = pd.read_csv(fileobj, index_col= False)
                df = df.replace(',', '', regex=True)
                # cols = list(df.columns)
                # df = df.applymap(str)
                # df[cols] = pd.to_numeric(df[cols].values.tolist(), errors= 'ignore')
                
            return df

        def FileScrubbing(df : pd.DataFrame) -> pd.DataFrame :
                
                df.columns = df.columns.str.replace(' ','')
                df.columns = df.columns.str.replace('-','')
                
                return df 
        
        df = load_data(self.FileObj)         
       
        df = FileScrubbing(df)
        
        tbl_name = tbl_nm_prpare(fileNM)
        
        self.dbop.Createtbl(df,tbl_name)
        
        