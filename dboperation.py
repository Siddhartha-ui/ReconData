import duckdb as db
import pandas as pd
import pyarrow as pa
import vaex
import streamlit as st


class dboperation(object):

    def __init__(self, user ) -> None:

        __user_name =  str(user)
        __user_db = f'UserProfile\\{__user_name}.db'
        __global_db = 'UserProfile\\datastore.db'
        db.default_connection.execute("SET GLOBAL pandas_analyze_sample=100000")    
        __conn = db.connect(__user_db)
        __connGlobal = db.connect(__global_db)
        self.con = __conn
        self.conglobal = __connGlobal
        self.DbDatastore = __global_db
        self.user = user    

    def __IsNotvalidDataSetName(self, tbl : str)-> bool :
            return tbl[0].isdigit()

    def __IsViewInternal(self, v_name: str) -> bool  :
            
            df = self.con.sql(f"""
                        SELECT view_name FROM duckdb_views()
                        where internal = 0
                        and view_name = '{v_name}'
                                     
                    
                    """).df()
            
            return not df.empty

    def getrelation(self, tbl : str)  :
        rel = self.con.sql(f"""
                    select * from {tbl}
                 """)
        return rel
    
    def getpl(self,tbl : str) -> pd.DataFrame :
        return self.con.sql(f"""
            select * from {tbl}
        """).df()

    def cretetblfromglobaltoloc(self, tbl : str):
        df_t = self.conglobal.sql(f"""
            select * from {tbl}
        """).df()

        self.con.execute(f"""
            create or replace table {tbl} As select * from df_t
        """)
        self.con.commit()

    

    def duck_rel(self,tbl : str, cols : str, filter : str, aggr : str, grp : str) -> pd.DataFrame :
    
        rel = self.con.execute(f"""
                    select * from {tbl}
                 """)
        
        if len(cols) > 0 and len(filter) > 0 and len(aggr) > 0 :
            rel = rel.project(cols).filter(filter).aggregate(aggr,group_expr=grp )
        elif len(cols) > 0 and len(filter) > 0 and len(aggr) == 0  : 
            rel = rel.project(cols).filter(filter)
        elif len(cols) == 0 :
            rel = rel 

        return rel.to_df()    

    def drop_view(self, v : str) :
        
        if self.__IsViewInternal(v) :
           
           self.con.execute(f"""
                    drop view {v}
            """) 
    
    def droptable(self, tbl : str) :

        try :    
            self.con.execute(f"""
                    drop view {tbl}
            """)

        except :
            self.con.execute(f"""
                    drop table {tbl}
            """)    

    def Createtbl(self, df: pd.DataFrame, tbl : str) :
        
        if self.__IsNotvalidDataSetName(tbl) :
          
          st.error("Cannot create a dataset starts with a number!") 
          st.stop()

        else :   

            df = df
            self.con.execute(f"""
                            create or replace table {tbl} AS select * from df
                                            
                        """)
            self.con.commit() 

    def showtblStruct(self, tbl : str) -> pd.DataFrame :
        df = self.con.sql(f"""
                        select * from {tbl}
                        where 1 =2 
                    
                    """).df()
        return df

    def getresult(self, query : str) -> pd.DataFrame :
        df = self.con.sql(f"""
                        {query}

                    """).df()
        
        return df

    def showquery(self, tbl : str, cols : str) -> str:

        ssql =  f"""
                select {cols} from {tbl}
        """
        return ssql

    


    def showtbl(self, tbl : str) -> vaex.DataFrame :
        arrow_table = self.con.execute(f"""
                        select * from {tbl}
                    
                    """).arrow()
        df = vaex.from_arrow_table(arrow_table)
        return df

    def showatbldef(self, tbl : str) -> pd.DataFrame :
        df = self.con.sql(f"""
                        DESCRIBE SELECT * FROM {tbl}
                    
                    """).df()
        return df

    def showalltbl(self) -> pd.DataFrame :

        try :
            df = self.con.sql(f"""
                        SHOW TABLES
                    
                    """).df()
        except :
            df = pd.DataFrame()    
        return df


    def showalltablesglobal(self) -> pd.DataFrame :

        try :
            df = self.conglobal.sql(f"""
                        SELECT table_name FROM duckdb_tables()
                    
                    """).df()
        except :
            df = pd.DataFrame()    
        return df

    def showallviews(self) -> pd.DataFrame :

        try :
            df = self.conglobal.sql(f"""
                        SELECT * FROM duckdb_views()
                        where internal = 0      
                    
                    """).df()
        except :
            df = pd.DataFrame()    
        return df
    
    def getviewquerygbls(self, v_name: str) -> str  :

            
            df = self.conglobal.sql(f"""
                        SELECT cast(sql as TEXT) as sql FROM duckdb_views()
                        where internal = 0
                        and view_name = '{v_name}'
                                     
                    
                    """).df()
            
            ssql = df['sql'].loc[df.index[0]]
            
            return ssql


    def getviewquery(self, v_name: str) -> str  :

            
            df = self.con.sql(f"""
                        SELECT cast(sql as TEXT) as sql FROM duckdb_views()
                        where internal = 0
                        and view_name = '{v_name}'
                                     
                    
                    """).df()
            
            ssql = df['sql'].loc[df.index[0]]
            
            return ssql

    def IsView(self, v_name: str) -> bool  :

            
            df = self.con.sql(f"""
                        SELECT view_name FROM duckdb_views()
                        where internal = 0
                        and view_name = '{v_name}'
                                     
                    
                    """).df()
            
            return not df.empty   


    def showalltblsgl(self, user : str) -> list :
        n = len(user)
        
        df = self.conglobal.sql(f"""
                        SELECT table_name FROM duckdb_tables()
                        where internal = 0
                        and table_name ilike '{user}%'              
                                     
                    
                    """).df()
            
        return df.values.tolist()

    def showallviewsgl(self, user : str) -> list :
        n = len(user)
        
        df = self.conglobal.sql(f"""
                        SELECT view_name FROM duckdb_views()
                        where internal = 0
                        and view_name ilike '{user}%'              
                                     
                    
                    """).df()
            
        return df.values.tolist()

    def showallviewsglobal(self) -> pd.DataFrame :

        try :
            df = self.conglobal.sql(f"""
                        SELECT view_name FROM duckdb_views()
                    
                    """).df()
        except :
            df = pd.DataFrame()    
        return df


    def showalltblglobal(self) -> pd.DataFrame :

        try :
            df = self.conglobal.sql(f"""
                        SHOW  TABLES
                    
                    """).df()
        except :
            df = pd.DataFrame()    
        return df

    def showatbldefglobal(self, tbl : str) -> pd.DataFrame :
        df = self.conglobal.sql(f"""
                        DESCRIBE SELECT * FROM {tbl}
                    
                    """).df()
        return df

    def renameview(self, v_old : str , v_new : str)  -> None:
    
        old_name  = str(v_old)
        new_name = str(v_new)
    

        self.con.execute (f"""
                    ALTER VIEW {old_name} RENAME TO {new_name}
                    """ )
        
    
    def mergetablesasview(self, tbl : str, tbl2 : str, col1 : str, col2 : str, jointype : str ):


        if self.__IsNotvalidDataSetName(tbl) :
          
          st.error("Cannot create a dataset starts with a number!") 
          st.stop()
        else :

            tblnm = f"""MERGED{tbl}"""
            
            self.con.execute(f"""
                            
                            create or replace VIEW {tblnm}
                            as
                            SELECT * FROM {tbl} a
                            {jointype} join {tbl2} b on a.{col1} = b.{col2}
                        
                        """)
            
            arrow_table = self.con.execute(f"""
                    select * from {tblnm}

            """).arrow()

            df = vaex.from_arrow_table(arrow_table)
            return df


    def CreateViewASis(self, querystring ):

        
            
            self.con.execute(f"""
                            
                            {querystring}
                        
                        """)
    
    def CreateView(self, querystring : str, viewname : str):

        if self.__IsNotvalidDataSetName(viewname) :
          
          st.error("Cannot create a dataset starts with a number!")
          st.stop()      
        else :

            tblnm = f"""{viewname}"""
            
            self.con.execute(f"""
                            
                            create or replace VIEW {tblnm}
                            as
                            {querystring}
                        
                        """)
        
    

    