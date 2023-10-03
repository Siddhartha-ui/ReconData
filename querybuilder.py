import pandas as pd
import pyarrow as pa

from dboperation import dboperation 
import streamlit as st
import vaex

class Querybuilder(object) :
    
    def __init__(self, user : str ,query : str, viewname = "", tbl = "", cols = "",filter = "",aggr = "") -> None:
        
        __dbop =  dboperation(st.session_state.user_name)
        self.dbop = __dbop
        self.query = query
        self.viewname = viewname
        self.user = user
        self.tbl = tbl
        self.cols = cols
        self.filter = filter
        self.aggr = aggr
        
        if not "cols" in st.session_state :
           st.session_state.cols = ''
        
        if not "aggr" in st.session_state :
           st.session_state.aggr = ''
        else :
           st.session_state.aggr = str(aggr)
        
        if not "filter" in st.session_state :
           st.session_state.filter = ''
        else :
           st.session_state.filter = str(filter)    

        if not "querystring" in st.session_state :
            st.session_state.querystring = ''
        else :
            st.session_state.querystring = str(query)

        


    def Result(self) -> pd.DataFrame :
        
        return self.dbop.getresult(self.query)
    
    def rel_to_df(self) -> vaex.DataFrame  :
        
        rel = self.dbop.getrelation(self.tbl)

        if len(self.cols) > 0 and len(self.filter) > 0 and len(self.aggr) > 0 :
            rel = rel.project(self.cols).filter(self.filter).aggregate(self.aggr)
           
            
        elif len(self.cols) > 0 and len(self.filter) > 0 and len(self.aggr) == 0  : 
            rel = rel.project(self.cols).filter(self.filter)
            
        elif len(self.cols) > 0 and len(self.filter) == 0 and len(self.aggr) > 0  : 
            rel = rel.project(self.cols).aggregate(self.aggr)
        elif len(self.cols) > 0 and len(self.filter) == 0 and len(self.aggr) == 0 :
             rel = rel.project(self.cols)
        elif len(self.cols) == 0 :
            rel = rel 

        st.session_state.dataset = vaex.from_arrow_table(rel.to_arrow_table())
        
        return st.session_state.dataset

    def rel_to_query(self) -> str :
        rel = self.dbop.getrelation(self.tbl)

        if len(self.cols) > 0 and len(self.filter) > 0 and len(self.aggr) > 0 :
            rel = rel.project(self.cols).filter(self.filter).aggregate(self.aggr)
        elif len(self.cols) > 0 and len(self.filter) > 0 and len(self.aggr) == 0  : 
            rel = rel.project(self.cols).filter(self.filter)
        elif len(self.cols) > 0 and len(self.filter) == 0 and len(self.aggr) > 0  : 
            rel = rel.project(self.cols).aggregate(self.aggr)
        elif len(self.cols) > 0 and len(self.filter) == 0 and len(self.aggr) == 0 :
             rel = rel.project(self.cols)
        elif len(self.cols) == 0 :
            rel = rel 
        
        return rel.sql_query()  

    def CreateView(self) :
        self.dbop.CreateView(self.query, self.viewname)   
