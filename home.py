import streamlit as st
#from mitosheet.streamlit.v1 import spreadsheet
from streamlit_option_menu import option_menu
import pandas as pd
import numpy as np
from login import login
from datetime import datetime
from dboperation import dboperation 
from upload import Upload
from querybuilder import Querybuilder
from allocation import Allocation

if __name__ == "__main__":

    
    st.set_page_config(page_title="Analytics", layout="wide" , page_icon="chart_with_upwards_trend")

    if "user_name" not in st.session_state :
        st.session_state.user_name = "" 
    if "sign_up" not in st.session_state :
        st.session_state.sign_up = False
    if "is_loggedin" not in st.session_state :
        st.session_state.is_loggedin = False
    
                  


def diff(n, mid):
    if (n > (mid * mid * mid)):
        return (n - (mid * mid * mid))
    else:
        return ((mid * mid * mid) - n)

def cubicRoot(n):
 
    
    start = 0
    end = n
 
    # Set precision
    e = 0.0000001
    while (True):
 
        mid = (start + end) / 2
        error = diff(n, mid)
 
        
        if (error <= e):
            return mid
 
        
        if ((mid * mid * mid) > n):
            end = mid
 
        
        else:
            start = mid

def SignUp():
        
            

            buff, col, col2, buff2 = st.columns([1,2,2,1]) 
            col.subheader("Create New Account")
            new_user = col.text_input("Username", key= "new_User")
            new_password = col.text_input("Password",type='password', key= "new_password")
            confirm_new_pasword = col.text_input("Confirm New Password",type='password', key= "new_confirm_pass")
            save_btn = col.button("Save")

            if len(new_user) > 0 and len(new_password) > 0 and len(confirm_new_pasword) > 0 and save_btn  :

                    log = login(username= str(new_user), password= str(new_password), confirm_password= str(confirm_new_pasword))

                    # df = log.view_all_users()
                    # st.dataframe(df)

                    if log.ValidateConfirmPass() :

                        
                        if  log.CheckDuplicateuser():
                            log.create_usertable()
                            
                            log.add_userdata()
                            st.success("You have successfully created a valid Account")
                            st.info("Go to Login Menu to login")
                        else :
                            st.error ("User Exists. Create a new user")      
                            
                    else :
                        st.error ("New Password and Confirm New Password does not match")  
            else :
                   pass 
                      

def GetCurrentdate() -> str :
    now = datetime.now()
    dt_string = now.strftime("%d-%m-%Y")
    return  str(dt_string)


def getKeysfortheUserConversion(tbl : list) -> list :
      
    li = []

    for i, val in enumerate(tbl):
        
        if val[0][0:len(st.session_state.user_name)] == st.session_state.user_name :
              li.append(val[0])  
    
    return li


def getKeysfortheUser(tbl : list) -> list :
      
    li = []

    for i, val in enumerate(tbl):

        #if val[0][0:len(st.session_state.user_name)] == st.session_state.user_name :
              li.append(val[0])  
    
    return li


def download_csv(df : pd.DataFrame) :
    csv = convert_df(df)
    st.download_button(
        "Download file",
        csv,
        "output.csv",
        "text/csv",
        key='download-csv'
    )        

def convert_df(df):
        return df.to_csv(index=False).encode('utf-8')

def queryarea(query : str) -> str :
    try :
        qry = st.text_area(label= " Query" , value=query)

    except :
        qry = st.text_area(label= " Query" , value='')
    return qry    


def resetsession_uploadeddata() :
    if "df" in st.session_state :
        del st.session_state["df"]

def resetsession() :
    if "df_data" in st.session_state :
        del st.session_state["df_data"]
        

def getdatainsession(tbl : str) -> pd.DataFrame:
    if not "df_data" in st.session_state :
       dbop = dboperation(st.session_state.user_name)
       st.session_state.df_data = dbop.showtbl(str(tbl))
       st.session_state.df_data = st.session_state.df_data.replace(',', '', regex=True) 
    return st.session_state.df_data

def onClickFunction():
    st.session_state.click = True

    

def populatefromDuckDBForProjection():

    if "click" not in st.session_state:
            st.session_state.click = False
    if not "dataset" in st.session_state :
        st.session_state.dataset = None
    
    
    up = Upload() 
    df = up.DUCKDB_to_TBL()

    
    if df.empty :
       st.error("Upload a file")
       st.stop()
    else :      

        tbl = df.values.tolist()

        KeyList = getKeysfortheUser(tbl)
        #KeyList = df.values.tolist()
        colb1, colb2 = st.columns([4,1])
        selec_tbl_1 = st.selectbox(label = "Select datasets", options= KeyList)
        drop_btn = st.button("drop dataset")
        
        if not selec_tbl_1 :
            
                st.error("Select a dataset")
                st.stop()
        else :
            
            tbl = str(selec_tbl_1)

            tbl = tbl.replace("\\", '').replace(".", '').replace("-",'').replace(" ",'').replace("_",'').replace("(" , '').replace(")",'')
            dbop = dboperation(st.session_state.user_name)
            if dbop.IsView(tbl) :

               with st.expander(label = "View SQL") :
                   st.text_area(label= " Query", value= dbop.getviewquery(tbl).strip()) 

            if drop_btn :
               
               dbop.droptable(str(tbl)) 
               st.info("Table/View is dropped")

            else :
                df = dbop.showtblStruct(str(tbl))
                with st.expander("Dataset Attributes") :
                    cols = st.multiselect(label= "columns" , options= df.columns, placeholder= "Select Columns")
                resultString = ', '.join('"{}"'.format(col) for col in cols).strip()
                

                df_selected = pd.DataFrame(df, columns= cols)
                
                if "cols" not in st.session_state :

                    columns = st.text_area("Projection", resultString )
                else :
                    st.session_state.cols = resultString
                    columns = st.text_area("Projection", st.session_state.cols)    

                col_att , col_val = st.columns([1,1])

                # if not df_selected.empty:
                #     pass

                filter =  st.text_area("Filter", '' ).strip()
                
                agg = st.text_area("Aggregation", '' ).strip()
                q_str = dbop.showquery(tbl,resultString)
                
                col1, col2, col3 = st.columns([2, 3,6])
                v_name = st.text_input(label= "Dataset Name", value= "")
                v_name = tbl_nm_prpare(v_name)
                
                qb = Querybuilder(user= st.session_state.user_name, query= str(q_str),viewname = str(v_name), tbl= str(tbl),cols= str(columns),filter= str(filter),aggr= str(agg))
                
                
                #col1.button("Show Result", on_click= onClickFunction)

                if col1.button("Show Result"):
                    # try :

                        with st.spinner("Processing query. Please wait..."):

                            arrow_table = qb.rel_to_df()
                            df = arrow_table.to_pandas_df()
   
                            st.session_state.dataset = df
                            with st.expander("Get Query"):
                                queryarea(qb.rel_to_query()) 

                            st.data_editor(st.session_state.dataset.head(100), use_container_width= True,hide_index= True)      
                            df.dropna(inplace = True) 
  
                            # # storing dtype before operation
                            # dtype_before = type(df["Partner"])

                            # # converting to list
                            # partnerlist = df["Partner"].tolist()
                            # Tranchelist = df["Tranche"].tolist()
                            # begincap = df["BeginCapital"].tolist()
                            # opcontrib = df["opContribution"].tolist()
                            # opwithdraw = df["opWithdraw"].tolist()
                            # pl = dbop.getpl('pl')

                            # st.dataframe(pl,hide_index= True,use_container_width= True)

                            # pl_list = pl["Value"].tolist()
                            # bucket_cols = pl["bucket"].to_list()
                            
                            # #spreadsheet(st.session_state.dataset)

                            # #if st.button("calculate") :
                            # cols = ["Partner Name" , "Tranch Name", "Begin Cap", "Op Contribition", "Op Withdraw", "Adjusted Capital", "ep", "Book Income"]
                            # for i in bucket_cols :
                            #     cols.append(i)

                            # cols.append("GAV")

                            
                                                        
                            # book_ep = Allocation(partnerlist, Tranchelist, begincap,opcontrib,opwithdraw,pl_list,rule = 1)           
                            # d = pd.DataFrame(book_ep.allocate(),columns= cols)
                            # #d = pd.DataFrame(x.allocate())
                            
                            # st.dataframe(d.head(100), use_container_width= True, hide_index= True)
                            

                            # #st.dataframe(st.session_state.dataset.head(100), use_container_width= True)
                            # download_csv(d)
                    # except :
                    #      st.error ("Correct your required expressions")
                
                if col2.button("Create Dataset") and len(v_name) > 0 :
                    #try :
                        qb_modifiedquery = Querybuilder(user= st.session_state.user_name, query= str(qb.rel_to_query()),viewname = str(v_name))
                        qb_modifiedquery.CreateView()
                        st.info("Dataset created")    
                    #except :
                     #   st.error("Error in Dataset creation . Check with show results")

def populatefromDuckDB():

    up = Upload() 
    df = up.DUCKDB_to_TBL()

    if df is None :
       st.error("Upload a file")
       st.stop()
    else :      

        tbl = df.values.tolist()

        KeyList = getKeysfortheUser(tbl)
        
        selec_tbl_1 = st.selectbox(label = "Select datasets", options= KeyList)
        drop_btn = st.button("drop dataset")
        
        
        if not selec_tbl_1 :
            
                st.error("Select a dataset")
                st.stop()
        else :
            
            
            tbl = str(selec_tbl_1)

            tbl = tbl.replace("\\", '').replace(".", '').replace("-",'').replace(" ",'').replace("_",'').replace("(" , '').replace(")",'')
            dbop = dboperation(st.session_state.user_name)
            if drop_btn :
               dbop.droptable(str(tbl)) 
               st.info("Table/View is dropped")

            else :
                df = dbop.showtblStruct(str(tbl))
                cols = st.multiselect(label= "columns" , options= df.columns, placeholder= "Select Columns")
                resultString = ', '.join('"{}"'.format(col) for col in cols)
                query_modified = st.text_area("Columns", resultString )

                if len(query_modified) == 0 :
                   q_str = dbop.showquery(tbl,cols= '*') 
                else :       
                    q_str = dbop.showquery(tbl,query_modified )
                
                col1, col2, col3 = st.columns([2, 3,6])
                v_name = st.text_input(label="Dataset Name", value= "")
                v_name = tbl_nm_prpare(str(v_name))

                q_str = queryarea(q_str)
                                
                qb = Querybuilder(user= st.session_state.user_name, query= str(q_str),viewname = str(v_name) )
                
                if col1.button("Create Dataset") and len(v_name) > 0 :
                    #try :
                        qb.CreateView()
                        st.info("Dataset created")    
                    ##   st.error("Error in Dataset creation . Check with show results")

                if col2.button ("Show result") :
                    
                    df = qb.Result()
                    st.data_editor(df.head(100), use_container_width= True,hide_index= True)
                    download_csv(df)        
                

def tbl_nm_prpare(tbl : str) -> str :
    tbl = tbl.replace("\\", '').replace(".", '').replace("-",'').replace(" ",'').replace("_",'').replace("(" , '').replace(")",'').replace("&",'') 
    return str(tbl)      

def SingleTblOp(dtset : bool , showbtn : bool, postbtn : bool):

    dbop = dboperation(st.session_state.user_name)
    df = dbop.showtblStruct(dtset)

    df = df.replace(np.nan,'',regex=True)
                    
    tbl = str(dtset)
    tbl = tbl_nm_prpare(tbl)                    
                    
    if showbtn :
        df_arrow = dbop.showtbl(dtset)
        df = df_arrow.to_pandas_df()

        st.data_editor(df.head(100), use_container_width= True, height= 400,hide_index= True)

    if postbtn :
            st.error ("Please select two different datasets")    
            #dbop.Createtbl(df, str(tbl))  


def PostDatalayout(KeyList : list):

    
    colx, coly, colz = st.columns([4,2,2])
    col1, col2 , buff1, buff2= st.columns([1,1,3,3])
    dtset1 = colx.selectbox(label = "Select source dataset", options= KeyList )
    dtset2 = colx.selectbox(label = "Select dest dataset", options= KeyList )

    showbtn = col1.button(label= "Show Data")
    postbtn = col2.button(label= "Merge")
    dbop = dboperation(st.session_state.user_name)

    if dtset1 and dtset2 :
        
        df = dbop.showtblStruct(dtset1)
        
        df = df.replace(np.nan,'',regex=True)

        df_1 = dbop.showtblStruct(dtset2)
        df_1 = df_1.replace(np.nan,'',regex=True)
        
        selec_tbl_cols = coly.selectbox(label = "select key col", options= df.select_dtypes(include='object').columns, key= "tbl1" )
        select_tbl_join = colz.selectbox("Join Type", options= ["Left", "Inner" , "Right" , "Full Outer"], key= "col1")
        
        selec_tbl_cols1 = coly.selectbox(label = "select key col", options= df_1.select_dtypes(include='object').columns, key= "tbl2")
        
        
        tbl = tbl_nm_prpare(dtset1)
        tb2 = tbl_nm_prpare(dtset2)
        
        if dtset1 == dtset2 :
           SingleTblOp(dtset1,showbtn=showbtn , postbtn = postbtn)  
        
        if dtset1 != dtset2 : 

            if postbtn :
               
               
               with st.spinner("Merging .Please wait ...") : 
                df_arrow = dbop.mergetablesasview(tbl= tbl, tbl2=tb2, col1= selec_tbl_cols , col2= selec_tbl_cols1, jointype= select_tbl_join)
                df = df_arrow.to_pandas_df()
                st.data_editor(df.head(100), use_container_width= True,hide_index= True)
    
    if dtset1 and not dtset2 :
       SingleTblOp(dtset1,showbtn=showbtn , postbtn = postbtn)     
                        
    if dtset2 and not dtset1 :
         SingleTblOp(dtset2,showbtn=showbtn , postbtn = postbtn)


def test():

    # log_in = login("sid", "Welcome!10")
    # df = log_in.view_all_users()
    
    dbop = dboperation(st.session_state.user_name)

    # df = dbop.showallviews()
    
    v_list = dbop.showallviewsgl(str(st.session_state.user_name))
    for i in v_list :
          
          v_query = dbop.getviewquerygbls(i[0])
          
          dbop.CreateViewASis(v_query)


    #log_in.updatepassword()
    #st.data_editor(df)
    df1 = dbop.showalltblglobal()
    # st.dataframe(df1)
    
    # tbl = dbop.showalltblsgl(str(st.session_state.user_name))
    
    
    # # tbl =  df_3.values.tolist()
    
    # keys = getKeysfortheUserConversion(tbl)
        
    # for t in keys :
    #      st.write(t) 
    #      dbop.cretetblfromglobaltoloc(str(t))
    #df_2 = dbop.showallviewsglobal()

    
    # vi = df_2.values.tolist()
    # keys_v = getKeysfortheUserConversion(vi)
    
    # for v in keys_v :
    #     dbop.creteviewfromglobaltoloc(str(v))

def dataWrangling(df : pd.DataFrame , n : int) :

    try :

        df.columns = df.iloc[n]
        #       # Using DataFrame.rename()
        df2 = df.rename(columns=df.iloc[n+1])

        #       # Convert row to header and remove the row
        df2 = df.rename(columns=df.iloc[n]).loc[n:]

        #       # Using DataFrame.rename() to convert row to column header
        df.rename(columns=df.iloc[n+1], inplace = True)

        #       # Using DataFrame.values[]
        header_row = df.iloc[n]
        df2 = pd.DataFrame(df.values[n +1:], columns=header_row)
        st.session_state.df = df2
        return    st.session_state.df
        
    except :
        st.error("duplicate column headers.Please check row index")
        
        st.session_state.df = None
        st.stop()
    # return    st.session_state.df  
 

def populatedata():
    
    up = Upload() 
    df = up.DUCKDB_to_TBL()

    if df is None :
       st.error("Upload a file")
       st.stop() 
    else :   
        tbl = df.values.tolist()
        
        KeyList = getKeysfortheUser(tbl)
        
        if len(KeyList) > 0 :
                
                PostDatalayout(KeyList)
     

def loginoperation() :
   
   
   result = False 
   buff, col, col2, buff2 = st.columns([1,2,2,1])    
   col.subheader("Login")
        
   username = col.text_input("User Name", key= "user")
   password = col.text_input("Password",type='password', key= "pass")
   col1, col2 = col.columns([2,1])
   login_btn = col1.button("Sign-In")
   
   
   if login_btn : 
        
        if len(username) == 0 :
           st.error ("Please enter user name") 
        elif  len(password) == 0 :
           st.error ("Please enter password") 
        else :

            log_in = login( username= str(username), password= str(password))
            if len(username) > 0 and len(password) > 0 :          
                log_in.create_usertable()
                result =log_in.login_user()
        if result:
                col1.success("Logged In as {}".format(username))
                st.session_state.is_loggedin = True
                st.session_state.user_name = str(username)
                
                
                
        else :
                st.error("Login credential does not exist.Please sign-Up") 

with st.sidebar:
        selected = option_menu(None, ["Sign-In", "Sign-Up", "Upload data", "---", "Model", 'Recon-query' , 'Recon-projection'  , 'Logout'], 
        icons=['person', 'door-open', 'cloud-upload', None, "list-task", 'bar-chart', 'bar-chart','door-closed'], 
        menu_icon="cast", default_index=0 ,
        styles={
             "container": {"padding": "0!important", "background-color": "#eee"},
             "icon": {"color": "orange", "font-size": "25px"}, 
             "nav-link": {"font-size": "15px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
             "nav-link-selected": {"background-color": "blue"},
         }
        
        )

if selected == "Model" :
   if not st.session_state.is_loggedin :
      st.error ("Please Sign-in ")
      st.stop()    
   else :
      
      populatedata()
      #test()


if selected == "Recon-query":
   if not st.session_state.is_loggedin :
      st.error ("Please Sign-in ")
      st.stop()    
   else :  
      populatefromDuckDB()

if selected == "Recon-projection":
   if not st.session_state.is_loggedin :
      st.error ("Please Sign-in ")
      st.stop()    
   else :
      #resetsession()  
      populatefromDuckDBForProjection()

if selected == "Sign-In" :
    
    
    if st.session_state.is_loggedin :
       st.error ("You are alreday logged in ") 
    else :
        
        loginoperation()
        
# if selected == "Spreadsheet" :
#     pass        
         
if selected == "Sign-Up" :
    SignUp()
    

if selected == "Upload data" :
   if not st.session_state.is_loggedin :
      st.error ("Please Sign-in ")
      st.stop()    
   else :

       col1, col2 = st.columns([1,4])        
       file = st.file_uploader(label= "Upload File (EXCEL or CSV)",type=['XLSX', 'CSV'] , on_change= resetsession_uploadeddata) 
       st.subheader("Custom dataset name")
       datasetname = st.text_input("", key= "dtst").strip() 

       btnPrcs =  st.button("Process data")

    #    input_number = st.number_input(label = "Enter row index to set as header", min_value= 0 )
    #    btn_reset = st.button("Reset") 
    #    placeholder =  st.empty()

    #    if file and not btnPrcs and not btn_reset :
           
    #        up = Upload(file,st.session_state.user_name,GetCurrentdate(), customfilename= str(datasetname))
    #        if st.session_state.df is None :     
    #             df = up.File_to_Dataframe()
    #             placeholder.dataframe(df.head(50),use_container_width= True)
    #        else :
    #             placeholder.dataframe(st.session_state.df,use_container_width= True)     
       
    #    if file and btn_reset :
    #       df = dataWrangling(st.session_state.df, int(input_number)) 
    #       placeholder.dataframe(df.head(50),use_container_width= True)
          

       if file and btnPrcs :
            
            with st.spinner("Uploading  data .Please wait...") :
                up = Upload(file,st.session_state.user_name,GetCurrentdate(), customfilename= str(datasetname))
                up.CSV_to_DUCKDB()
            
if selected == "Logout" :
   if st.session_state.is_loggedin :
        st.subheader(f"User  {st.session_state.user_name} successfully logged out")
        st.session_state.clear()
   else :
        pass 
     
