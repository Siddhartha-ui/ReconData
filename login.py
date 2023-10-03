import hashlib
import duckdb as db
import pandas as pd


class login :
	
    def __init__(self, username, password, confirm_password = '') -> None:
            __conn = db.connect('UserDB\\User.db')
            __hashed_pwd = hashlib.sha256(str.encode(password)).hexdigest()

            self.conn = __conn
            
            self.username = username
            self.password = password
            self.hashed_pswd = __hashed_pwd
            self.confirm_password = confirm_password
                        
    def updatedefaultpassword(self, user : str) :
        
        user = f"'{user}'" 
        self.conn.execute(f"""
            UPDATE userstable
            
            SET password = 'f3b4040600031598b66d56f31233fd2f35f30c7944506d7a44e75e4e336440db'
            where  username = {user}          

        """)
        self.conn.commit()

    def deleteallusers(self) :
         self.conn.execute('delete from userstable')

    def create_usertable(self):

           self.conn.execute('CREATE  TABLE IF NOT EXISTS userstable(username TEXT,password TEXT)')
    
    def add_userdata(self):
        username = f"'{self.username}'"
        password = f"'{self.hashed_pswd}'"
        
        self.conn.execute('INSERT INTO userstable(username,password) VALUES (?,?)',(self.username,self.hashed_pswd))
        self.conn.commit()
        
    def change_password(self, user : str):
        pass 
         

    def login_user(self) -> bool :
        username = f"'{self.username}'"
        password = f"'{self.hashed_pswd}'"

        df = self.conn.execute(f'''
            SELECT * FROM userstable WHERE username = {username} AND password = {password}
        ''').df()
        
        if not df.empty :
            return True
        else :
            return False

    
    # def check_hashes(self) -> str:
        
    #     if make_hashes(self.password) == self.hashed_pswd :
    #        return self.hashed_pswd
    #     else :
    #        return ''  

    def make_hashes(self):
            return hashlib.sha256(str.encode(self.password)).hexdigest()


    def SignIn(self) -> bool :
        login.create_usertable(self)
        hashed_pswd = login.make_hashes(self.password)
        result = login.login_user(self.username,login.check_hashes(self.password,hashed_pswd))
        return result    
    
    def CheckDuplicateuser(self) -> bool :
        username = f"'{self.username}'"
        df = self.conn.sql(f'''
                SELECT * FROM userstable WHERE username = {username}
        ''').df()
            
        return df.empty
         
    def view_all_users(self) -> pd.DataFrame:
        df = self.conn.sql('SELECT * FROM userstable').df()

        return  df    
        
    def ValidateConfirmPass(self) -> bool :
	    return (self.password == self.confirm_password)

