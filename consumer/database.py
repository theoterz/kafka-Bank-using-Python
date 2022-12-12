from decimal import *
import psycopg2

class Database:

    con = psycopg2.connect(dbname = "bank", user = "postgres", password = "", host = "localhost")
    statement = con.cursor()
    
    def login(self, username, password):
        
        self.statement.execute("SELECT * FROM users;")
        result = self.statement.fetchone()
        id = -1

        while result is not None:
            if(result[1] == username and result[2] == password):
                id = result[0]
                break
            result = self.statement.fetchone()
        
        return id

    def withdrawal(self, id, ammount):
        reply = ""
        self.statement.execute("SELECT * FROM users where id="+str(id)+" FOR UPDATE;")
        result = self.statement.fetchone()
        if(Decimal(ammount) > result[3]): reply = "Balance is not enough!"
        else:
            balance = result[3] - Decimal(ammount)
            sql = "UPDATE users SET balance = "+str(balance) +" where id=" +str(id) +";"
            self.statement.execute(sql)
            self.con.commit()
            reply = "Success! New Balance: " + str(balance)
        
        return reply
    
    def deposit(self, id, ammount):
        reply = ""
        self.statement.execute("SELECT * FROM users where id="+str(id)+" FOR UPDATE;")
        result = self.statement.fetchone()
        balance = result[3] + Decimal(ammount)
        sql = "UPDATE users SET balance = "+str(balance) +" where id=" +str(id) +";"
        self.statement.execute(sql)
        self.con.commit()
        reply = "Success! New Balance: " + str(balance)
        
        return reply
    
    def showBalance(self, id):
        reply = ""
        self.statement.execute("SELECT * FROM users where id="+str(id)+";")
        result = self.statement.fetchone()
        balance = result[3]
        reply = "Balance: " + str(balance)

        return reply
    
    def closeConnection(self):
        self.statement.close()
        self.con.close()
