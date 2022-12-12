def printMenu():
        print("========================")
        print("1.Withdrawal")
        print("2.Deposit")
        print("3.Show Balance")
        print("4.Exit")
        print("========================")

def generateLoginRequest(transaction, uuid) -> dict:

    username = input("Username:")
    password = input("Password:")

    return {
        'transaction': transaction,
        'username': username,
        'password': password,
        'uuid': uuid
    }
    
def generateRequest(id, uuid) -> dict:

    transaction = 0
    while(transaction<1 or transaction>5):
        printMenu()
        transaction = int(input("Choose Transaction:"))
    
    if(transaction == 1 or transaction == 2):
        ammount = input("Enter Ammount:")
    else: ammount = 0
        

    return {
        'transaction' : transaction,
        'id' : id,
        'ammount' : ammount,
        'uuid': uuid
    }

