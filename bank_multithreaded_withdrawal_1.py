import time
from concurrent.futures import ThreadPoolExecutor

class BankAccount:
    def __init__(self, balance=0):
        self.balance = balance

    def withdraw(self, amount):
        print(f"Retrait en cours de {amount} ?")
        if self.balance >= amount:
            print(f"Oui, retrait de la somme de {amount}")
            new_balance = self.balance - amount
            time.sleep(0.1)  # Simulate a delay
            self.balance = new_balance
            print(f"Suite a retrait de {amount}, Solde restant intermediaire : {self.balance}")
        else:
            print("Insufficient balance")

account = BankAccount(1000)

with ThreadPoolExecutor(max_workers=2) as executor:
    executor.submit(account.withdraw, 500)
    executor.submit(account.withdraw, 700)

print(f"Final account balance: {account.balance}")
