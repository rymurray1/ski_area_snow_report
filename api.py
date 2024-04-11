import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="ryanmmurray8",
  password="Alpine1!"
)

print(mydb)
