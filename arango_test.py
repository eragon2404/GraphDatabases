from pyArango.connection import *

conn = Connection(username="root", password="arango")
db = conn["test"]

print(db)
res = db.AQLQuery("INSERT { name: \"test\" }")