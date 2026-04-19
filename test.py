from pymongo import MongoClient
import certifi

uri = "mongodb+srv://pamod:pamod123@serandip.e53xt2m.mongodb.net/?retryWrites=true&w=majority&appName=serandip"

client = MongoClient(
    uri,
    tls=True,
    tlsCAFile=certifi.where(),
    serverSelectionTimeoutMS=10000,
)

print(client.admin.command("ping"))