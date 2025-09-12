from essa import *

args = GetArguments()

db, connection = ConnectToDatabase('YourStrong.Passw0rd')

template = ReadTemplateFromPath(f"./templates/{args["template"]}.csv")
raport = ParseTemplateToRaport(db, template)
SaveRaportToAFile(raport, f"./raports/{args["raport"]}.csv")

CloseDatabaseConnection(db, connection)