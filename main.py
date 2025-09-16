from essa import *

db, connection = ConnectToDatabase('YourStrong.Passw0rd')

args = GetArguments()
Log(f"Arguments: {str(args)}")

template = ReadTemplateFromPath(f"./templates/{args["template"]}.csv")
raport = ParseTemplateToRaport(db, template)
SaveRaportToAFile(raport, f"./raports/{args["raport"]}.csv")

CloseDatabaseConnection(db, connection)