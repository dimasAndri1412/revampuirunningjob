from ConnectToDB import *
import pandas as pd


def writetotablerknprm():
    connectDBLocal = None
    try:
        connectDBLocal = ConnectRknPrmLocal()
        reslt = pd.read_sql("select * from ParamFitur", connectDBLocal)

        print(reslt.head())

    except Exception as e:
        print(f"Terjadi error: {e}")

writetotablerknprm()