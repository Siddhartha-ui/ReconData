import pandas as pd
import h5py
import pyarrow as pa
import pyarrow.parquet as pq


def GetKey(user : str , file_name : str) -> str :
    sKey = f"{user}" + '\\' + f"{file_name}" 
    return sKey

def WriteToHDF(FileNm : str ,df : pd.DataFrame, sKey : str) -> None :
    
    with pd.HDFStore(FileNm, mode= 'a', complevel= 9 , complib= 'blosc') as hdf:
        hdf.put(key=sKey, value=df, format= 'fixed', data_columns=True)
    
