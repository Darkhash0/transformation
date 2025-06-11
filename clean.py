import pandas as pd
from io import StringIO

# Simulate the data as if it were read from an Excel file
data = """Parameter#1
ADR
NF_ADRPART01:ClientId

M
NF_ADRPART01:MailingEffectiveDate
NF_ADRPART01:MailingAddrline1
NF_ADRPART01:MailingAddrline2
NF_ADRPART01:MailingAddrline3

NF_ADRPART01:MailingCity
StateCodeCIF
NF_ADRPART01:MailingZipcode
CountryCodeCIF
NF_ADRPART01:MailingForeignAddrInd
NF_ADRPART01:ReturnedAddressIndicator"""

# Use StringIO to simulate reading from a file
data_io = StringIO(data)

# Read the data into a DataFrame
df = pd.read_csv(data_io, header=0)

def my_excel_func(df):
    # Fill NaN values with 'SKIP'
    my_columns = df['Parameter#1'].fillna('SKIP')
    
    my_cols_list = []
    
    for col in my_columns:
        col = str(col).strip()  # Convert to string and remove leading/trailing spaces
        
        if "ADR" in col:
            continue  # Skip if 'ADR' is in the string
        
        if ':' in col:
            split_value = col.split(':')[1].strip()
            print(f"Splitted column: {split_value}")
            my_cols_list.append(split_value)
        else:
            my_cols_list.append(col)
    
    return my_cols_list

# Call the function with the simulated DataFrame
cleaned_list = my_excel_func(df)
print("Final list:", cleaned_list)
