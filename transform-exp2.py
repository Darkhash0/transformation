import os
import json
import pandas as pd
from langchain_openai import AzureChatOpenAI
import gradio as gr

os.environ["AZURE_OPENAI_API_KEY"] = "xxx"
os.environ["AZURE_OPENAI_ENDPOINT"] = "https://codedocumentation.openai.azure.com/"
os.environ["AZURE_OPENAI_API_VERSION"] = "2024-02-15-preview"
os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT_NAME"] = "gpt-4o"


model = AzureChatOpenAI(
    openai_api_version=os.environ["AZURE_OPENAI_API_VERSION"],
    azure_deployment=os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT_NAME"],
    temperature=0.0
)


def load_input_data(input_file_path):
    df = pd.read_csv(input_file_path, delimiter='|')
    df.columns = df.columns.str.lower()
    return df


def load_transformation_rules(rules_file_path):
    mapping_df = pd.read_excel(rules_file_path, engine='openpyxl', sheet_name='Mapping')
    df = pd.read_excel(rules_file_path, engine='openpyxl', sheet_name='Transform')
    df = df.dropna(how='any')
    df['Transformed Value'] = df['Transformed Value'].str.replace('\xa0', '', regex=False)
    df = df.dropna(subset=["MapName",'Map Criteria#1','Transformed Value'])
    transformation_dict = {}

    for map_name in df['MapName'].unique():
        group_df = df[df['MapName'] == map_name]
        inner_dict = dict(zip(group_df['Map Criteria#1'], group_df['Transformed Value']))
        transformation_dict[map_name] = inner_dict

    mapping_instructions=[]

    for _, row in mapping_df.iterrows():
        source_col_raw = row.get('Parameter#1', None)
        rule_type = row.get('Transformation Type', None)
        target_col = row.get('STG_Column_Name', None)

        if isinstance(source_col_raw, str) and ':' in source_col_raw:
            source_col = source_col_raw.split(':', 1)[1]
        else:
            source_col = source_col_raw

        if rule_type == 'D':
            instruction = {
                "type":"D",
                "Default_Value":source_col,
                "target_column":target_col
            }
            mapping_instructions.append(instruction)
        
        elif rule_type == 'O':
            instruction = {
                "type":"O",
                "source_column":source_col,
                "target_column":target_col
            }
            mapping_instructions.append(instruction)

        
        elif rule_type == 'T':
            instruction = {
                "type":"T",
                "source_column":source_col,
                "target_column":target_col,
                "mapping":transformation_dict
            }
            mapping_instructions.append(instruction)
        
        elif rule_type == 'A':
            return {
                "type": "A",
                "source_column": source_col,
                "target_column": target_col,
                "auto_generate_rule": f"Generate a unique ID using the current timestamp in the format YYYY-DD-MM-HH-MM-SS-ms. Return the generated ID(s) as a string. And assign this value(s) to the {target_col} column.",
            }

        
        else:
            print(f'\nUnknown rule type:{rule_type}')
    
    print(f"\nMapping Instructions:{mapping_instructions}")
    print(f"\ntransformation dictionary:{transformation_dict}")

    return mapping_instructions


def transform_row_with_ai(input_row, mapping_instructions):
    if not input_row:
        return {}

    prompt = f"""
You are a data transformation expert.

Your job is to transform the given input row using the provided structured transformation rules.

----------------------
TRANSFORMATION RULES:
{json.dumps(mapping_instructions, indent=2)}
----------------------

RULE TYPES:
- 'T' (Transform): Replace values using mapping dictionary. Find the column name and replace the value(s) properly.
- 'D' (Default): Replace source column value with the default value given.
- 'O' (One-to-One): Copy the source column's value as-is into the target column.
- 'A' (Auto-Generate): Generate the unique ID(s).

IMPORTANT INSTRUCTIONS:
1. Apply transformations exactly as instructed.
2. Replace the original column(s) with the tranformed column(s).
3. Do not retain the original column if it has been tranformed.
4. Use the new column name specified for the transformation.
5. If a value is not found in the mapping or the source is empty, use an empty string "".
6. Maintain the tranformed column order in the final output.

INPUT ROW:
{json.dumps(input_row, indent=2)}

Return all the transformed row(s) as a valid JSON object.

"""

    response = model.invoke(prompt)
    content = response.content.strip()
    start_index = content.find('{')
    end_index = content.rfind('}') + 1

    if start_index == -1 or end_index == -1:
        print(f"Invalid AI response: {content}")
        return {}

    try:
        return json.loads(content[start_index:end_index])
    except json.JSONDecodeError:
        print(f"Failed to decode AI response: {content}")
        return {}


def transform_excel(input_csv, mapping_excel):
    input_df = load_input_data(input_csv)
    mapping_instructions  = load_transformation_rules(mapping_excel)

    result_rows = []

    for _, row in input_df.iterrows():
        input_row = row.to_dict()
        transformed_row = transform_row_with_ai(input_row, mapping_instructions)
        print(f"\nAI input:\n{json.dumps(input_row,indent=2)}\nAI output:\n{transformed_row}")
        result_rows.append(transformed_row)

    output_df = pd.DataFrame(result_rows)


    output_folder = "Output"
    os.makedirs(output_folder, exist_ok=True)

    output_file = os.path.join(output_folder, "mapped_output_file.csv")
    output_df.to_csv(output_file, index=False)
    print(f"\nTransformation complete. Output saved to: {output_file}")


transform_excel('CIFINPUT/NF_CLIENT_24042025.csv', "CIFINPUT/TRANS_NAM 4.xlsx")
#TRANS_ADRPART01
#TRANS_NAM 4
#TRANS_ADRPART02





'''
excel file:
->Transform sheet data:
Parameter#1                             
NAM
NF_NAM:ClientID
NF_NAM:PersonInd
NF_NAM:TaxId
NF_NAM:EffectiveDate
NF_NAM:LastName
NF_NAM:FirstName
NF_NAM:MiddleName
NF_NAM:NameSalutation
NF_NAM:NameSuffix
NAM
NF_NAM:Citizenship
NF_NAM:Gender
NF_NAM:DateOfBirth
NF_NAM:CompanyName
NF_NAM:CompanyType
NF_NAM:IndividualContactID
NAM
NAM
NAM
NAM
NAM
NAM
NAM
NAM
NAM
NAM
NF_NAM:DateOfDeath
StateCodeCIF

----------------------

Transformation Type
D
O
T
O
O
O
O
O
O
O
D
O
T
O
O
O
O
D
D
D
D
D
D
D
D
D
D
O
T
----------------
STG_Column_Name
Type
UniqueID
PersonInd
TaxId
EffectiveDate
Lastname
Firstname
Middlename
NameSalutation
Namesuffix
BusinessDesig
Citizenship
Gender
DateofBirth
CompanyName
CompanyType
IndividualContactID
OriginMemClientID
EmployeeInd
DNCInd
AgeAdmitted
ShareInformation
Marketing
VerbalPassword
VerbalPasswordReason
SpecialProcessingIndicator
SpecialProcessingReason
Dateofdeath
StateCode
------------------

The Correct ouput is correct because I removed the transformation rule A which describes as follows:
"Generate a unique ID using the current timestamp in the format YYYY-DD-MM-HH-MM-SS-ms. Return the generated ID(s) as a string. And assign this value(s) to the {target_col} column."


Correct ouput:
AI input:
{
  "personind": "P",
  "taxid": NaN,
  "effectivedate": "11-NOV-2013",
  "lastname": "AZTEFF",
  "firstname": "AMAY",
  "middlename": "AKNINE",
  "namesalutation": NaN,
  "namesuffix": NaN,
  "citizenship": NaN,
  "gender": NaN,
  "dateofbirth": "06-FEB-1979",
  "companyname": NaN,
  "individualcontactid": NaN,
  "dateofdeath": NaN,
  "mailingeffectivedate": NaN,
  "mailingaddrline1": NaN,
  "mailingaddrline2": NaN,
  "mailingaddrline3": NaN,
  "mailingcity": NaN,
  "mailingstatecode": NaN,
  "mailingzipcode": NaN,
  "mailingcountrycode": NaN,
  "mailingforeignaddrind": NaN,
  "residenceeffectivedate": NaN,
  "residenceaddrline1": NaN,
  "residenceaddrline2": NaN,
  "residenceaddrline3": NaN,
  "residencecity": NaN,
  "residencestatecode": NaN,
  "residencezipcode": NaN,
  "residencecountrycode": NaN,
  "residenceforeignaddrind": NaN,
  "returnedaddressindicator": NaN,
  "preferredphonetype": NaN,
  "preferredeffectivedate": NaN,
  "preferredphonenumber": NaN,
  "additionalphonetype": NaN,
  "additionaleffectivedate": NaN,
  "additionalphonenumber": NaN,
  "webeffectivedate": NaN,
  "webtype": NaN,
  "webaddress": NaN,
  "policynumber": 213230955,
  "policysuffix": "MT",
  "relationcode": 7,
  "distpct": 100.0,
  "relattoinsured": "Spouse",
  "relatcontuniqid": NaN,
  "partyalert": NaN,
  "contractalert": NaN,
  "clientid": 1000000023,
  "companytype": "AA",
  "statecodecif": NaN
}
AI output:
{'Type': 'NAM', 'UniqueID': 1000000023, 'PersonInd': 'Y', 'TaxId': '', 'EffectiveDate': '11-NOV-2013', 'Lastname': 'AZTEFF', 'Firstname': 'AMAY', 'Middlename': 'AKNINE', 'NameSalutation': '', 'Namesuffix': '', 'BusinessDesig': None, 'Citizenship': '', 'Gender': '', 'DateofBirth': '06-FEB-1979', 'CompanyName': '', 'CompanyType': 'AA', 'IndividualContactID': '', 'OriginMemClientID': None, 'EmployeeInd': None, 'DNCInd': None, 'AgeAdmitted': None, 'ShareInformation': None, 'Marketing': None, 'VerbalPassword': None, 'VerbalPasswordReason': None, 'SpecialProcessingIndicator': None, 'SpecialProcessingReason': None, 'Dateofdeath': '', 'StateCode': ''}


--------------------------------------------
The wrong ouput is wrong because the it is not returning other columns which are transformed using other transformation rule(s) like T or D or O.

Wrong output:
AI input:
{
  "personind": "P",
  "taxid": NaN,
  "effectivedate": "11-NOV-2013",
  "lastname": "AZTEFF",
  "firstname": "AMAY",
  "middlename": "AKNINE",
  "namesalutation": NaN,
  "namesuffix": NaN,
  "citizenship": NaN,
  "gender": NaN,
  "dateofbirth": "06-FEB-1979",
  "companyname": NaN,
  "individualcontactid": NaN,
  "dateofdeath": NaN,
  "mailingeffectivedate": NaN,
  "mailingaddrline1": NaN,
  "mailingaddrline2": NaN,
  "mailingaddrline3": NaN,
  "mailingcity": NaN,
  "mailingstatecode": NaN,
  "mailingzipcode": NaN,
  "mailingcountrycode": NaN,
  "mailingforeignaddrind": NaN,
  "residenceeffectivedate": NaN,
  "residenceaddrline1": NaN,
  "residenceaddrline2": NaN,
  "residenceaddrline3": NaN,
  "residencecity": NaN,
  "residencestatecode": NaN,
  "residencezipcode": NaN,
  "residencecountrycode": NaN,
  "residenceforeignaddrind": NaN,
  "returnedaddressindicator": NaN,
  "preferredphonetype": NaN,
  "preferredeffectivedate": NaN,
  "preferredphonenumber": NaN,
  "additionalphonetype": NaN,
  "additionaleffectivedate": NaN,
  "additionalphonenumber": NaN,
  "webeffectivedate": NaN,
  "webtype": NaN,
  "webaddress": NaN,
  "policynumber": 213230955,
  "policysuffix": "MT",
  "relationcode": 7,
  "distpct": 100.0,
  "relattoinsured": "Spouse",
  "relatcontuniqid": NaN,
  "partyalert": NaN,
  "contractalert": NaN,
  "clientid": 1000000023,
  "companytype": "AA",
  "statecodecif": NaN
}
AI output:
{'AddressID': '2023-24-11-12-00-00-000'}

'''