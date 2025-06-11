import os
import json
import pandas as pd
from langchain_openai import AzureChatOpenAI
import gradio as gr

os.environ["AZURE_OPENAI_API_KEY"] = "xxxxx" # Replace with your actual key
os.environ["AZURE_OPENAI_ENDPOINT"] = "xx" # Replace with your actual endpoint
os.environ["AZURE_OPENAI_API_VERSION"] = "xxx" # Replace with your actual version
os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT_NAME"] = "gpt-4o" # Replace with your deployment name


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

        # Extract substring after colon if present
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
            instruction = {
                "type":"A",
                "source_column":source_col,
                "target_column":target_col,
                "auto-generated-value": f"Generate a random alphanumeric string with a maximum length of 8 characters. The first 4 characters should be extracted from the value in the {source_col} column. The remaining characters should be randomly generated to complete the string. Assign the final string to the {target_col} column."
            }
            mapping_instructions.append(instruction)
    
    print(f"\nMapping Instructions:{mapping_instructions}")
    print(f"\ntransformation dictionary:{transformation_dict}")

    return mapping_instructions,transformation_dict


def transform_row_with_ai(input_row, mapping_instructions):
    if not input_row:
        return {}

    prompt = f"""
            You are a data transformation engine.

            Your job is to transform the given input row using the provided structured transformation rules.

            ----------------------
            TRANSFORMATION RULES:
            {json.dumps(mapping_instructions, indent=2)}
            ----------------------

            RULE TYPES:
            - 'T' (Transform): Replace values using mapping dictionary. Find the column name and replace the value(s) properly.
            - 'D' (Default): Replace source column value with the default value given.
            - 'O' (One-to-One): Copy the source column's value as-is into the target column.
            - 'A' (Auto-Generate): Auto generate the random characters as specified (e.g., 0019AZ00 -> first four characters are from 'ClientId' column and the rest characters are random alphanumeric characters generated.)

            INSTRUCTIONS:
            1. Apply the transfomations exactly as instructed.
            2. Replace the original column(s) with the tranformed column(s).
            3. Do not retain the original column if it has been tranformed.
            4. Use the new column name specified for the transformation.
            5. If value is missing or not found in a mapping, leave the value as blank "".

            INPUT ROW:
            {json.dumps(input_row, indent=2)}

            Return only the transformed row as a valid JSON dictionary with final target column names.
        
        
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
    mapping_instructions ,transformation_dict = load_transformation_rules(mapping_excel)

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


transform_excel('CIFINPUT/NF_CLIENT_24042025.csv', "CIFINPUT/TRANS_ADRPART01.xlsx")


'''
TRANS_ADRPART01.xlsx data:

Parameter#1
ADR
NF_ADRPART01:ClientId
AddressID
M
NF_ADRPART01:MailingEffectiveDate
NF_ADRPART01:MailingAddrline1
NF_ADRPART01:MailingAddrline2
NF_ADRPART01:MailingAddrline3
CTS
NF_ADRPART01:MailingCity
NF_ADRPART01:MailingStateCode
NF_ADRPART01:MailingZipcode
NF_ADRPART01:MailingCountryCode
NF_ADRPART01:MailingForeignAddrInd
NF_ADRPART01:ReturnedAddressIndicator


NF_CLIENT_24042025.csv data:

PERSONIND| TAXID    | EFFECTIVEDATE| LASTNAME| FIRSTNAME| MIDDLENAME| NAMESALUTATION| NAMESUFFIX| CITIZENSHIP| GENDER| DATEOFBIRTH| COMPANYNAME| INDIVIDUALCONTACTID| DATEOFDEATH| MAILINGEFFECTIVEDATE| MAILINGADDRLINE1| MAILINGADDRLINE2| MAILINGADDRLINE3| MAILINGCITY| MAILINGSTATECODE| MAILINGZIPCODE| MAILINGCOUNTRYCODE| MAILINGFOREIGNADDRIND| RESIDENCEEFFECTIVEDATE| RESIDENCEADDRLINE1| RESIDENCEADDRLINE2| RESIDENCEADDRLINE3| RESIDENCECITY| RESIDENCESTATECODE| RESIDENCEZIPCODE| RESIDENCECOUNTRYCODE| RESIDENCEFOREIGNADDRIND| RETURNEDADDRESSINDICATOR| PREFERREDPHONETYPE| PREFERREDEFFECTIVEDATE| PREFERREDPHONENUMBER| ADDITIONALPHONETYPE| ADDITIONALEFFECTIVEDATE| ADDITIONALPHONENUMBER| WEBEFFECTIVEDATE| WEBTYPE| WEBADDRESS         | POLICYNUMBER| POLICYSUFFIX| RELATIONCODE| DISTPCT| RELATTOINSURED| RELATCONTUNIQID| PARTYALERT| CONTRACTALERT| CLIENTID  | COMPANYTYPE| StateCodeCIF

O        |          | 05-DEC-2013  |         |          |           |               |           |            |       |            | UNKNOWN    | PETTY              |            |                     |                 |                 |                 |            |                 |               |                   |                      |                       |                   |                   |                   |              |                   |                 |                     |                        |                         |                   |                       |                     |                    |                        |                      |                 |        |                    |    213234642| MT          |            9|     0.0|               |                |           |              | 1000000017| AA         |           61
O        |          | 09-NOV-2017  |         |          |           |               |           |            |       |            | ESTATE     | WILSON             |            |                     |                 |                 |                 |            |                 |               |                   |                      |                       |                   |                   |                   |              |                   |                 |                     |                        |                         |                   |                       |                     |                    |                        |                      |                 |        |                    |    213230370| MT          |            9|   100.0|               |                |           |    1145763835| 1000000018| AA         |           14
P        |  90808069| 07-NOV-2013  | SURAJ   | KHAN     | N         |               |           |           1| F     | 07-FEB-1970|            |                    |            | 07-NOV-2013         | 1030 MISSISSIP  |                 |                 | URUWU      |               37|      557591312|                  1| N                    | 07-NOV-2013           | 1030 MISSISSIP    |                   |                   | URUWU        |                 57|        557591312|                    1| N                      | Y                       |                  0| 07-NOV-2013           |           5185661373|                   0| 07-NOV-2013            |            5185661373|                 |        |                    |    213228188| MT          |            1|     0.0|               |                |           |              | 1000000019| AA         |             
P        |  90808069| 07-NOV-2013  | SURAJ   | KHAN     | N         |               |           |           1| F     | 07-FEB-1970|            |                    |            | 07-NOV-2013         | 1030 MISSISSIP  |                 |                 | URUWU      |               37|      557591312|                  1| N                    | 07-NOV-2013           | 1030 MISSISSIP    |                   |                   | URUWU        |                 57|        557591312|                    1| N                      | Y                       |                  0| 07-NOV-2013           |           5185661373|                   0| 07-NOV-2013            |            5185661373|                 |        |                    |    213228188| MT          |           12|     0.0|               |                |           |              | 1000000020| AA         |             


'''
