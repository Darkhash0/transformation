import os
import json
import pandas as pd
from langchain_openai import AzureChatOpenAI
import gradio as gr

os.environ["AZURE_OPENAI_API_KEY"] = "70e7"
os.environ["AZURE_OPENAI_ENDPOINT"] = "https://codedocumentation.openai.azure.com/"
os.environ["AZURE_OPENAI_API_VERSION"] = "2024-02-15-preview"
os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT_NAME"] = "gpt-4o"


model = AzureChatOpenAI(
    openai_api_version=os.environ["AZURE_OPENAI_API_VERSION"],
    azure_deployment=os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT_NAME"],
    temperature=0.0
)


def load_input_data(input_file_path):
    return pd.read_csv(input_file_path)


def load_transformation_rules(rules_file_path):
    df = pd.read_excel(rules_file_path, engine='openpyxl', sheet_name='sample')
    transformation_dict = {} # parsing the rules in to the dictionary, Main structure for ai to understand
    # source_to_target_col_map = {}

    print("Excel columns:", df.columns.tolist())

    for _, row in df.iterrows():
        source_col = row['SOURCE_COLUMN'] #SOURCE_COLUMN header in rules excel
        target_col = row['TARGET_COLUMN'] #TARGET_COLUMN header in rules excel
        rule_type = row['TYPE'] #TYPE header in rules excel
        source_val = row.get('SOURCE_VALUE', None) #SOURCE_VALUE header in rules excel
        transformed_val = row['TRANSFORMED_VALUE'] #TRANSFORMED_VALUE header in rules excel

        # source_to_target_col_map[source_col] = target_col

        if source_col not in transformation_dict:
            transformation_dict[source_col] = {
            "type": rule_type,
            "target_column": target_col
            }

        if rule_type == 'T':
            transformation_dict[source_col].setdefault("mapping", {})[source_val] = transformed_val #{'PersonInd': {'type': 'T', 'target_column': 'PersonInd', 'mapping': {'N': 'Company', 'Y': 'Individual'}}}

        elif rule_type == 'D':
            transformation_dict[source_col]["value"] = transformed_val

        # No additional data needed for 'O', just the target_column is enough

        # just printing the dicitonaries to check:
        print(f"\ntransformation dictionary:\n{transformation_dict}")
        # print(f"\nsource_to_target_col_map dictionary:\n{source_to_target_col_map}")

    return transformation_dict


def transform_row_with_ai(input_row, transformation_dict):
    if not input_row:
        return {}

    prompt = f"""
            You are a data transformation engine.

            Your job is to transform the given input row using the provided structured transformation rules.

            ----------------------
            TRANSFORMATION RULES:
            {json.dumps(transformation_dict, indent=2)}
            ----------------------

            RULE TYPES:
            - 'T' (Transform): Replace values using mapping dictionary.
            - 'D' (Default): Replace column value with the default value given.
            - 'O' (One-to-One): Copy the source column's value as-is into the target column.

            INSTRUCTIONS:
            1. Always return **target column names** (not source column names).
            2. If value is missing or not found in a mapping, leave the value as blank "".

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
    transformation_dict = load_transformation_rules(mapping_excel)

    result_rows = []

    for _, row in input_df.iterrows():
        input_row = row.to_dict()
        transformed_row = transform_row_with_ai(input_row, transformation_dict)
        print(f"\nAI input:\n{json.dumps(input_row,indent=2)}\nAI output:\n{transformed_row}")
        result_rows.append(transformed_row)

    output_df = pd.DataFrame(result_rows)

    # keeping the same column orders in output
    # ordered_columns = [
    # transformation_dict[col]["target_column"]
    # for col in input_df.columns if col in transformation_dict
    # ]
    # output_df = output_df.reindex(columns=ordered_columns)

    output_folder = "Output"
    os.makedirs(output_folder, exist_ok=True)

    output_file = os.path.join(output_folder, "mapped_output_file.csv")
    output_df.to_csv(output_file, index=False)

    # output_df.to_csv(output_path, index=False)
    # print(f"Transformation complete. Output saved to: {output_path}")

    return output_file


# transform_excel('DATA/NAM 4.csv', "DATA/SOURCE_TARGET_MAPPING.xlsx", "output_exp3.csv")

with gr.Blocks() as demo:
    gr.Markdown("## CSV Column Mapper using Azure OpenAI")
    file_input = gr.File(label="Upload Source CSV")
    excel_input = gr.File(label="Upload Excel with Columns")
    output_file = gr.File(label="Download Mapped CSV")
    submit_button = gr.Button("Map Columns")

    submit_button.click(fn=transform_excel, inputs=[file_input, excel_input], outputs=[output_file])

demo.launch(share=True)

'''
OUTPUT:
AI input:
{
  "PersonInd": "N",
  "TaxId": "232-77-6710",
  "EffectiveDate": "2024-05-03",
  "LastName": "Aguirre",
  "FirstName": "Paula",
  "MiddleName": "Eric",
  "NameSalutation": "Dr",
  "NameSuffix": "III",
  "Citizenship": "Spain",
  "Gender": "M",
  "DateOfBirth": "1997-03-22",
  "CompanyName": "Holmes Smith and Kim",
  "CompanyType": "Corporate",
  "IndividualContactID": "CONT-538921"
}
AI output:
{'PersonInd': 'Company', 'Tax_Id': '232-77-6710', 'Effective_Date': '2024-05-03', 'LastName': 'Aguirre', 'FirstName': 'Paula', 'MiddleName': 'Eric', 'NameSalutation': 'Dr', 'NameSuffix': 'III', 'Citizenship': 'sp', 'Gender': 1, 'DateOfBirth': '1997-03-22', 'CompanyName': 'CTS', 'CompanyType': 'Corporate', 'IndividualContactID': 'CONT-538921'}

AI input:
{
  "PersonInd": "Y",
  "TaxId": "419-50-3417",
  "EffectiveDate": "2024-08-06",
  "LastName": "Wheeler",
  "FirstName": "Sarah",
  "MiddleName": "Matthew",
  "NameSalutation": "Mr",
  "NameSuffix": "III",
  "Citizenship": "North Korea",
  "Gender": "M",
  "DateOfBirth": "2003-04-24",
  "CompanyName": "Rodriguez-Ramirez",
  "CompanyType": "Individual",
  "IndividualContactID": "CONT-216093"
}
AI output:
{'PersonInd': 'Individual', 'Tax_Id': '419-50-3417', 'Effective_Date': '2024-08-06', 'LastName': 'Wheeler', 'FirstName': 'Sarah', 'MiddleName': 'Matthew', 'NameSalutation': 'Mr', 'NameSuffix': 'III', 'Citizenship': 'Nk', 'Gender': 1, 'DateOfBirth': '2003-04-24', 'CompanyName': 'CTS', 'CompanyType': 'Individual', 'IndividualContactID': 'CONT-216093'}

AI input:
{
  "PersonInd": "N",
  "TaxId": "787-44-2552",
  "EffectiveDate": "2023-06-08",
  "LastName": "Hale",
  "FirstName": "Benjamin",
  "MiddleName": "Rebecca",
  "NameSalutation": "Mr",
  "NameSuffix": "Jr",
  "Citizenship": "Togo",
  "Gender": "F",
  "DateOfBirth": "2002-12-26",
  "CompanyName": "Johnson LLC",
  "CompanyType": "Corporate",
  "IndividualContactID": "CONT-436981"
}
AI output:
{'PersonInd': 'Company', 'Tax_Id': '787-44-2552', 'Effective_Date': '2023-06-08', 'LastName': 'Hale', 'FirstName': 'Benjamin', 'MiddleName': 'Rebecca', 'NameSalutation': 'Mr', 'NameSuffix': 'Jr', 'Citizenship': '', 'Gender': 2, 'DateOfBirth': '2002-12-26', 'CompanyName': 'CTS', 'CompanyType': 'Corporate', 'IndividualContactID': 'CONT-436981'}

AI input:
{
  "PersonInd": "Y",
  "TaxId": "810-31-8111",
  "EffectiveDate": "2024-09-21",
  "LastName": "Reed",
  "FirstName": "Charles",
  "MiddleName": "Jason",
  "NameSalutation": "Mr",
  "NameSuffix": "III",
  "Citizenship": "Malawi",
  "Gender": "F",
  "DateOfBirth": "2002-09-20",
  "CompanyName": "Smith-Martinez",
  "CompanyType": "Corporate",
  "IndividualContactID": "CONT-19602"
}
AI output:
{'PersonInd': 'Individual', 'Tax_Id': '810-31-8111', 'Effective_Date': '2024-09-21', 'LastName': 'Reed', 'FirstName': 'Charles', 'MiddleName': 'Jason', 'NameSalutation': 'Mr', 'NameSuffix': 'III', 'Citizenship': '', 'Gender': 2, 'DateOfBirth': '2002-09-20', 'CompanyName': 'CTS', 'CompanyType': 'Corporate', 'IndividualContactID': 'CONT-19602'}

AI input:
{
  "PersonInd": "Y",
  "TaxId": "383-84-5381",
  "EffectiveDate": "2024-11-09",
  "LastName": "Mitchell",
  "FirstName": "Dustin",
  "MiddleName": "Michael",
  "NameSalutation": "Prof",
  "NameSuffix": "Sr",
  "Citizenship": "Laos",
  "Gender": "M",
  "DateOfBirth": "1968-10-14",
  "CompanyName": "Rodriguez Obrien and Stephens",
  "CompanyType": "Partnership",
  "IndividualContactID": "CONT-801411"
}
AI output:
{'PersonInd': 'Individual', 'Tax_Id': '383-84-5381', 'Effective_Date': '2024-11-09', 'LastName': 'Mitchell', 'FirstName': 'Dustin', 'MiddleName': 'Michael', 'NameSalutation': 'Prof', 'NameSuffix': 'Sr', 'Citizenship': '', 'Gender': 1, 'DateOfBirth': '1968-10-14', 'CompanyName': 'CTS', 'CompanyType': 'Partnership', 'IndividualContactID': 'CONT-801411'}

AI input:
{
  "PersonInd": "N",
  "TaxId": "692-20-4283",
  "EffectiveDate": "2023-09-08",
  "LastName": "Ferguson",
  "FirstName": "Kendra",
  "MiddleName": "Amanda",
  "NameSalutation": "Prof",
  "NameSuffix": "Sr",
  "Citizenship": "Benin",
  "Gender": "M",
  "DateOfBirth": "1987-01-26",
  "CompanyName": "Mullins and Sons",
  "CompanyType": "Individual",
  "IndividualContactID": "CONT-246408"
}
AI output:
{'PersonInd': 'Company', 'Tax_Id': '692-20-4283', 'Effective_Date': '2023-09-08', 'LastName': 'Ferguson', 'FirstName': 'Kendra', 'MiddleName': 'Amanda', 'NameSalutation': 'Prof', 'NameSuffix': 'Sr', 'Citizenship': '', 'Gender': 1, 'DateOfBirth': '1987-01-26', 'CompanyName': 'CTS', 'CompanyType': 'Individual', 'IndividualContactID': 'CONT-246408'}

AI input:
{
  "PersonInd": "Y",
  "TaxId": "851-21-1764",
  "EffectiveDate": "2023-11-20",
  "LastName": "Lucero",
  "FirstName": "Thomas",
  "MiddleName": "Bernard",
  "NameSalutation": "Mr",
  "NameSuffix": "Sr",
  "Citizenship": "Uganda",
  "Gender": "F",
  "DateOfBirth": "1986-06-04",
  "CompanyName": "Ramos Alexander and Davis",
  "CompanyType": "Individual",
  "IndividualContactID": "CONT-386015"
}
AI output:
{'PersonInd': 'Individual', 'Tax_Id': '851-21-1764', 'Effective_Date': '2023-11-20', 'LastName': 'Lucero', 'FirstName': 'Thomas', 'MiddleName': 'Bernard', 'NameSalutation': 'Mr', 'NameSuffix': 'Sr', 'Citizenship': '', 'Gender': 2, 'DateOfBirth': '1986-06-04', 'CompanyName': 'CTS', 'CompanyType': 'Individual', 'IndividualContactID': 'CONT-386015'}

AI input:
{
  "PersonInd": "Y",
  "TaxId": "590-29-7246",
  "EffectiveDate": "2024-04-22",
  "LastName": "Morrison",
  "FirstName": "Kelly",
  "MiddleName": "Brenda",
  "NameSalutation": "Dr",
  "NameSuffix": "Sr",
  "Citizenship": "Mexico",
  "Gender": "M",
  "DateOfBirth": "2003-12-03",
  "CompanyName": "Christian-Steele",
  "CompanyType": "Partnership",
  "IndividualContactID": "CONT-803016"
}
AI output:
{'PersonInd': 'Individual', 'Tax_Id': '590-29-7246', 'Effective_Date': '2024-04-22', 'LastName': 'Morrison', 'FirstName': 'Kelly', 'MiddleName': 'Brenda', 'NameSalutation': 'Dr', 'NameSuffix': 'Sr', 'Citizenship': '', 'Gender': 1, 'DateOfBirth': '2003-12-03', 'CompanyName': 'CTS', 'CompanyType': 'Partnership', 'IndividualContactID': 'CONT-803016'}

AI input:
{
  "PersonInd": "N",
  "TaxId": "916-72-0308",
  "EffectiveDate": "2024-04-05",
  "LastName": "Collins",
  "FirstName": "Rhonda",
  "MiddleName": "Cassie",
  "NameSalutation": "Dr",
  "NameSuffix": NaN,
  "Citizenship": "North Macedonia",
  "Gender": "F",
  "DateOfBirth": "1952-02-28",
  "CompanyName": "Nguyen and Sons",
  "CompanyType": "Individual",
  "IndividualContactID": "CONT-83236"
}
AI output:
{'PersonInd': 'Company', 'Tax_Id': '916-72-0308', 'Effective_Date': '2024-04-05', 'LastName': 'Collins', 'FirstName': 'Rhonda', 'MiddleName': 'Cassie', 'NameSalutation': 'Dr', 'NameSuffix': '', 'Citizenship': 'NM', 'Gender': 2, 'DateOfBirth': '1952-02-28', 'CompanyName': 'CTS', 'CompanyType': 'Individual', 'IndividualContactID': 'CONT-83236'}

AI input:
{
  "PersonInd": "N",
  "TaxId": "486-92-5941",
  "EffectiveDate": "2023-11-20",
  "LastName": "Hernandez",
  "FirstName": "Charles",
  "MiddleName": "Adam",
  "NameSalutation": "Prof",
  "NameSuffix": NaN,
  "Citizenship": "Sao Tome and principe",
  "Gender": "M",
  "DateOfBirth": "1969-10-28",
  "CompanyName": "Bush Inc",
  "CompanyType": "Individual",
  "IndividualContactID": "CONT-813216"
}
AI output:
{'PersonInd': 'Company', 'Tax_Id': '486-92-5941', 'Effective_Date': '2023-11-20', 'LastName': 'Hernandez', 'FirstName': 'Charles', 'MiddleName': 'Adam', 'NameSalutation': 'Prof', 'NameSuffix': '', 'Citizenship': '', 'Gender': 1, 'DateOfBirth': '1969-10-28', 'CompanyName': 'CTS', 'CompanyType': 'Individual', 'IndividualContactID': 'CONT-813216'}


'''