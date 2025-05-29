import pandas as pd
from flask import Flask, render_template, url_for, request
import os
import json
import pandas as pd
from langchain_openai import AzureChatOpenAI
import gradio as gr

os.environ["AZURE_OPENAI_API_KEY"] = "70683718b85747ea89724db4214873e7"
os.environ["AZURE_OPENAI_ENDPOINT"] = "https://codedocumentation.openai.azure.com/"
os.environ["AZURE_OPENAI_API_VERSION"] = "2024-02-15-preview"
os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT_NAME"] = "gpt-4o"


model = AzureChatOpenAI(
    openai_api_version=os.environ["AZURE_OPENAI_API_VERSION"],
    azure_deployment=os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT_NAME"],
    temperature=0.0
)

app = Flask(__name__)


def transform_to_df(file):
    return pd.read_csv(file)

csv_df = transform_to_df('sampledata.csv')

def transformation_dict(form_data):
    for key,value in form_data.items():
        print(f"\n key:{key} values:{value}")
    

def display_data(form_data):
    print('displaying display data function')
    result_list = []
    

    # Convert mapping string to dictionary
    mapping_str = form_data['mapping2']
    mapping_dict = {}
    for line in mapping_str.splitlines():
        if '=' in line:
            key, value = line.split('=')
            mapping_dict[key.strip()] = value.strip()
    
    if form_data["type1"] == 'O':
       result_list.append({
            "type": form_data["type1"],
            "type_payload": {
                "source_column": form_data["source_column1"]
            },
            "target_column": form_data["target_column1"]
        })

    if form_data["type2"] == 'T':
        result_list.append( {
            "type": form_data["type2"],
            "type_payload": {
                "source_column": form_data["source_column2"],
                "mapping": mapping_dict
            },
        "target_column": form_data["target_column2"]
        })

    
    

    print(result_list)
    print('\n calling go to func')
    go_to_func(result_list)


def transform_row_with_ai(input_row, transformation_dict):
    if not input_row:
        return {}

    prompt = f"""
            You are a data transformation engine.

            Your job is to transform the given input row using the provided structured transformation types.

            ----------------------
            TRANSFORMATION typeS:
            {json.dumps(transformation_dict, indent=2)}
            ----------------------

            type TYPES:
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




def go_to_func(transformation_dict):
    input_df = transform_to_df('sampledata.csv')


    result_rows = []

    for _, row in input_df.iterrows():
        input_row = row.to_dict()
        transformed_row = transform_row_with_ai(input_row, transformation_dict)
        print(f"\nAI input:\n{json.dumps(input_row,indent=2)}\nAI output:\n{transformed_row}")
        merged_row = {**input_row, **transformed_row}
        result_rows.append(merged_row)
        

    
    
    output_df = pd.DataFrame(result_rows)

    final_columns = list(input_df.columns) + [col for col in output_df.columns if col not in input_df.columns]
    output_df = output_df[final_columns]

    output_folder = "Output"
    os.makedirs(output_folder, exist_ok=True)

    output_file = os.path.join(output_folder, "mapped_output_file.csv")
    output_df.to_csv(output_file, index=False)
    print(f"Transformation complete. Output saved to: {output_file}")


@app.route('/', methods=['GET', 'POST'])
def home(): 
    if request.method == 'POST':
        form_data =  request.form
        transformation_dict(form_data)
        display_data(form_data)
    return 'Hi'

@app.route('/forms')
def forms():
    return render_template('form.html', source_cls=csv_df.columns)


if __name__ == "__main__":
    app.run(debug=True)



'''

❗ Problem:
    Your current implementation only returns the transformed column(s) (e.g., people_gender) from the AI, and drops all the original columns from the final output.
    displaying display data function

{'people_gender': {'type': 'T', 'type_payload': {'source_column': 'Gender', 'mapping': {'M': '1', 'F': '2'}}, 'target_column': 'people_gender'}}

WRONG OUTPUT:

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
{'people_gender': '1'}

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
{'people_gender': '1'}

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
{'people_gender': '2'}

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
{'people_gender': '2'}

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
{'people_gender': '1'}

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
{'people_gender': '1'}

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
{'people_gender': '2'}

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
{'people_gender': '1'}

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
{'people_gender': '2'}

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
  "NameSuffix": NaN,
  "Citizenship": "Sao Tome and principe",
  "Gender": "M",
  "DateOfBirth": "1969-10-28",
  "CompanyName": "Bush Inc",
  "CompanyType": "Individual",
  "IndividualContactID": "CONT-813216"
}
AI output:
  "Citizenship": "Sao Tome and principe",
  "Gender": "M",
  "DateOfBirth": "1969-10-28",
  "CompanyName": "Bush Inc",
  "CompanyType": "Individual",
  "IndividualContactID": "CONT-813216"
}
AI output:
  "DateOfBirth": "1969-10-28",
  "CompanyName": "Bush Inc",
  "CompanyType": "Individual",
  "IndividualContactID": "CONT-813216"
}
AI output:
  "CompanyType": "Individual",
  "IndividualContactID": "CONT-813216"
}
AI output:
  "IndividualContactID": "CONT-813216"
}
AI output:
}
AI output:
AI output:
{'people_gender': '1'}
Transformation complete. Output saved to: Output\mapped_output_file.csv
127.0.0.1 - - [28/May/2025 16:07:19] "POST / HTTP/1.1" 200 -




{'people_gender': '1'}
Transformation complete. Output saved to: Output\mapped_output_file.csv
127.0.0.1 - - [28/May/2025 16:07:19] "POST / HTTP/1.1" 200 -



{'people_gender': '1'}
Transformation complete. Output saved to: Output\mapped_output_file.csv
127.0.0.1 - - [28/May/2025 16:07:19] "POST / HTTP/1.1" 200 -


{'people_gender': '1'}
Transformation complete. Output saved to: Output\mapped_output_file.csv
127.0.0.1 - - [28/May/2025 16:07:19] "POST / HTTP/1.1" 200 -
{'people_gender': '1'}
Transformation complete. Output saved to: Output\mapped_output_file.csv
{'people_gender': '1'}
{'people_gender': '1'}
Transformation complete. Output saved to: Output\mapped_output_file.csv
127.0.0.1 - - [28/May/2025 16:07:19] "POST / HTTP/1.1" 200 -






{'people_gender': '1'}
Transformation complete. Output saved to: Output\mapped_output_file.csv
127.0.0.1 - - [28/May/2025 16:07:19] "POST / HTTP/1.1" 200 -



{'people_gender': '1'}
Transformation complete. Output saved to: Output\mapped_output_file.csv
127.0.0.1 - - [28/May/2025 16:07:19] "POST / HTTP/1.1" 200 -
{'people_gender': '1'}
Transformation complete. Output saved to: Output\mapped_output_file.csv
127.0.0.1 - - [28/May/2025 16:07:19] "POST / HTTP/1.1" 200 -

{'people_gender': '1'}
Transformation complete. Output saved to: Output\mapped_output_file.csv
127.0.0.1 - - [28/May/2025 16:07:19] "POST / HTTP/1.1" 200 -
{'people_gender': '1'}
Transformation complete. Output saved to: Output\mapped_output_file.csv

'''
