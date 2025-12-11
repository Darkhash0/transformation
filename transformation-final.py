import os
import json
import pandas as pd
import numpy as np
from langchain_openai import AzureChatOpenAI
import gradio as gr

os.environ["AZURE_OPENAI_API_KEY"] = "70683714873e7"
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
    df.columns = df.columns.str.capitalize()
    return df

transformation_dict = {}
mapping_instructions=[]

def load_transformation_rules(rules_file_path):
    # df = pd.read_excel(rules_file_path, engine='openpyxl', sheet_name='Transform')
    excel_file_df = pd.read_excel(rules_file_path, engine='openpyxl', sheet_name=None)

    transform_sheet_df = excel_file_df['Transform']
    transform_sheet_df = ( 
        transform_sheet_df.replace(r'^\s*$', np.nan, regex=True)
        .dropna(subset=["MapName", "Map Criteria#1", "Transformed Value"]) 
        .dropna(axis=1,how='all')
        )
    
    for col in transform_sheet_df.columns:
        transform_sheet_df[col] = transform_sheet_df[col].apply(lambda x: x.strip().replace('\xa0', '') if isinstance(x, str) else x)

    for map_name in transform_sheet_df['MapName'].unique():
        group_df = transform_sheet_df[transform_sheet_df['MapName'] == map_name]
        inner_dict = dict(zip(group_df['Map Criteria#1'], group_df['Transformed Value']))
        transformation_dict[map_name] = inner_dict
    

    # mapping_df = pd.read_excel(rules_file_path, engine='openpyxl', sheet_name='Mapping')
    mapping_sheet_df = excel_file_df['Mapping']
    
    for _, row in mapping_sheet_df.iterrows():
        source_col_raw = row.get('Parameter#1', None)
        rule_type = row.get('Transformation Type', None)
        target_col = row.get('STG_Column_Name', None)

        if isinstance(source_col_raw, str) and '+' in source_col_raw:
            fpart = source_col_raw.split('+')[0]
            spart = source_col_raw.split('+')[1]
            if ':' in fpart and ':' in spart:
                first_source_col = fpart.split(':', 1)[1]
                second_source_col = spart.split(':', 1)[1]
            else:
                print("\nthere is no :")

        elif isinstance(source_col_raw, str) and ':' in source_col_raw:
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
        
        # elif rule_type == 'A':
        #     instruction = {
        #         "type": "A",
        #         "source_column": source_col,
        #         "target_column": target_col,
        #         "auto_generate_rule": f"Generate a unique ID using the current timestamp in the format YYYY-DD-MM-HH-MM-SS-ms. Return the generated ID(s) as a string. And assign this value(s) to the {target_col} column.",
        #     }
        #     mapping_instructions.append(instruction)

        elif rule_type == 'A':
            instruction = {
                "type": "A",
                "source_column": source_col,
                "target_column": target_col,
                "auto_generate_rule": f"Generate a unique ID using last four characters of ClientId column followed by current timestamp in the format YYYY-DDMM-HHMM (Day and Month, Hours and Minutes are combined) separated with hyphens(-) after every four characters having max length of 16 characters. Return the generated unique ID as a string. And assign this value to the {target_col} column. Ensure each ID must be unique. No duplicate ID(s)."
                
            }
            mapping_instructions.append(instruction)
        
        # elif rule_type == 'A':
        #     instruction = {
        #         "type": "A",
        #         "source_column": source_col,
        #         "target_column": target_col,
        #     }
        #     mapping_instructions.append(instruction)
        

        elif rule_type == 'J':
            instruction = {
                "type":"J",
                "source_column_1":first_source_col,
                "source_column_2":second_source_col,
                "target_column":target_col,
            }
            mapping_instructions.append(instruction)
        
        elif rule_type == 'X': #custom rule
            instruction = {
                "type":"X",
                "source_column":source_col,
                "target_column":target_col,
                "instruction":"Change the date format from DD-MMM-YYYY to YYYY/DD/MMM. Do not modify the month abbreviation or convert it to a numeric format. Only rearrange the components of the date as specified."
            }
            mapping_instructions.append(instruction)

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
- 'T' (Transform): Get the value from exact source column and replace the value using mapping dictionary. Find the matching column name in mapping dictionary and replace the correct value properly.
- 'D' (Default): Replace source column value with the given default value.
- 'O' (One-to-One): Get value from exact source column and copy as-is into the target column.
- 'A' (Auto-Generate): Generate a unique ID as specified. Ensure each ID must be unique. No duplicate ID(s).
- 'J' (Join/Concatenate): Join/Concatenate given source columns values into the target column.
- 'X' (Custom rule): Exactly follow the instructions.

TRANSFORMATION INSTRUCTIONS:
-> Apply transformations exactly as instructed.
-> For each transformation rule, locate the exact source column name in the input row.
-> If the exact source column exists, use its value according to the rule type.
-> If a value is not found in the mapping dictionary or the exact source column is empty, use an empty string "". Do not guess/randomly replace values.
-> Replace the original column(s) with the tranformed column(s).
-> Do not retain the original column if it has been tranformed.
-> Use the new column name specified for the transformation.
-> Maintain the tranformed column order in the final output.
-> Do not skip any value(s) from the input dataset. carefully map all the necessary value(s).

----------------------
INPUT ROW:
{json.dumps(input_row, indent=2)}
----------------------

Return all the transformed row(s) as a valid JSON object containing only the target columns from the transformation rules.

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


transform_excel('CIFINPUT/NF_CLIENT_24042025.csv', "CIFINPUT/TRANS_REL 3.xlsx")


'''
Excel files:
TRANS_NAM 4 - ok
TRANS_ADRPART02 -ok
TRANS_PHOPART01 -ok
TRANS_PHOPART02 -ok
TRANS_WEB 2 -ok
TRANS_REL 3 -ok
'''