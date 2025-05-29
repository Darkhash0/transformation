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
    


def process_data(form_data):
    print('displaying display data function')
    result_dict = {}

    #string to dictionary
    # mapping_str = form_data['mapping']
    # mapping_dict = {}
    # for line in mapping_str.splitlines():
    #     if '=' in line:
    #         key, value = line.split('=')
    #         mapping_dict[key.strip()] = value.strip()

    # Tform
    # result_dict[form_data["target_column"]] = {
    #     "type": form_data["type"],
    #     "rule": {
    #         "source_column": form_data["source_column"],
    #         "mapping": mapping_dict
    #     },
    #     "target_column": form_data["target_column"]
    # }


    # Oform
    # result_dict[form_data["target_column"]] = {
    #     "type": form_data["type"],
    #     "rule": {
    #         "source_column": form_data["source_column"]
    #     },
    #     "target_column": form_data["target_column"]
    #     }


    # Xform
    result_dict[form_data["target_column"]] = {
        "type": form_data["type"],
        "rule": {
            "source_column": form_data["source_column"],
            "instruction": form_data['mapping']
        },
        "target_column": form_data["target_column"]
        }


    print(result_dict)
    print('\n calling go to func')
    go_to_func(result_dict)


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
            - 'X' (Custom rule): Follow the custom instruction exactly

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
        result_rows.append(transformed_row)

    output_df = pd.DataFrame(result_rows)

    output_folder = "Output"
    os.makedirs(output_folder, exist_ok=True)

    output_file = os.path.join(output_folder, "mapped_output_file.csv")
    output_df.to_csv(output_file, index=False)
    print(f"Transformation complete. Output saved to: {output_file}")




@app.route('/', methods=['GET', 'POST'])
def home(): 
    return 'Hi'

@app.route('/Tforms')
def Tforms():
    return render_template('Tform.html', source_cls=csv_df.columns)



@app.route('/Oforms')
def Oforms(): 
    return render_template('Oform.html', source_cls=csv_df.columns)


@app.route('/Xforms')
def Xforms(): 
    return render_template('Xform.html', source_cls=csv_df.columns)


@app.route('/processing', methods=['GET', 'POST'])
def processing():
    if request.method == 'POST':
        form_data =  request.form
        transformation_dict(form_data)
        process_data(form_data)
    return render_template('processing.html')


 
if __name__ == "__main__":
    app.run(debug=True)

