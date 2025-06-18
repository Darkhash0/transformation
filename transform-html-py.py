import pandas as pd
from flask import Flask, render_template, request
import os
import json
import pandas as pd
from langchain_openai import AzureChatOpenAI

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

csv_df = transform_to_df('NAM 4.csv')


def transformation_dict(form_data):
    for key,value in form_data.items():
        print(f"\n key:{key} values:{value}")
    

def process_data(form_data):
    print('displaying process_data function')
    result_dict = {}

    rule_type = form_data.get("type")

    if rule_type == "T":
        # Mapping string to dictionary
        mapping_str = form_data.get("mapping", "")
        mapping_dict = {}
        for line in mapping_str.splitlines():
            if '=' in line:
                key, value = line.split('=')
                mapping_dict[key.strip()] = value.strip()

        result_dict[form_data["target_column"]] = {
            "type": "T",
            "rule": {
                "source_column": form_data["source_column"],
                "mapping": mapping_dict
            },
            "target_column": form_data["target_column"]
        }

    elif rule_type == "O":
        result_dict[form_data["target_column"]] = {
            "type": "O",
            "rule": {
                "source_column": form_data["source_column"]
            },
            "target_column": form_data["target_column"]
        }

    elif rule_type == "X":
        result_dict[form_data["target_column"]] = {
            "type": "X",
            "rule": {
                "source_column": form_data["source_column"],
                "instruction": form_data.get('mapping', '')
            },
            "target_column": form_data["target_column"]
        }

    elif rule_type == "C":
        result_dict[form_data["target_column"]] = {
            "type": "C",
            "rule": {
                "source_column1": form_data["source_column1"],
                "source_column2": form_data["source_column2"],
                "instruction": form_data.get('mapping', '')
            },
            "target_column": form_data["target_column"]
        }

    else:
        print(f"Unknown transformation type: {rule_type}")
        return

    print(result_dict)
    print('\n calling go_to_func')
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
            - 'O' (One-to-One): Copy the source column's value as-is into the target column.
            - 'C' (Concatenate): Concatenate source column(s) as per the custom instruction exactly into the target column.
            - 'X' (Custom rule): Follow the custom instruction exactly

            INSTRUCTIONS:
            1. Apply the transfomations exactly as instructed.
            2. Replace the original column(s) with the tranformed column(s).
            3. Do not retain the original column if it has been tranformed.
            4. Keep all other columns unchanged and in their original order.
            5. Use the new column name specified for the transformation.

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
    input_df = transform_to_df('NAM 4.csv')


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


@app.route('/Tformsdynamic')
def Tformsdynamic():
    return render_template('updatedropdown.html', source_cls=csv_df.columns.tolist())



@app.route('/Oforms')
def Oforms(): 
    return render_template('Oform.html', source_cls=csv_df.columns)


@app.route('/Xforms')
def Xforms(): 
    return render_template('Xform.html', source_cls=csv_df.columns)


@app.route('/Cforms')
def Cforms(): 
    return render_template('Conform.html', source_cls=csv_df.columns)


@app.route('/processing', methods=['GET', 'POST'])
def processing():
    if request.method == 'POST':
        form_data =  request.form
        transformation_dict(form_data)
        process_data(form_data)
    return render_template('processing.html')


 
if __name__ == "__main__":
    app.run(debug=True)

