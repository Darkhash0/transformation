import pandas as pd
from flask import Flask, render_template, url_for, request
import os
import json
import pandas as pd
from langchain_openai import AzureChatOpenAI
# import gradio as gr # Gradio is imported but not used in the Flask app part
import numpy as np # For handling NaN

os.environ["AZURE_OPENAI_API_KEY"] = "xxxxx" # Replace with your actual key
os.environ["AZURE_OPENAI_ENDPOINT"] = "xx" # Replace with your actual endpoint
os.environ["AZURE_OPENAI_API_VERSION"] = "xxx" # Replace with your actual version
os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT_NAME"] = "gpt-4o" # Replace with your deployment name


model = AzureChatOpenAI(
    openai_api_version=os.environ["AZURE_OPENAI_API_VERSION"],
    azure_deployment=os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT_NAME"],
    temperature=0.0
)

app = Flask(__name__)


def transform_to_df(file_path): # Changed 'file' to 'file_path' for clarity
    return pd.read_csv(file_path)

# Load the CSV once globally or pass it around; avoid reloading in multiple functions if it's static
# For this example, keeping it as is, but consider optimizing if 'NAM 4.csv' doesn't change per request
csv_df = transform_to_df('NAM 4.csv')

def transformation_dict_debug(form_data): # Renamed to avoid conflict if it was for debugging
    print("\n--- Form Data Received ---")
    for key,value in form_data.items():
        print(f"key: {key}, value: {value}")
    print("-------------------------\n")


def display_data(form_data):
    print('displaying display data function')
    result_dict = {}

    # Convert mapping string to dictionary
    mapping_str = form_data['mapping']
    mapping_dict = {}
    for line in mapping_str.splitlines():
        if '=' in line:
            key_val_pair = line.split('=', 1) # Split only on the first '='
            if len(key_val_pair) == 2:
                key, value = key_val_pair
                mapping_dict[key.strip()] = value.strip()

    result_dict[form_data["target_column"]] = {
        "type": "T", # Assuming type 'T' for now based on original logic
        "rule_payload": {
            "source_column": form_data["source_column"],
            "mapping": mapping_dict
        },
        "target_column": form_data["target_column"]
    }

    print("--- Generated Transformation Rule ---")
    print(json.dumps(result_dict, indent=2))
    print("-----------------------------------\n")
    print('Calling go_to_func...')
    go_to_func(result_dict, csv_df.copy()) # Pass the dataframe to avoid reloading


def transform_row_with_ai(input_row_dict_sanitized, transformation_rules_dict):
    # input_row_dict_sanitized is already a Python dict with NaN replaced by None
    if not input_row_dict_sanitized:
        return {}

    prompt = f"""
        You are a data transformation engine. Your task is to process an INPUT ROW based on TRANSFORMATION RULES and return a complete JSON dictionary.

        TRANSFORMATION RULES:
        {json.dumps(transformation_rules_dict, indent=2)}

        RULE TYPES:
        - 'T' (Transform): Uses a mapping to change values from a `source_column` to a `target_column`.
        - 'D' (Default): Sets a `target_column` to a default value. The `source_column` might be specified for context but its value isn't directly used for mapping.
        - 'O' (One-to-One): Copies the value from a `source_column` to a `target_column`.

        INSTRUCTIONS:
        1.  **Preserve all original columns and their values from the `INPUT ROW` by default.** Start your processing with a copy of the `INPUT ROW`.
        2.  For each rule in `TRANSFORMATION RULES`:
            a.  Identify the `source_column` (if applicable for the rule type) and the `target_column`.
            b.  Apply the rule to determine the value for the `target_column`.
                - For 'T' rule: If the `source_column`'s value is not in the mapping, or if the `source_column` itself is missing from `INPUT ROW` or its value is null, the `target_column`'s value should be an empty string `""`.
                - For 'O' rule: If `source_column` is missing from `INPUT ROW` or its value is null, the `target_column`'s value should be an empty string `""`.
                - For 'D' rule: Use the provided default value.
            c.  Update your working copy of the row:
                i.  Set the `target_column` to the new value. This might overwrite an existing column if `target_column` has the same name as an original column, or it will add a new column if the name is different.
                ii. **If a `source_column` was specified in the rule, existed in the `INPUT ROW`, and its name is DIFFERENT from the `target_column` name, then REMOVE the original `source_column` from the row.** This ensures the source data is replaced by its transformed version under the new name and avoids data duplication.
        3.  The final output must be a single JSON dictionary representing the fully transformed row. This dictionary should include all original columns that were not explicitly removed as per instruction 2.c.ii, plus any new target columns.
        4.  Return only the valid JSON dictionary as your response.

        INPUT ROW (provided as a JSON object, where 'null' represents missing/NaN values):
        {json.dumps(input_row_dict_sanitized, indent=2)}

        Example for 'T' type if INPUT ROW is {{"id": 10, "gender_code": "M", "age": 30}} and TRANSFORMATION RULES are {{ "gender_full": {{ "type": "T", "rule_payload": {{ "source_column": "gender_code", "mapping": {{"M": "Male", "F": "Female"}} }}, "target_column": "gender_full" }} }}:
        The expected output JSON would be: {{"id": 10, "age": 30, "gender_full": "Male"}} (Original "gender_code" is removed as it's different from "gender_full" and was processed).

        Example for 'O' type if INPUT ROW is {{"A": 1, "B": 2, "C": 3}} and TRANSFORMATION RULES are {{ "new_B": {{ "type": "O", "rule_payload": {{ "source_column": "B" }}, "target_column": "new_B" }} }}:
        The expected output JSON would be: {{"A": 1, "C": 3, "new_B": 2}} (Original "B" is removed).

        Return only the transformed row as a valid JSON dictionary.
    """

    try:
        response = model.invoke(prompt)
        content = response.content.strip()
        
        # The AI response might sometimes include markdown ```json ... ```
        if content.startswith("```json"):
            content = content[7:]
        if content.endswith("```"):
            content = content[:-3]
        content = content.strip()

        start_index = content.find('{')
        end_index = content.rfind('}') + 1

        if start_index == -1 or end_index == -1 or start_index >= end_index : # Check for valid braces
            print(f"Invalid AI response structure (no valid JSON object found): {content}")
            return {"error": "AI response not a valid JSON object", "raw_response": content}


        return json.loads(content[start_index:end_index])
    except json.JSONDecodeError as e:
        print(f"Failed to decode AI response: {content}. Error: {e}")
        return {"error": "JSONDecodeError", "raw_response": content}
    except Exception as e:
        print(f"An unexpected error occurred during AI call or processing: {e}")
        return {"error": str(e), "raw_response": ""}


def go_to_func(transformation_rules_dict, input_df): # Pass df to avoid reloading
    result_rows = []
    print(f"\nStarting transformations for {len(input_df)} rows...")

    for index, row in input_df.iterrows():
        original_row_dict = row.to_dict()
        
        # Handle NaN/NA for JSON serialization, converting to None (which becomes null in JSON)
        # This ensures the prompt sent to the AI contains valid JSON.
        input_row_for_ai = {k: (None if pd.isna(v) else v) for k, v in original_row_dict.items()}

        print(f"\n--- Processing Row {index + 1} ---")
        # print(f"Original row data (Python dict):\n{original_row_dict}") # For debugging internal representation
        # print(f"Input to AI (JSON compatible):\n{json.dumps(input_row_for_ai, indent=2)}") # For debugging AI input

        transformed_row_from_ai = transform_row_with_ai(input_row_for_ai, transformation_rules_dict)
        
        # print(f"AI output (raw content might be logged in transform_row_with_ai on error)")
        print(f"AI output (parsed JSON):\n{json.dumps(transformed_row_from_ai, indent=2)}")

        if not isinstance(transformed_row_from_ai, dict) or not transformed_row_from_ai or "error" in transformed_row_from_ai:
            print(f"Warning: AI returned invalid, empty, or error data for row {index + 1}. Original input: {input_row_for_ai}")
            print(f"AI response details: {transformed_row_from_ai.get('raw_response', 'N/A') if isinstance(transformed_row_from_ai, dict) else 'Not a dict'}")
            # Fallback: append the original row (sanitized version) to maintain data integrity in output
            result_rows.append(input_row_for_ai) 
            continue
        
        result_rows.append(transformed_row_from_ai)

    if not result_rows:
        print("No rows were processed or AI returned empty/error for all. Output will be empty.")
        output_df = pd.DataFrame()
    else:
        output_df = pd.DataFrame(result_rows)

    # Sensible column ordering for the output CSV
    if not output_df.empty:
        final_ordered_columns = []
        original_input_cols = list(input_df.columns) # Columns from the source CSV
        
        # Map of source_column -> target_column for columns that were renamed
        renamed_source_to_target_map = {}
        for target_col, rule_details in transformation_rules_dict.items():
            source_col = rule_details.get("rule_payload", {}).get("source_column")
            if source_col and source_col != target_col:
                renamed_source_to_target_map[source_col] = target_col

        # Start with original columns, replacing them with their target names if they were renamed
        for col_name in original_input_cols:
            if col_name in renamed_source_to_target_map: # This column was a source for a rename
                target_name = renamed_source_to_target_map[col_name]
                if target_name in output_df.columns and target_name not in final_ordered_columns:
                    final_ordered_columns.append(target_name)
            elif col_name in output_df.columns and col_name not in final_ordered_columns: # Original column, not renamed, and present in output
                final_ordered_columns.append(col_name)
        
        # Add any other columns present in the output_df that weren't covered
        # (e.g., new target columns that weren't renames of existing ones)
        for col_name in output_df.columns:
            if col_name not in final_ordered_columns:
                final_ordered_columns.append(col_name)
        
        # Ensure all columns in the ordering list actually exist in the DataFrame
        existing_final_columns = [col for col in final_ordered_columns if col in output_df.columns]
        output_df = output_df[existing_final_columns]

    output_folder = "Output"
    os.makedirs(output_folder, exist_ok=True)
    output_file = os.path.join(output_folder, "mapped_output_file.csv")
    output_df.to_csv(output_file, index=False)
    print(f"\nTransformation complete. Output saved to: {output_file}")


@app.route('/', methods=['GET', 'POST'])
def home(): 
    if request.method == 'POST':
        form_data = request.form
        transformation_dict_debug(form_data) # Use the renamed debug function
        display_data(form_data) # This will call go_to_func
        return 'Transformation process initiated and likely completed. Check console and Output folder.'
    return 'Flask app is running. POST to this endpoint to trigger transformation or GET /forms for the form.'

@app.route('/forms')
def forms():
    # Ensure csv_df is available here; it's loaded globally in this script
    return render_template('form.html', source_cls=csv_df.columns)


if __name__ == "__main__":
    # Ensure 'NAM 4.csv' exists in the same directory or provide the correct path.
    # Ensure 'form.html' exists in a 'templates' subdirectory.
    if not os.path.exists('NAM 4.csv'):
        print("Error: 'NAM 4.csv' not found. Please create it or provide the correct path.")
    if not os.path.exists('templates/form.html'):
         print("Error: 'templates/form.html' not found. Please create it.")
    app.run(debug=True)

"""
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

    else:
        print(f"Unknown transformation type: {rule_type}")
        return

    print(result_dict)
    print('\n calling go_to_func')
    go_to_func(result_dict)

"""