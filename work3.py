import pandas as pd
from flask import Flask, render_template, url_for, request, jsonify
import os
import json
import pandas as pd
from langchain_openai import AzureChatOpenAI
import numpy as np

os.environ["AZURE_OPENAI_API_KEY"] = "xxxxx"
os.environ["AZURE_OPENAI_ENDPOINT"] = "xx"
os.environ["AZURE_OPENAI_API_VERSION"] = "xxx"
os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT_NAME"] = "gpt-4o"

model = AzureChatOpenAI(
    openai_api_version=os.environ["AZURE_OPENAI_API_VERSION"],
    azure_deployment=os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT_NAME"],
    temperature=0.0
)

app = Flask(__name__)

def transform_to_df(file):
    """Load CSV and handle NaN values properly"""
    df = pd.read_csv(file)
    # Replace NaN with None for proper JSON serialization
    df = df.where(pd.notnull(df), None)
    return df

# Load CSV once at startup
csv_df = transform_to_df('NAM 4.csv')

def clean_row_for_json(row_dict):
    """Clean row data for JSON serialization"""
    cleaned = {}
    for key, value in row_dict.items():
        if pd.isna(value) or value is np.nan:
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned

def transform_row_with_ai(input_row, transformation_dict):
    """Transform a single row using AI with improved error handling"""
    if not input_row:
        return {}

    # Clean the input row for JSON serialization
    clean_input = clean_row_for_json(input_row)
    
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
3. Return ONLY a valid JSON object, no additional text or explanation.

INPUT ROW:
{json.dumps(clean_input, indent=2)}

Return only the transformed row as a valid JSON dictionary with final target column names.
"""

    try:
        response = model.invoke(prompt)
        content = response.content.strip()
        
        # More robust JSON extraction
        if content.startswith('```json'):
            content = content.replace('```json', '').replace('```', '').strip()
        
        # Find JSON boundaries
        start_index = content.find('{')
        end_index = content.rfind('}') + 1

        if start_index == -1 or end_index <= start_index:
            print(f"No valid JSON found in AI response: {content}")
            return {}

        json_str = content[start_index:end_index]
        
        try:
            result = json.loads(json_str)
            return result
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            print(f"Problematic JSON: {json_str}")
            return {}
            
    except Exception as e:
        print(f"Error in AI transformation: {e}")
        return {}

def process_transformations(transformation_dict, batch_size=5):
    """Process transformations with batching and better error handling"""
    input_df = transform_to_df('NAM 4.csv')
    
    print(f"Processing {len(input_df)} rows...")
    result_rows = []
    failed_rows = []

    for index, row in input_df.iterrows():
        print(f"Processing row {index + 1}/{len(input_df)}")
        
        input_row = row.to_dict()
        transformed_row = transform_row_with_ai(input_row, transformation_dict)
        
        if transformed_row:
            result_rows.append(transformed_row)
            print(f"✓ Row {index + 1} transformed successfully")
        else:
            failed_rows.append(index + 1)
            print(f"✗ Row {index + 1} transformation failed")
            # Add empty row to maintain index alignment
            result_rows.append({})

    # Create output DataFrame
    if result_rows:
        output_df = pd.DataFrame(result_rows)
        
        # Create output directory
        output_folder = "Output"
        os.makedirs(output_folder, exist_ok=True)
        
        # Save results
        output_file = os.path.join(output_folder, "mapped_output_file.csv")
        output_df.to_csv(output_file, index=False)
        
        print(f"\n=== TRANSFORMATION COMPLETE ===")
        print(f"Total rows processed: {len(input_df)}")
        print(f"Successful transformations: {len(result_rows) - len(failed_rows)}")
        print(f"Failed transformations: {len(failed_rows)}")
        if failed_rows:
            print(f"Failed row numbers: {failed_rows}")
        print(f"Output saved to: {output_file}")
        
        return {
            "success": True,
            "total_rows": len(input_df),
            "successful_rows": len(result_rows) - len(failed_rows),
            "failed_rows": failed_rows,
            "output_file": output_file
        }
    else:
        print("No successful transformations!")
        return {
            "success": False,
            "error": "No successful transformations"
        }

def create_transformation_dict(form_data):
    """Create transformation dictionary from form data"""
    result_dict = {}
    
    # Convert mapping string to dictionary
    mapping_str = form_data['mapping']
    mapping_dict = {}
    
    for line in mapping_str.splitlines():
        line = line.strip()
        if '=' in line:
            key, value = line.split('=', 1)  # Split only on first '='
            mapping_dict[key.strip()] = value.strip()

    result_dict[form_data["target_column"]] = {
        "type": "T",
        "rule_payload": {
            "source_column": form_data["source_column"],
            "mapping": mapping_dict
        },
        "target_column": form_data["target_column"]
    }

    return result_dict

@app.route('/', methods=['GET', 'POST'])
def home(): 
    if request.method == 'POST':
        try:
            form_data = request.form
            print("=== STARTING TRANSFORMATION ===")
            
            # Create transformation dictionary
            transformation_dict = create_transformation_dict(form_data)
            print(f"Transformation rules: {json.dumps(transformation_dict, indent=2)}")
            
            # Process transformations
            result = process_transformations(transformation_dict)
            
            if result["success"]:
                return jsonify({
                    "status": "success",
                    "message": f"Transformation complete! Processed {result['successful_rows']}/{result['total_rows']} rows successfully.",
                    "details": result
                })
            else:
                return jsonify({
                    "status": "error",
                    "message": "Transformation failed",
                    "details": result
                }), 500
                
        except Exception as e:
            print(f"Error in home route: {e}")
            return jsonify({
                "status": "error",
                "message": f"An error occurred: {str(e)}"
            }), 500
    
    return 'Data Transformation API - Use /forms to access the form interface'

@app.route('/forms')
def forms():
    return render_template('form.html', source_cls=csv_df.columns)

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "csv_loaded": len(csv_df) > 0,
        "csv_rows": len(csv_df),
        "csv_columns": list(csv_df.columns)
    })

if __name__ == "__main__":
    print(f"Loaded CSV with {len(csv_df)} rows and {len(csv_df.columns)} columns")
    print(f"Columns: {list(csv_df.columns)}")
    app.run(debug=True)