import streamlit as st
import pandas as pd
import json
import io
import os
from typing import Dict, Any, List
import time

# Import for Azure OpenAI
try:
    from langchain_openai import AzureChatOpenAI
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

# Import for regular OpenAI
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# ===== CONFIGURATION SECTION =====
# Set your API configuration here
AI_CONFIG = {
    "type": "openai",  # Options: "openai" or "azure"
    "api_key": "your-api-key-here",  # Replace with your actual API key
    
    # Only needed for Azure OpenAI
    "endpoint": "https://your-resource.openai.azure.com/",
    "api_version": "2024-02-15-preview",
    "deployment_name": "gpt-35-turbo"
}
# ================================

# Configure page
st.set_page_config(
    page_title="AI Data Transformation System",
    page_icon="🤖",
    layout="wide"
)

class AITransformer:
    def __init__(self, config_type: str, **kwargs):
        self.config_type = config_type

        if config_type == "azure" and AZURE_AVAILABLE:
            # Set up environment variables for Azure
            os.environ["AZURE_OPENAI_API_KEY"] = kwargs.get("api_key", "")
            os.environ["AZURE_OPENAI_ENDPOINT"] = kwargs.get("endpoint", "")
            os.environ["AZURE_OPENAI_API_VERSION"] = kwargs.get("api_version", "")
            os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT_NAME"] = kwargs.get("deployment_name", "")

            # Initialize Azure OpenAI model
            self.model = AzureChatOpenAI(
                openai_api_version=os.environ["AZURE_OPENAI_API_VERSION"],
                azure_deployment=os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT_NAME"],
                temperature=0.0
            )

        elif config_type == "openai" and OPENAI_AVAILABLE:
            self.client = OpenAI(api_key=kwargs.get("api_key", ""))
        else:
            raise ValueError(f"Configuration type '{config_type}' not supported or libraries not installed")

    def transform_row(self, row_data: dict, rules: dict) -> dict:
        """Transform a single row using AI based on the rules"""

        # Create the prompt
        prompt = f"""
            You are a data transformation expert. Apply the following transformation rules to the input data row.

            RULE TYPES:
            - T (Translate): Transform values based on mapping (e.g., "Male" -> "M", "Female" -> "F")
            - D (Default): Replace the source value with the default value
            - C (Concatenate): Join multiple fields with a separator
            - R (Rename): Copy column as-is but with a new name
            - X (Custom): Apply custom logic as described

            TRANSFORMATION RULES:
            {json.dumps(rules, indent=2)}

            INPUT ROW:
            {json.dumps(row_data, indent=2)}

            INSTRUCTIONS:
            1. Apply each rule to create the target columns
            2. For T rules: Use the mapping provided
            3. For D rules: Replace the source value with the default value
            4. For C rules: Join the specified columns with the separator
            5. For R rules: Copy the value to new column name
            6. For X rules: Follow the custom instruction exactly
            7. Return ONLY the transformed target columns as JSON
            8. Do not include original columns unless they are target columns

            OUTPUT (JSON only):
            """

        try:
            if self.config_type == "azure":
                # Use Azure OpenAI via LangChain
                response = self.model.invoke(prompt)
                result_text = response.content.strip()

            elif self.config_type == "openai":
                # Use regular OpenAI
                response = self.client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                    max_tokens=1000
                )
                result_text = response.choices[0].message.content.strip()

            # Try to extract JSON from the response
            try:
                # Remove any markdown formatting
                if "```json" in result_text:
                    result_text = result_text.split("```json")[1].split("```")[0]
                elif "```" in result_text:
                    result_text = result_text.split("```")[1].split("```")[0]

                result = json.loads(result_text)
                return result
            except json.JSONDecodeError:
                st.error(f"Failed to parse AI response as JSON: {result_text}")
                return {}

        except Exception as e:
            st.error(f"AI transformation failed: {str(e)}")
            return {}

def validate_config():
    """Validate the internal configuration"""
    if AI_CONFIG["api_key"] == "your-api-key-here":
        st.error("⚠️ Please update the API_KEY in the configuration section of the code!")
        st.code("""
# Update this section in the code:
AI_CONFIG = {
    "type": "openai",  # or "azure"
    "api_key": "your-actual-api-key-here",
    # ... other config
}
        """)
        st.stop()
    
    config_type = AI_CONFIG["type"]
    
    if config_type == "azure":
        if not AZURE_AVAILABLE:
            st.error("Azure OpenAI libraries not installed. Please install: pip install langchain-openai")
            st.stop()
        required_fields = ["api_key", "endpoint", "api_version", "deployment_name"]
        missing = [field for field in required_fields if not AI_CONFIG.get(field)]
        if missing:
            st.error(f"Missing Azure OpenAI configuration: {', '.join(missing)}")
            st.stop()
    
    elif config_type == "openai":
        if not OPENAI_AVAILABLE:
            st.error("OpenAI library not installed. Please install: pip install openai")
            st.stop()
        if not AI_CONFIG.get("api_key"):
            st.error("Missing OpenAI API key in configuration")
            st.stop()
    
    else:
        st.error(f"Unsupported AI configuration type: {config_type}")
        st.stop()

def create_rule_configuration():
    """Create the rule configuration UI"""
    st.subheader("🔧 Configure Transformation Rules")

    rules = {}

    # Add rule button
    if 'rule_count' not in st.session_state:
        st.session_state.rule_count = 1

    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("➕ Add Rule"):
            st.session_state.rule_count += 1

    # Rule configuration
    for i in range(st.session_state.rule_count):
        with st.expander(f"Rule {i+1}", expanded=True):
            col1, col2 = st.columns(2)

            with col1:
                rule_type = st.selectbox(
                    "Rule Type",
                    ["T (Translate)", "D (Default)", "C (Concatenate)", "R (Rename)", "X (Custom)"],
                    key=f"rule_type_{i}"
                )

                target_column = st.text_input("Target Column Name", key=f"target_{i}")

            with col2:
                rule_code = rule_type[0]  # Extract just the letter

                if rule_code == "T":
                    source_col = st.text_input("Source Column", key=f"source_{i}")
                    mapping_text = st.text_area(
                        "Value Mapping (key=value, one per line)",
                        placeholder="Male=M\nFemale=F\nOther=O",
                        key=f"mapping_{i}"
                    )

                    if target_column and source_col and mapping_text:
                        mapping = {}
                        for line in mapping_text.strip().split('\n'):
                            if '=' in line:
                                key, value = line.split('=', 1)
                                mapping[key.strip()] = value.strip()

                        rules[target_column] = {
                            "type": "T",
                            "rule_payload": {
                                "source_column": source_col,
                                "mapping": mapping
                            },
                            "target_column": target_column
                        }

                elif rule_code == "D":
                    source_col = st.text_input("Source Column", key=f"source_{i}")
                    default_value = st.text_input("Default Value", key=f"default_{i}")

                    if target_column and source_col and default_value:
                        rules[target_column] = {
                            "type": "D",
                            "rule_payload": {
                                "source_column": source_col,
                                "default_value": default_value
                            },
                            "target_column": target_column
                        }

                elif rule_code == "C":
                    columns_text = st.text_input(
                        "Columns to Join (comma-separated)",
                        placeholder="first_name,last_name",
                        key=f"columns_{i}"
                    )
                    separator = st.text_input("Separator", value=" ", key=f"separator_{i}")

                    if target_column and columns_text:
                        columns = [col.strip() for col in columns_text.split(',')]
                        rules[target_column] = {
                            "type": "C",
                            "rule_payload": {
                                "columns": columns,
                                "separator": separator
                            },
                            "target_column": target_column
                        }

                elif rule_code == "R":
                    source_col = st.text_input("Source Column", key=f"source_{i}")

                    if target_column and source_col:
                        rules[target_column] = {
                            "type": "R",
                            "rule_payload": {
                                "source_column": source_col
                            },
                            "target_column": target_column
                        }

                elif rule_code == "X":
                    custom_instruction = st.text_area(
                        "Custom Transformation Instruction",
                        placeholder="e.g., Mask email after @ symbol with ***",
                        key=f"custom_{i}"
                    )
                    source_col = st.text_input("Source Column", key=f"source_{i}")

                    if target_column and custom_instruction and source_col:
                        rules[target_column] = {
                            "type": "X",
                            "rule_payload": {
                                "source_column": source_col,
                                "instruction": custom_instruction
                            },
                            "target_column": target_column
                        }

    return rules

def main():
    st.title("🤖 AI-Powered Data Transformation System")
    st.markdown("Upload CSV, configure transformation rules, and download the transformed data!")

    # Validate internal configuration
    validate_config()

    # Show current AI configuration in sidebar (for reference)
    with st.sidebar:
        st.header("AI Configuration")
        st.success(f"✅ {AI_CONFIG['type'].title()} configured")
        if AI_CONFIG['type'] == 'azure':
            st.info(f"Endpoint: {AI_CONFIG['endpoint']}")
            st.info(f"Deployment: {AI_CONFIG['deployment_name']}")
        
        st.markdown("---")
        st.markdown("**To change configuration:**")
        st.markdown("Update the `AI_CONFIG` section in the code")

    # File upload
    st.subheader("📁 Upload Data")
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

    if uploaded_file is not None:
        # Read and display the CSV
        df = pd.read_csv(uploaded_file)

        st.subheader("Source Columns Preview")
        st.dataframe(pd.DataFrame([df.columns], index=["Columns"]))
        st.info(f"Dataset contains {len(df)} rows and {len(df.columns)} columns")

        # Rule configuration
        rules = create_rule_configuration()

        # Show configured rules
        if rules:
            st.subheader("📋 Configured Rules")
            st.json(rules)

        # Transform button
        if st.button("🚀 Transform Data", type="primary"):
            if not rules:
                st.error("Please configure at least one transformation rule")
                return

            # Initialize AI transformer
            try:
                if AI_CONFIG["type"] == "azure":
                    transformer = AITransformer(
                        config_type="azure",
                        api_key=AI_CONFIG["api_key"],
                        endpoint=AI_CONFIG["endpoint"],
                        api_version=AI_CONFIG["api_version"],
                        deployment_name=AI_CONFIG["deployment_name"]
                    )
                else:
                    transformer = AITransformer(
                        config_type="openai",
                        api_key=AI_CONFIG["api_key"]
                    )
            except Exception as e:
                st.error(f"Failed to initialize AI transformer: {str(e)}")
                return

            # Progress tracking
            progress_bar = st.progress(0)
            status_text = st.empty()

            transformed_rows = []
            total_rows = len(df)

            # Transform each row
            for idx, (_, row) in enumerate(df.iterrows()):
                status_text.text(f"Transforming row {idx + 1} of {total_rows}")
                progress_bar.progress((idx + 1) / total_rows)

                # Convert row to dict and handle NaN values
                row_dict = row.to_dict()
                row_dict = {k: (v if pd.notna(v) else None) for k, v in row_dict.items()}

                # Transform the row
                transformed_row = transformer.transform_row(row_dict, rules)
                transformed_rows.append(transformed_row)

                # Small delay to avoid rate limits
                time.sleep(0.1)

            # Create output dataframe
            if transformed_rows and any(transformed_rows):
                output_df = pd.DataFrame(transformed_rows)

                st.subheader("✅ Transformation Complete!")
                st.dataframe(output_df)

                # Download button
                csv_buffer = io.StringIO()
                output_df.to_csv(csv_buffer, index=False)
                csv_data = csv_buffer.getvalue()

                st.download_button(
                    label="📥 Download Transformed CSV",
                    data=csv_data,
                    file_name="transformed_data.csv",
                    mime="text/csv"
                )

                # Show summary
                st.info(f"Successfully transformed {len(output_df)} rows with {len(output_df.columns)} target columns")
            else:
                st.error("Transformation failed. Please check your rules and try again.")

if __name__ == "__main__":
    main()