import os
import json
import pandas as pd
from langchain_openai import AzureChatOpenAI
import logging
from typing import Dict, List, Any, Tuple
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataTransformationEngine:
    def __init__(self, azure_config: Dict[str, str]):
        """
        Initialize the AI-powered data transformation engine.
        
        Args:
            azure_config: Dictionary containing Azure OpenAI configuration
        """
        self.model = self._setup_azure_openai(azure_config)
        
    def _setup_azure_openai(self, config: Dict[str, str]) -> AzureChatOpenAI:
        """Setup Azure OpenAI client with provided configuration."""
        for key, value in config.items():
            os.environ[key] = value
            
        return AzureChatOpenAI(
            openai_api_version=os.environ["AZURE_OPENAI_API_VERSION"],
            azure_deployment=os.environ["AZURE_OPENAI_CHAT_DEPLOYMENT_NAME"],
            temperature=0.0
        )

    def load_input_data(self, input_file_path: str) -> pd.DataFrame:
        """
        Load input CSV data with pipe delimiter and normalize column names.
        
        Args:
            input_file_path: Path to input CSV file
            
        Returns:
            DataFrame with normalized column names
        """
        try:
            df = pd.read_csv(input_file_path, delimiter='|')
            df.columns = df.columns.str.lower().str.strip()
            logger.info(f"Loaded input data: {len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.error(f"Error loading input data: {e}")
            raise

    def load_transformation_rules(self, rules_file_path: str) -> Tuple[List[Dict], Dict[str, Dict]]:
        """
        Load transformation rules from Excel file with enhanced error handling.
        
        Args:
            rules_file_path: Path to Excel file containing transformation rules
            
        Returns:
            Tuple of (mapping_instructions, transformation_dict)
        """
        try:
            # Load mapping sheet
            mapping_df = pd.read_excel(rules_file_path, engine='openpyxl', sheet_name='Mapping')
            
            # Load transform sheet
            transform_df = pd.read_excel(rules_file_path, engine='openpyxl', sheet_name='Transform')
            transform_df = transform_df.dropna(how='any')
            
            # Clean transformed values
            if 'Transformed Value' in transform_df.columns:
                transform_df['Transformed Value'] = transform_df['Transformed Value'].astype(str).str.replace('\xa0', '', regex=False)
            
            # Filter valid rows
            required_cols = ["MapName", 'Map Criteria#1', 'Transformed Value']
            transform_df = transform_df.dropna(subset=required_cols)
            
            # Build transformation dictionary
            transformation_dict = self._build_transformation_dict(transform_df)
            
            # Build mapping instructions
            mapping_instructions = self._build_mapping_instructions(mapping_df, transformation_dict)
            
            logger.info(f"Loaded {len(mapping_instructions)} transformation rules")
            logger.info(f"Loaded {len(transformation_dict)} transformation mappings")
            
            return mapping_instructions, transformation_dict
            
        except Exception as e:
            logger.error(f"Error loading transformation rules: {e}")
            raise

    def _build_transformation_dict(self, transform_df: pd.DataFrame) -> Dict[str, Dict]:
        """Build transformation dictionary from transform sheet."""
        transformation_dict = {}
        
        for map_name in transform_df['MapName'].unique():
            group_df = transform_df[transform_df['MapName'] == map_name]
            inner_dict = dict(zip(group_df['Map Criteria#1'], group_df['Transformed Value']))
            transformation_dict[map_name] = inner_dict
            
        return transformation_dict

    def _build_mapping_instructions(self, mapping_df: pd.DataFrame, transformation_dict: Dict) -> List[Dict]:
        """Build mapping instructions from mapping sheet."""
        mapping_instructions = []
        
        for _, row in mapping_df.iterrows():
            source_col_raw = row.get('Parameter#1', None)
            rule_type = row.get('Transformation Type', None)
            target_col = row.get('STG_Column_Name', None)
            
            # Skip invalid rows
            if pd.isna(rule_type) or pd.isna(target_col):
                continue
                
            # Extract substring after colon if present
            source_col = self._extract_source_column(source_col_raw)
            
            # Build instruction based on rule type
            instruction = self._build_instruction(rule_type, source_col, target_col, transformation_dict)
            
            if instruction:
                mapping_instructions.append(instruction)
                
        return mapping_instructions

    def _extract_source_column(self, source_col_raw: Any) -> str:
        """Extract source column name from raw parameter value."""
        if isinstance(source_col_raw, str) and ':' in source_col_raw:
            return source_col_raw.split(':', 1)[1].strip()
        return str(source_col_raw) if source_col_raw is not None else ""

    def _build_instruction(self, rule_type: str, source_col: str, target_col: str, 
                          transformation_dict: Dict) -> Dict[str, Any]:
        """Build individual transformation instruction."""
        rule_type = rule_type.upper().strip()
        
        if rule_type == 'D':
            return {
                "type": "D",
                "default_value": source_col,
                "target_column": target_col,
                "description": f"Set {target_col} to default value: {source_col}"
            }
            
        elif rule_type == 'O':
            return {
                "type": "O",
                "source_column": source_col,
                "target_column": target_col,
                "description": f"Copy {source_col} to {target_col}"
            }
            
        elif rule_type == 'T':
            return {
                "type": "T",
                "source_column": source_col,
                "target_column": target_col,
                "mapping": transformation_dict,
                "description": f"Transform {source_col} to {target_col} using mapping dictionary"
            }
            
        elif rule_type == 'A':
            return {
                "type": "A",
                "source_column": source_col,
                "target_column": target_col,
                "auto_generated_rule": f"Generate a random alphanumeric string with a maximum length of 8 characters. The first 4 characters should be extracted from the value in the {source_col} column. The remaining characters should be randomly generated to complete the string. Assign the final string to the {target_col} column.",
                "description": f"Auto-generate {target_col} based on {source_col}"
            }
            
        else:
            logger.warning(f"Unknown rule type: {rule_type}")
            return None

    def transform_row_with_ai(self, input_row: Dict[str, Any], mapping_instructions: List[Dict]) -> Dict[str, Any]:
        """
        Transform a single row using AI with enhanced prompt and error handling.
        
        Args:
            input_row: Dictionary representing a single row of data
            mapping_instructions: List of transformation rules
            
        Returns:
            Transformed row as dictionary
        """
        if not input_row:
            return {}

        prompt = self._build_transformation_prompt(input_row, mapping_instructions)
        
        try:
            response = self.model.invoke(prompt)
            content = response.content.strip()
            
            # Extract JSON from response
            transformed_row = self._extract_json_from_response(content)
            
            if not transformed_row:
                logger.warning(f"Failed to transform row: {input_row}")
                return {}
                
            return transformed_row
            
        except Exception as e:
            logger.error(f"Error transforming row: {e}")
            return {}

    def _build_transformation_prompt(self, input_row: Dict[str, Any], mapping_instructions: List[Dict]) -> str:
        """Build comprehensive transformation prompt for AI."""
        return f"""
You are a precise data transformation engine. Your task is to transform the given input row using the provided transformation rules.

TRANSFORMATION RULES:
{json.dumps(mapping_instructions, indent=2)}

RULE TYPES EXPLAINED:
- 'D' (Default): Replace with the specified default value
- 'O' (One-to-One): Copy source column value directly to target column
- 'T' (Transform): Use the mapping dictionary to transform values. Find the appropriate mapping by column context.
- 'A' (Auto-Generate): Generate values according to the specified pattern

CRITICAL INSTRUCTIONS:
1. Apply transformations exactly as specified
2. Use the target column names from the rules
3. For 'T' type rules, search through the mapping dictionary to find the appropriate transformation
4. If a value is not found in mapping or source is empty, use empty string ""
5. For 'A' type rules, follow the auto-generation pattern precisely
6. Only include transformed columns in the output
7. Ensure all target columns from the rules are present in the output

INPUT ROW DATA:
{json.dumps(input_row, indent=2)}

OUTPUT REQUIREMENTS:
- Return ONLY a valid JSON object
- Use exact target column names from the transformation rules
- Include all target columns specified in the rules
- Use empty string "" for missing or unmappable values

JSON OUTPUT:
"""

    def _extract_json_from_response(self, content: str) -> Dict[str, Any]:
        """Extract JSON object from AI response with multiple fallback strategies."""
        # Try to find JSON block
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', content, re.DOTALL)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except json.JSONDecodeError:
                pass
        
        # Try to find JSON object directly
        start_index = content.find('{')
        end_index = content.rfind('}') + 1
        
        if start_index != -1 and end_index != -1:
            try:
                json_str = content[start_index:end_index]
                return json.loads(json_str)
            except json.JSONDecodeError:
                pass
        
        logger.error(f"Could not extract valid JSON from AI response: {content}")
        return {}

    def transform_data(self, input_csv_path: str, mapping_excel_path: str, 
                      output_folder: str = "Output") -> str:
        """
        Main transformation method that processes the entire dataset.
        
        Args:
            input_csv_path: Path to input CSV file
            mapping_excel_path: Path to Excel file with transformation rules
            output_folder: Output directory for results
            
        Returns:
            Path to output file
        """
        try:
            # Load input data and transformation rules
            input_df = self.load_input_data(input_csv_path)
            mapping_instructions, transformation_dict = self.load_transformation_rules(mapping_excel_path)
            
            # Log transformation summary
            logger.info(f"Processing {len(input_df)} rows with {len(mapping_instructions)} transformation rules")
            
            # Transform each row
            result_rows = []
            for idx, (_, row) in enumerate(input_df.iterrows()):
                input_row = row.to_dict()
                transformed_row = self.transform_row_with_ai(input_row, mapping_instructions)
                
                if transformed_row:
                    result_rows.append(transformed_row)
                    if (idx + 1) % 10 == 0:  # Log progress every 10 rows
                        logger.info(f"Processed {idx + 1}/{len(input_df)} rows")
                else:
                    logger.warning(f"Failed to transform row {idx + 1}")
            
            # Save results
            output_path = self._save_results(result_rows, output_folder)
            
            logger.info(f"Transformation complete. Processed {len(result_rows)}/{len(input_df)} rows successfully")
            return output_path
            
        except Exception as e:
            logger.error(f"Error during transformation: {e}")
            raise

    def _save_results(self, result_rows: List[Dict], output_folder: str) -> str:
        """Save transformation results to CSV file."""
        if not result_rows:
            raise ValueError("No valid transformed rows to save")
        
        # Create output DataFrame
        output_df = pd.DataFrame(result_rows)
        
        # Ensure output folder exists
        os.makedirs(output_folder, exist_ok=True)
        
        # Save to CSV
        output_file = os.path.join(output_folder, "mapped_output_file.csv")
        output_df.to_csv(output_file, index=False)
        
        logger.info(f"Results saved to: {output_file}")
        logger.info(f"Output shape: {output_df.shape}")
        
        return output_file

    def validate_configuration(self) -> bool:
        """Validate that the transformation engine is properly configured."""
        try:
            # Test AI model connection
            test_response = self.model.invoke("Test connection")
            return bool(test_response.content)
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            return False


# Usage example and main execution
def main():
    # Azure OpenAI configuration
    azure_config = {
        "AZURE_OPENAI_API_KEY": "xxxxx",  # Replace with your actual key
        "AZURE_OPENAI_ENDPOINT": "xx",    # Replace with your actual endpoint
        "AZURE_OPENAI_API_VERSION": "xxx", # Replace with your actual version
        "AZURE_OPENAI_CHAT_DEPLOYMENT_NAME": "gpt-4o"  # Replace with your deployment name
    }
    
    # Initialize transformation engine
    engine = DataTransformationEngine(azure_config)
    
    # Validate configuration
    if not engine.validate_configuration():
        logger.error("Engine configuration validation failed. Please check your Azure OpenAI settings.")
        return
    
    # Execute transformation
    try:
        output_path = engine.transform_data(
            input_csv_path='CIFINPUT/NF_CLIENT_24042025.csv',
            mapping_excel_path="CIFINPUT/TRANS_ADRPART01.xlsx",
            output_folder="Output"
        )
        print(f"Transformation completed successfully. Output saved to: {output_path}")
        
    except Exception as e:
        logger.error(f"Transformation failed: {e}")


if __name__ == "__main__":
    main()