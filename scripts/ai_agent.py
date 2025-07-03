import google.generativeai as genai
import pandas as pd
from dotenv import load_dotenv
import os
from langgraph.graph import StateGraph, END
from pydantic import BaseModel
from typing import List, Dict, Any

# Load API key from environment
load_dotenv()
gemini_api_key = os.getenv("GEMINI_API_KEY")

if not gemini_api_key:
    raise ValueError("❌ GEMINI_API_KEY is missing. Set it in .env or as an environment variable.")

# Configure Gemini API
genai.configure(api_key=gemini_api_key)

# Define AI Model
model = genai.GenerativeModel('gemini-1.5-flash')

class CleaningState(BaseModel):
    """State schema defining input and output for the LangGraph agent."""
    input_text: str
    structured_response: str = ""
    batch_data: List[Dict[str, Any]] = []
    cleaned_responses: List[str] = []

class AIAgent:
    def __init__(self):
        self.graph = self.create_graph()
    
    def create_graph(self):
        """Creates and returns a LangGraph agent graph with state management."""
        graph = StateGraph(CleaningState)
        
        # ✅ FIX: Ensure agent outputs structured response
        def agent_logic(state: CleaningState) -> CleaningState:
            """Processes input and returns a structured response."""
            try:
                response = model.generate_content(state.input_text)
                cleaned_text = response.text if response.text else ""
                return CleaningState(
                    input_text=state.input_text, 
                    structured_response=cleaned_text
                )  # ✅ Ensuring structured response
            except Exception as e:
                print(f"❌ Error in agent logic: {e}")
                return CleaningState(
                    input_text=state.input_text,
                    structured_response="Error processing request"
                )
        
        graph.add_node("cleaning_agent", agent_logic)
        graph.add_edge("cleaning_agent", END)
        graph.set_entry_point("cleaning_agent")
        return graph.compile()
    
    def process_data(self, df, batch_size=20):
        """Processes data in batches to avoid Gemini's token limit."""
        cleaned_responses = []
        
        for i in range(0, len(df), batch_size):
            df_batch = df.iloc[i:i + batch_size]  # ✅ Process 20 rows at a time
            
            # Create batch prompt
            batch_text = ""
            for idx, row in df_batch.iterrows():
                batch_text += f"Row {idx}: {row.to_dict()}\n"
            
            prompt = f"""
You are an AI Data Cleaning Agent. Analyze the dataset:
{batch_text}

Please provide:
1. Data quality assessment
2. Missing value analysis
3. Outlier detection
4. Recommended cleaning steps
5. Data type corrections needed

Format your response as structured text with clear sections.
"""
            
            try:
                # Process batch through the graph
                initial_state = CleaningState(input_text=prompt)
                result = self.graph.invoke(initial_state)
                
                cleaned_responses.append({
                    'batch_number': i // batch_size + 1,
                    'rows_processed': len(df_batch),
                    'analysis': result['structured_response']
                })
                
                print(f"✅ Processed batch {i // batch_size + 1}/{(len(df) - 1) // batch_size + 1}")
                
            except Exception as e:
                print(f"❌ Error processing batch {i // batch_size + 1}: {e}")
                cleaned_responses.append({
                    'batch_number': i // batch_size + 1,
                    'rows_processed': len(df_batch),
                    'analysis': f"Error: {str(e)}"
                })
        
        return cleaned_responses
    
    def analyze_single_text(self, text: str) -> str:
        """Analyzes a single text input using the AI agent."""
        try:
            initial_state = CleaningState(input_text=text)
            result = self.graph.invoke(initial_state)
            return result['structured_response']
        except Exception as e:
            return f"Error analyzing text: {str(e)}"
    
    def clean_column(self, df, column_name: str) -> pd.DataFrame:
        """Cleans a specific column in the DataFrame using AI analysis."""
        if column_name not in df.columns:
            print(f"❌ Column '{column_name}' not found in DataFrame")
            return df
        
        # Get sample data for analysis
        sample_data = df[column_name].dropna().head(10).tolist()
        
        prompt = f"""
Analyze this column data and suggest cleaning operations:
Column: {column_name}
Sample values: {sample_data}

Provide specific cleaning recommendations:
1. Data type conversion needed
2. Pattern standardization
3. Invalid value handling
4. Missing value strategy
"""
        
        analysis = self.analyze_single_text(prompt)
        print(f"📊 Analysis for column '{column_name}':")
        print(analysis)
        
        return df
    
    def generate_data_report(self, df) -> str:
        """Generates a comprehensive data quality report."""
        # Basic statistics
        stats = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'missing_values': df.isnull().sum().sum(),
            'duplicate_rows': df.duplicated().sum(),
            'data_types': df.dtypes.to_dict()
        }
        
        prompt = f"""
Generate a comprehensive data quality report for this dataset:

Dataset Statistics:
- Total Rows: {stats['total_rows']}
- Total Columns: {stats['total_columns']}
- Missing Values: {stats['missing_values']}
- Duplicate Rows: {stats['duplicate_rows']}
- Column Data Types: {stats['data_types']}

Column Names: {list(df.columns)}

Sample Data (first 3 rows):
{df.head(3).to_dict()}

Provide:
1. Executive Summary
2. Data Quality Score (1-10)
3. Critical Issues Found
4. Recommended Actions
5. Priority Cleaning Steps
"""
        
        return self.analyze_single_text(prompt)

# Helper function to create agent instance
def create_ai_agent():
    """Creates and returns an AI agent instance."""
    return AIAgent()

# Example usage function
def example_usage():
    """Example of how to use the AI agent."""
    # Create agent
    agent = create_ai_agent()
    
    # Example DataFrame
    sample_data = {
        'name': ['John Doe', 'Jane Smith', None, 'Bob Johnson'],
        'age': [25, 30, 35, None],
        'email': ['john@email.com', 'jane@invalid', 'bob@email.com', 'test@email.com'],
        'salary': [50000, 60000, 70000, 80000]
    }
    
    df = pd.DataFrame(sample_data)
    
    # Generate report
    print("🔍 Generating Data Quality Report...")
    report = agent.generate_data_report(df)
    print(report)
    
    # Process data in batches
    print("\n🔄 Processing data in batches...")
    results = agent.process_data(df, batch_size=2)
    
    for result in results:
        print(f"\n📋 Batch {result['batch_number']} ({result['rows_processed']} rows):")
        print(result['analysis'])

if __name__ == "__main__":
    example_usage()