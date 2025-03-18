import streamlit as st
import json
import pandas as pd
import re
import os
import zipfile
import openai  # Correct import for OpenAI API
import time

st.set_page_config(layout="wide")

def load_json(file):
    return json.load(file)

def calculate_metadata(tables):
    num_partitions, max_row_count, total_table_size = 0, 0, sum(t.get("estimatedSize", 0) for t in tables)
    table_metadata = []
    expressions_data = []

    for table in tables:
        partitions = table.get("partitions", [])
        num_partitions += len(partitions)
        table_row_count = sum(p.get("rows", 0) for p in partitions)
        max_row_count = max(max_row_count, table_row_count)

        expression = partitions[0].get("source", {}).get("expression", "") if partitions else ""

        table_metadata.append({
            "Table Name": table.get("name", "Unknown"),
            "Mode": partitions[0].get("mode", "Unknown") if partitions else "Unknown",
            "Partitions": len(partitions),
            "Rows": table_row_count,
            "Table Size": table.get("estimatedSize", 0),
            "% of Total Size": round(table.get("estimatedSize", 0) / total_table_size * 100, 2) if total_table_size > 0 else 0,
            "Is Hidden": table.get("isHidden", False),
            "Latest Partition Modified": max(p.get("modifiedTime", "Unknown") for p in partitions) if partitions else "Unknown",
            "Latest Partition Refreshed": max(p.get("refreshedTime", "Unknown") for p in partitions) if partitions else "Unknown",
            "Lineage Tag": table.get("lineageTag", "Unknown")
        })

        # Store expressions data separately for the new tab
        expressions_data.append({
            "Table Name": table.get("name", "Unknown"),
            "Expression": expression
        })

    return num_partitions, max_row_count, total_table_size, table_metadata, expressions_data

def parse_model_bim(file):
    data = load_json(file)
    tables = data.get("model", {}).get("tables", [])
    num_partitions, max_row_count, total_table_size, table_metadata, expressions_data = calculate_metadata(tables)

    doc_info = {
        "Attribute": ["Model Name", "Date Modified", "Total Size of Model", "Storage Format", "Number of Tables", "Number of Partitions", "Max Row Count of Biggest Table", "Total Columns", "Total Measures"],
        "Value": [
            data.get("name", "Unknown"),
            data.get("lastUpdate", "Unknown"),
            data.get("model", {}).get("estimatedSize", "Not Available"),
            data.get("model", {}).get("defaultPowerBIDataSourceVersion", "Unknown"),
            len(tables),
            num_partitions,
            max_row_count,
            sum(len(t.get("columns", [])) for t in tables),
            sum(len(t.get("measures", [])) for t in tables)
        ]
    }
    return doc_info, table_metadata, expressions_data

def parse_dax_vpa_view(file):
    data = load_json(file)
    relationships = data.get("Relationships", [])
    for rel in relationships:
        rel["cardinality"] = rel.get("cardinality", "Unknown")

    return ({table["TableName"]: table for table in data.get("Tables", [])},
            data.get("Columns", []),
            data.get("Measures", []),
            relationships)

def merge_metadata(model_data, dax_table_data):
    for table in model_data:
        dax_info = dax_table_data.get(table["Table Name"], {})
        table.update({"Columns Size": dax_info.get("ColumnsSize", "N/A"), "DAX Table Size": dax_info.get("TableSize", "N/A")})
    return model_data

def display_data(tab, data, filter_options, expression_filter=None):
    df = pd.DataFrame(data)
    with st.container():
        filter_cols = st.columns(len(filter_options))
        for idx, (key, options) in enumerate(filter_options.items()):
            selected_option = filter_cols[idx].selectbox(f"Filter by {key}", ["All"] + df[key].unique().tolist())
            if selected_option != "All":
                df = df[df[key] == selected_option]

        # Add expression filter if provided
        if expression_filter:
            search_expression = st.text_input("Search Expressions", "")
            if search_expression:
                # Escape the search expression to handle special characters
                escaped_expression = re.escape(search_expression)
                # Perform case-insensitive search and allow special characters
                df = df[df[expression_filter].str.contains(escaped_expression, case=False, na=False)]

    st.dataframe(df)

def display_expressions(expressions_data):
    # Create a dropdown for table selection
    tables = [expr["Table Name"] for expr in expressions_data]
    selected_table = st.selectbox("Select Table", ["All"] + tables)

    if selected_table == "All":
        # Display all expressions
        for expr in expressions_data:
            if expr["Expression"]:  # Only show if there's an expression
                st.subheader(expr["Table Name"])
                st.code(expr["Expression"], language="m")
                st.divider()
    else:
        # Display expression for selected table
        for expr in expressions_data:
            if expr["Table Name"] == selected_table and expr["Expression"]:
                st.subheader(expr["Table Name"])
                st.code(expr["Expression"], language="m")

def ask_gpt(merged_table_data, columns_data, measures_data, expressions_data):
    """Function to handle user questions and interact with OpenAI API."""
    st.header("Ask GPT")
    user_question = st.text_input("Enter your question:")
    api_key = st.text_input("Enter your OpenAI API Key:", type="password")  # Password input for security
    if st.button("Get Answer"):
        if api_key and user_question:
            openai.api_key = api_key
            # Prepare the context for the question
            context = {
                "merged_table_data": merged_table_data,
                "columns_data": columns_data,
                "measures_data": measures_data,
                "expressions_data": expressions_data
            }
            # Generate a prompt for GPT
            prompt = f"Based on the following data:\n{context}\n\nUser question: {user_question}\n\nAnswer:"
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",  # Specify the model
                messages=[{"role": "user", "content": prompt}]
            )
            answer = response['choices'][0]['message']['content']
            st.write(answer)
        else:
            st.error("Please enter both your question and API key.")

# Streamlit UI
def main():
    st.title("Power BI Model Documentation")
    st.write("Upload `model.bim` and `DaxVpaView.json` files to generate documentation.")

    uploaded_vpax = st.file_uploader("Upload vpax file", type=["vpax"])

    if uploaded_vpax:
        try:
            # Extract files from the vpax zip file
            with zipfile.ZipFile(uploaded_vpax, 'r') as zip_ref:
                zip_ref.extractall("extracted_files")  # Extract to a temporary directory
                # Load the specific files
                with open("extracted_files/model.bim", encoding='utf-8-sig') as bim_file:
                    doc_info, table_data, expressions_data = parse_model_bim(bim_file)
                with open("extracted_files/DaxVpaView.json", encoding='utf-8-sig') as dax_file:
                    dax_table_data, columns_data, measures_data, relationships_data = parse_dax_vpa_view(dax_file)

            merged_table_data = merge_metadata(table_data, dax_table_data)

            # Temporary success message
            success_message = st.empty()  # Create an empty container for the message
            success_message.success("Files processed successfully!")
            time.sleep(3)  # Wait for 3 seconds
            success_message.empty()  # Clear the message

            # Streamlit tabs
            tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["Model Metadata", "Tables Metadata", "Columns Metadata", "Measures Metadata", "Table Expressions", "Ask GPT", "Relationships"])

            with tab1: st.dataframe(pd.DataFrame(doc_info))
            with tab2: 
                # Remove the "Lineage Tag" column from Tables Metadata
                tables_df = pd.DataFrame(merged_table_data).drop(columns=["Lineage Tag"], errors='ignore')
                display_data(tab2, tables_df, {"Mode": tables_df})
            with tab3: 
                # Remove the specified columns from Columns Metadata
                columns_df = pd.DataFrame(columns_data).drop(columns=["EncodingHint", "State", "isRowNumber"], errors='ignore')
                display_data(tab3, columns_df, {"TableName": columns_df, "DataType": columns_df, "DisplayFolder": columns_df})
            with tab4: 
                measures_df = pd.DataFrame(measures_data)
                st.subheader("Measures Metadata")
                st.dataframe(measures_df)
            with tab5: 
                display_expressions(expressions_data)
            with tab6: 
                ask_gpt(merged_table_data, columns_data, measures_data, expressions_data)  # Call the ask_gpt function
            with tab7:  # Relationships tab
                relationships_df = pd.DataFrame(relationships_data).drop(columns=["RelationshipName", "cardinality"], errors='ignore')
                st.subheader("Relationships Metadata")
                col1, col2 = st.columns(2)
                with col1:
                    selected_relationship_table = st.selectbox("Filter by Table Name", ["All"] + relationships_df['FromTableName'].unique().tolist())
                with col2:
                    # Removed cardinality filter since it's no longer in the DataFrame
                    selected_cardinality = st.selectbox("Filter by Cardinality", ["All"])  # Only "All" option now
                
                if selected_relationship_table != "All":
                    relationships_df = relationships_df[(relationships_df['FromTableName'] == selected_relationship_table) | (relationships_df['ToTableName'] == selected_relationship_table)]

                st.dataframe(relationships_df)

        except Exception as e:
            st.error(f"Error processing file: {e}")

if __name__ == "__main__":
    main()
