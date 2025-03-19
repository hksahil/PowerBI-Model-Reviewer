import streamlit as st
import json
import pandas as pd
import re
import os
import zipfile
import openai  # Correct import for OpenAI API
import time
from streamlit_react_flow import react_flow  # Import the react_flow component

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

def prepare_relationship_elements(relationships_data):
    """Prepare elements for the relationship flow diagram."""
    elements = []
    for idx, rel in enumerate(relationships_data):  # Use enumerate to get a unique index
        from_node = {
            "id": f"from-{idx}-{rel['FromTableName']}",  # Unique ID for from node
            "data": {"label": rel['FromTableName']},
            "position": {"x": 100, "y": 100},  # Adjust positions as needed
            "type": "input"
        }
        to_node = {
            "id": f"to-{idx}-{rel['ToTableName']}",  # Unique ID for to node
            "data": {"label": rel['ToTableName']},
            "position": {"x": 300, "y": 100},  # Adjust positions as needed
            "type": "output"
        }
        elements.append(from_node)
        elements.append(to_node)
        elements.append({
            "id": f"e-{idx}-{rel['FromTableName']}-{rel['ToTableName']}",  # Unique ID for edge
            "source": from_node["id"],
            "target": to_node["id"],
            "animated": True
        })
    return elements

def render_relationship_visualizer(elements):
    """Render the relationship flow diagram."""
    flow_styles = {"height": 500, "width": 1100}
    react_flow("relationship-visualizer", elements=elements, flow_styles=flow_styles)

def display_model_metadata(doc_info, relationships_data):
    """Display the model metadata and the relationship visualizer."""
    st.subheader("Model Metadata")
    
    # Display model information in a more fancy way
    for attribute, value in zip(doc_info['Attribute'], doc_info['Value']):
        col1, col2 = st.columns([1, 3])  # Create two columns
        with col1:
            st.markdown(f"**{attribute}:**")  # Bold attribute name
        with col2:
            st.write(value)  # Display the value

    # Alternatively, you can use an expander for more details
    with st.expander("View Detailed Model Information"):
        st.write(doc_info)

    # Relationship Visualizer
    st.subheader("Relationship Visualizer")
    
    # Prepare elements and render the flow diagram
    elements = prepare_relationship_elements(relationships_data)
    render_relationship_visualizer(elements)

def display_tables_metadata(merged_table_data):
    """Display the tables metadata."""
    tables_df = pd.DataFrame(merged_table_data).drop(columns=["Lineage Tag"], errors='ignore')
    display_data("Tables Metadata", tables_df, {"Mode": tables_df})

def display_columns_metadata(columns_data):
    """Display the columns metadata."""
    columns_df = pd.DataFrame(columns_data).drop(columns=["EncodingHint", "State", "isRowNumber"], errors='ignore')
    display_data("Columns Metadata", columns_df, {"TableName": columns_df, "DataType": columns_df, "DisplayFolder": columns_df})

def display_measures_metadata(measures_data):
    """Display the measures metadata."""
    measures_df = pd.DataFrame(measures_data)
    
    # Create filter options
    filter_options = {
        "MeasureName": measures_df['MeasureName'].unique(),
        "TableName": measures_df['TableName'].unique(),
        "DataType": measures_df['DataType'].unique()
    }

    # Create filter columns
    filter_cols = st.columns(len(filter_options))
    filtered_df = measures_df

    for idx, (key, options) in enumerate(filter_options.items()):
        selected_option = filter_cols[idx].selectbox(f"Filter by {key}", ["All"] + options.tolist())
        if selected_option != "All":
            filtered_df = filtered_df[filtered_df[key] == selected_option]

    # Check if 'MeasureExpression' column exists before adding the search bar
    if 'MeasureExpression' in measures_df.columns:
        # Add search bar for MeasureExpression
        search_expression = st.text_input("Search Measure Expression", "")
        if search_expression:
            # Escape the search expression to handle special characters
            escaped_expression = re.escape(search_expression)
            # Perform case-insensitive search
            filtered_df = filtered_df[filtered_df['MeasureExpression'].str.contains(escaped_expression, case=False, na=False)]
    else:
        st.warning("The 'MeasureExpression' column does not exist in the measures data.")

    st.dataframe(filtered_df)

def main():
    st.title("👨🏻‍💻 Power BI Assistant")
    st.write("Upload `.vpax` files to generate documentation.")

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
            tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Model Metadata", "Tables Metadata", "Columns Metadata", "Measures Metadata", "Table Expressions", "Ask GPT"])

            with tab1:
                display_model_metadata(doc_info, relationships_data)

            with tab2:
                display_tables_metadata(merged_table_data)

            with tab3:
                display_columns_metadata(columns_data)

            with tab4:
                display_measures_metadata(measures_data)

            with tab5:
                display_expressions(expressions_data)

            with tab6:
                ask_gpt(merged_table_data, columns_data, measures_data, expressions_data)  # Call the ask_gpt function

        except Exception as e:
            st.error(f"Error processing file: {e}")

if __name__ == "__main__":
    main()
