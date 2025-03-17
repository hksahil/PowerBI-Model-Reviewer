import streamlit as st
import json
import pandas as pd
import re

def parse_model_bim(file):
    data = json.load(file)
    
    model_name = data.get("name", "Unknown")
    date_modified = data.get("lastUpdate", "Unknown")
    total_size = data.get("model", {}).get("estimatedSize", "Not Available")
    storage_format = data.get("model", {}).get("defaultPowerBIDataSourceVersion", "Unknown")
    
    tables = data.get("model", {}).get("tables", [])
    num_tables = len(tables)
    total_columns = sum(len(t.get("columns", [])) for t in tables)
    total_measures = sum(len(t.get("measures", [])) for t in tables)
    
    max_row_count = 0
    num_partitions = 0
    total_table_size = sum(t.get("estimatedSize", 0) for t in tables if isinstance(t.get("estimatedSize"), int))
    table_metadata = []
    
    for table in tables:
        partitions = table.get("partitions", [])
        num_partitions += len(partitions)
        table_size = table.get("estimatedSize", 0)
        percentage_of_total = (table_size / total_table_size * 100) if total_table_size > 0 else 0
        is_hidden = table.get("isHidden", False)
        lineage_tag = table.get("lineageTag", "Unknown")
        
        mode = "Unknown"
        expression = ""
        table_row_count = 0
        latest_modified_time = "Unknown"
        latest_refreshed_time = "Unknown"
        
        for partition in partitions:
            row_count = partition.get("rows", 0)
            table_row_count += row_count
            max_row_count = max(max_row_count, row_count)
            mode = partition.get("mode", mode)
            expression = partition.get("source", {}).get("expression", "")
            
            partition_modified_time = partition.get("modifiedTime", "Unknown")
            partition_refreshed_time = partition.get("refreshedTime", "Unknown")
            
            latest_modified_time = max(latest_modified_time, partition_modified_time)
            latest_refreshed_time = max(latest_refreshed_time, partition_refreshed_time)
        
        table_metadata.append({
            "Table Name": table.get("name", "Unknown"),
            "Mode": mode,
            "Partitions": len(partitions),
            "Rows": table_row_count,
            "Table Size": table_size,
            "% of Total Size": round(percentage_of_total, 2),
            "Expression": expression,
            "Is Hidden": is_hidden,
            "Latest Partition Modified": latest_modified_time,
            "Latest Partition Refreshed": latest_refreshed_time,
            "Lineage Tag": lineage_tag
        })
    
    return {
        "Attribute": ["Model Name", "Date Modified", "Total Size of Model", "Storage Format", "Number of Tables", "Number of Partitions", "Max Row Count of Biggest Table", "Total Columns", "Total Measures"],
        "Value": [model_name, date_modified, total_size, storage_format, num_tables, num_partitions, max_row_count, total_columns, total_measures]
    }, table_metadata

def parse_dax_vpa_view(file):
    data = json.load(file)
    dax_table_data = {table["TableName"]: table for table in data.get("Tables", [])}
    columns_data = data.get("Columns", [])
    measures_data = data.get("Measures", [])
    relationships_data = data.get("Relationships", [])
    
    for rel in relationships_data:
        rel["from"] = f"{rel['FromTableName']}.{rel['FromFullColumnName']}"
        rel["to"] = f"{rel['ToTableName']}.{rel['ToFullColumnName']}"
        rel["cardinality"] = f"{rel['FromCardinalityType']}-{rel['ToCardinalityType']}-{rel['CrossFilteringBehavior']}"

    return dax_table_data, columns_data, measures_data, relationships_data

def merge_metadata(model_data, dax_table_data):
    for table in model_data:
        dax_info = dax_table_data.get(table["Table Name"], {})
        table["Columns Size"] = dax_info.get("ColumnsSize", "N/A")
        table["DAX Table Size"] = dax_info.get("TableSize", "N/A")
    return model_data

# Streamlit UI
st.title("Power BI Model Documentation")
st.write("Upload `model.bim` and `DaxVpaView.json` files to generate documentation.")

uploaded_bim = st.file_uploader("Upload model.bim", type=["bim"])
uploaded_dax = st.file_uploader("Upload DaxVpaView.json", type=["json"])

if uploaded_bim and uploaded_dax:
    try:
        with uploaded_bim as bim_file, uploaded_dax as dax_file:
            doc_info, table_data = parse_model_bim(bim_file)
            dax_table_data, columns_data, measures_data, relationships_data = parse_dax_vpa_view(dax_file)

            merged_table_data = merge_metadata(table_data, dax_table_data)

            st.success("Files processed successfully!")

            tab1, tab2, tab3, tab4, tab5 = st.tabs(["Model Metadata", "Tables Metadata", "Columns Metadata", "Measures Metadata", "Relationships Metadata"])

            with tab1:
                df = pd.DataFrame(doc_info)
                st.dataframe(df)

            with tab2:
                table_df = pd.DataFrame(merged_table_data)
                selected_mode = st.selectbox("Filter by Mode", ["All"] + table_df['Mode'].unique().tolist())
                if selected_mode != "All":
                    table_df = table_df[table_df['Mode'] == selected_mode]
                st.dataframe(table_df)

            with tab3:
                columns_df = pd.DataFrame(columns_data)
                col1, col2, col3 = st.columns(3)
                with col1:
                    selected_column_table = st.selectbox("Filter by Table Name", ["All"] + columns_df['TableName'].unique().tolist())
                with col2:
                    selected_datatype = st.selectbox("Filter by DataType", ["All"] + columns_df['DataType'].unique().tolist())
                with col3:
                    selected_displayfolder = st.selectbox("Filter by Display Folder", ["All"] + columns_df['DisplayFolder'].unique().tolist())

                if selected_column_table != "All":
                    columns_df = columns_df[columns_df['TableName'] == selected_column_table]
                if selected_datatype != "All":
                    columns_df = columns_df[columns_df['DataType'] == selected_datatype]
                if selected_displayfolder != "All":
                    columns_df = columns_df[columns_df['DisplayFolder'] == selected_displayfolder]
                st.dataframe(columns_df)

            with tab4:
                measures_df = pd.DataFrame(measures_data)
                col1, col2, col3 = st.columns(3)
                with col1:
                    selected_measure_table = st.selectbox("Filter by Table Name", ["All"] + measures_df['TableName'].unique().tolist())
                with col2:
                    selected_measure_name = st.selectbox("Filter by Measure Name", ["All"] + measures_df['MeasureName'].unique().tolist())
                with col3:
                    search_expression = st.text_input("Search Expression").strip()

                if selected_measure_table != "All":
                    measures_df = measures_df[measures_df['TableName'] == selected_measure_table]
                if selected_measure_name != "All":
                    measures_df = measures_df[measures_df['MeasureName'] == selected_measure_name]
                if search_expression:
                    escaped_expression = re.escape(search_expression)
                    measures_df = measures_df[measures_df['MeasureExpression'].str.contains(escaped_expression, case=False, na=False)]

                st.dataframe(measures_df)

            with tab5:
                relationships_df = pd.DataFrame(relationships_data)
                col1, col2 = st.columns(2)
                with col1:
                    all_tables = sorted(set(relationships_df['FromTableName'].tolist() + relationships_df['ToTableName'].tolist()))
                    selected_relationship_table = st.selectbox("Filter by Table Name", ["All"] + all_tables)
                with col2:
                    selected_cardinality = st.selectbox("Filter by Cardinality", ["All"] + relationships_df['cardinality'].unique().tolist())

                if selected_relationship_table != "All":
                    relationships_df = relationships_df[(relationships_df['FromTableName'] == selected_relationship_table) | (relationships_df['ToTableName'] == selected_relationship_table)]
                if selected_cardinality != "All":
                    relationships_df = relationships_df[relationships_df['cardinality'] == selected_cardinality]

                st.dataframe(relationships_df)

    except Exception as e:
        st.error(f"Error processing file: {e}")
