import streamlit as st
import json
import pandas as pd

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
    
    # Add custom relationship columns
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

            # Merging metadata
            merged_table_data = merge_metadata(table_data, dax_table_data)

            st.success("Files processed successfully!")

            # Display Model Metadata
            df = pd.DataFrame(doc_info)
            st.subheader("Model Metadata")
            st.table(df)

            # Display Tables Metadata
            table_df = pd.DataFrame(merged_table_data)
            st.subheader("Tables Metadata")
            st.dataframe(table_df)

            # Display Columns Metadata
            columns_df = pd.DataFrame(columns_data)
            st.subheader("Columns Metadata")
            st.dataframe(columns_df)

            # Display Measures Metadata
            measures_df = pd.DataFrame(measures_data)
            st.subheader("Measures Metadata")
            st.dataframe(measures_df)

            # Display Relationships Metadata
            relationships_df = pd.DataFrame(relationships_data)
            st.subheader("Relationships Metadata")
            st.dataframe(relationships_df)

    except Exception as e:
        st.error(f"Error processing file: {e}")
