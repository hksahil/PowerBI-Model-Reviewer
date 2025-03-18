import streamlit as st
import json
import pandas as pd
import re
import networkx as nx  # New import for visualizing relationships
import matplotlib.pyplot as plt  # New import for plotting
import plotly.graph_objects as go
import dash
import dash_cytoscape as cyto
from dash import html
import holoviews as hv
from holoviews import opts
from bokeh.io import show
from bokeh.models import GraphRenderer, StaticLayoutProvider, Circle, LabelSet, ColumnDataSource
from bokeh.plotting import figure
from bokeh.layouts import column
from pyvis.network import Network
import os

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

# Streamlit UI
st.title("Power BI Model Documentation")
st.write("Upload `model.bim` and `DaxVpaView.json` files to generate documentation.")

uploaded_bim = st.file_uploader("Upload model.bim", type=["bim"])
uploaded_dax = st.file_uploader("Upload DaxVpaView.json", type=["json"])

if uploaded_bim and uploaded_dax:
    try:
        doc_info, table_data, expressions_data = parse_model_bim(uploaded_bim)
        dax_table_data, columns_data, measures_data, relationships_data = parse_dax_vpa_view(uploaded_dax)
        merged_table_data = merge_metadata(table_data, dax_table_data)

        st.success("Files processed successfully!")

        tab1, tab2, tab3, tab4, tab5 = st.tabs(["Model Metadata", "Tables Metadata", "Columns Metadata", "Measures Metadata", "Table Expressions"])

        with tab1: st.dataframe(pd.DataFrame(doc_info))
        with tab2: display_data(tab2, merged_table_data, {"Mode": merged_table_data})
        with tab3: display_data(tab3, columns_data, {"TableName": columns_data, "DataType": columns_data, "DisplayFolder": columns_data})
        with tab4: display_data(tab4, measures_data, {"TableName": measures_data, "MeasureName": measures_data}, expression_filter="MeasureExpression")
        with tab5: display_expressions(expressions_data)

    except Exception as e:
        st.error(f"Error processing file: {e}")
