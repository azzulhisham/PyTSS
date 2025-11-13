import streamlit as st
import plotly.express as px
import streamlit.components.v1 as components

# Create a Plotly chart
fig = px.scatter(
    x=[1, 2, 3, 4],
    y=[10, 20, 25, 30],
    title="Floating Chart"
)

# Export Plotly chart to HTML string
plot_html = fig.to_html(include_plotlyjs='cdn', full_html=False)

# Inject floating div with chart
components.html(f"""
    <div style="
        position: fixed;
        top: 20px;
        right: 20px;
        width: 400px;
        height: 600px;
        background-color: white;
        border: 1px solid #ccc;
        border-radius: 10px;
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
        z-index: 9999;
        overflow: hidden;
    ">
        {plot_html}
    </div>
""", height=620)

# Regular Streamlit content
st.title("Main App Content")
st.write("Scroll around — the chart floats in the corner.")