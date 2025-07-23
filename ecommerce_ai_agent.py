import sqlite3
import pandas as pd
from flask import Flask, request, jsonify, Response
import json
import time
from transformers import AutoModelForCausalLM, AutoTokenizer
import matplotlib.pyplot as plt
import io
import base64
from threading import Thread

# Initialize Flask app
app = Flask(__name__)

# Initialize SQLite database
def init_db():
    conn = sqlite3.connect('ecommerce.db')
    c = conn.cursor()

    # Create tables
    c.execute('''
        CREATE TABLE IF NOT EXISTS product_eligibility (
            product_id TEXT PRIMARY KEY,
            is_eligible BOOLEAN,
            category TEXT,
            min_price REAL
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS ad_sales_metrics (
            product_id TEXT,
            ad_spend REAL,
            ad_impressions INTEGER,
            ad_clicks INTEGER,
            ad_conversions INTEGER,
            cpc REAL,
            FOREIGN KEY (product_id) REFERENCES product_eligibility(product_id)
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS total_sales_metrics (
            product_id TEXT,
            total_sales REAL,
            units_sold INTEGER,
            avg_price REAL,
            FOREIGN KEY (product_id) REFERENCES product_eligibility(product_id)
        )
    ''')

    # Sample data insertion
    c.executemany('''
        INSERT OR REPLACE INTO product_eligibility VALUES (?, ?, ?, ?)
    ''', [
        ('P001', 1, 'Electronics', 99.99),
        ('P002', 1, 'Clothing', 29.99),
        ('P003', 0, 'Books', 15.99)
    ])

    c.executemany('''
        INSERT OR REPLACE INTO ad_sales_metrics VALUES (?, ?, ?, ?, ?, ?)
    ''', [
        ('P001', 1000.0, 10000, 500, 50, 2.0),
        ('P002', 500.0, 8000, 400, 40, 1.25),
        ('P003', 200.0, 3000, 150, 15, 1.33)
    ])

    c.executemany('''
        INSERT OR REPLACE INTO total_sales_metrics VALUES (?, ?, ?, ?)
    ''', [
        ('P001', 5000.0, 100, 50.0),
        ('P002', 3000.0, 120, 25.0),
        ('P003', 1000.0, 60, 16.67)
    ])

    conn.commit()
    conn.close()

# Initialize LLM (Using a small model for local execution)
model_name = "distilgpt2"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForCausalLM.from_pretrained(model_name)

def generate_sql_query(question):
    # Simplified natural language to SQL conversion
    question = question.lower()
    prompt = f"Convert this question to a SQL query: {question}\nAvailable tables: product_eligibility, ad_sales_metrics, total_sales_metrics"

    inputs = tokenizer(prompt, return_tensors="pt", max_length=512, truncation=True)
    outputs = model.generate(**inputs, max_length=200)
    sql_query = tokenizer.decode(outputs[0], skip_special_tokens=True)

    # Basic query mapping for demo purposes
    if "total sales" in question:
        return "SELECT SUM(total_sales) as total FROM total_sales_metrics"
    elif "roas" in question:
        return """
        SELECT a.product_id, (t.total_sales / a.ad_spend) as roas
        FROM ad_sales_metrics a
        JOIN total_sales_metrics t ON a.product_id = t.product_id
        """
    elif "highest cpc" in question:
        return """
        SELECT product_id, cpc
        FROM ad_sales_metrics
        WHERE cpc = (SELECT MAX(cpc) FROM ad_sales_metrics)
        """
    return sql_query

def execute_query(query):
    conn = sqlite3.connect('ecommerce.db')
    c = conn.cursor()
    c.execute(query)
    results = c.fetchall()
    columns = [desc[0] for desc in c.description]
    conn.close()
    return results, columns

def generate_visualization(results, columns, query_type):
    df = pd.DataFrame(results, columns=columns)
    img = io.BytesIO()
    
    if query_type == "roas":
        plt.figure(figsize=(8, 6))
        plt.bar(df['product_id'], df['roas'])
        plt.title('Return on Ad Spend by Product')
        plt.xlabel('Product ID')
        plt.ylabel('ROAS')
        plt.savefig(img, format='png')
        plt.close()
    elif query_type == "highest cpc":
        plt.figure(figsize=(8, 6))
        plt.bar(df['product_id'], df['cpc'])
        plt.title('Highest CPC by Product')
        plt.xlabel('Product ID')
        plt.ylabel('CPC')
        plt.savefig(img, format='png')
        plt.close()
    
    img.seek(0)
    return base64.b64encode(img.getvalue()).decode()

# Stream response generator
def stream_response(answer, visualization=None):
    for char in answer:
        yield f"data: {json.dumps({'text': char})}\n\n"
        time.sleep(0.05)
    if visualization:
        yield f"data: {json.dumps({'visualization': visualization})}\n\n"

@app.route('/ask', methods=['POST'])
def ask_question():
    data = request.get_json()
    question = data.get('question', '')
    
    # Generate and execute SQL query
    sql_query = generate_sql_query(question)
    results, columns = execute_query(sql_query)
    
    # Format human-readable response
    query_type = "roas" if "roas" in question.lower() else "highest cpc" if "highest cpc" in question.lower() else ""
    answer = f"Query: {question}\nResults:\n"
    for row in results:
        answer += ", ".join([f"{col}: {val}" for col, val in zip(columns, row)]) + "\n"
    
    # Generate visualization if applicable
    visualization = None
    if query_type:
        visualization = generate_visualization(results, columns, query_type)
    
    # Return streamed response
    return Response(stream_response(answer, visualization), mimetype='text/event-stream')

def run_app():
    init_db()
    app.run(debug=True)

if __name__ == '__main__':
    Thread(target=run_app).start()
