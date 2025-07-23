from langchain_community.llms import Ollama
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from database import setup_database
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
import re


class EcommerceAgent:
    def __init__(self):
        self.engine = setup_database()
        self.llm = Ollama(model="llama3")  # or "mistral", "gemma"

        # Enhanced SQL generation prompt
        self.sql_prompt = PromptTemplate(
            input_variables=["question"],
            template="""
            You are a SQL expert working with e-commerce data. Generate ONLY a SELECT query.
            Database schema:
            - sales_metrics(date TEXT, item_id INTEGER, total_sales REAL, total_units_ordered INTEGER)
            - ad_metrics(date TEXT, item_id INTEGER, ad_sales REAL, impressions INTEGER, 
                         ad_spend REAL, clicks INTEGER, units_sold INTEGER)
            - product_eligibility(eligibility_datetime_utc TEXT, item_id INTEGER, 
                                eligibility BOOLEAN, message TEXT)

            Rules:
            1. Always use explicit column names (no *)
            2. Only query relevant tables
            3. Include WHERE clauses when appropriate
            4. Never modify data (only SELECT)

            Convert this to SQL: {question}
            Return ONLY the SQL query, nothing else.
            """
        )

        # Enhanced answer generation prompt
        self.answer_prompt = PromptTemplate(
            input_variables=["question", "data", "sql"],
            template="""
            As a data analyst, answer this question based on the query results:

            Question: {question}
            SQL Used: {sql}
            Data: {data}

            Provide:
            1. A clear answer with key numbers
            2. Business interpretation
            3. Any data limitations
            4. Suggested follow-up questions

            If no results, explain why and suggest alternatives.
            """
        )

    def _sanitize_sql(self, sql):
        """Clean and validate the generated SQL"""
        # Remove any markdown code blocks
        sql = re.sub(r'```sql|```', '', sql, flags=re.IGNORECASE).strip()

        # Remove trailing semicolon if present
        sql = sql.rstrip(';')

        # Ensure it's a SELECT query
        if not sql.lower().lstrip().startswith('select'):
            raise ValueError(f"Generated query is not a SELECT statement: {sql}")

        # Basic SQL injection protection (modified to allow simple queries)
        forbidden = ['insert ', 'update ', 'delete ', 'drop ', 'alter ', 'truncate ', 'create ']
        if any(cmd in sql.lower() for cmd in forbidden):
            raise ValueError(f"Query contains forbidden operations: {sql}")

        return sql

    def generate_sql(self, question):
        chain = LLMChain(llm=self.llm, prompt=self.sql_prompt)
        raw_sql = chain.run(question=question).strip()
        return self._sanitize_sql(raw_sql)

    def execute_query(self, sql):
        """Execute query with error handling"""
        try:
            with self.engine.connect() as conn:
                return pd.read_sql(sql, conn)
        except Exception as e:
            raise ValueError(f"SQL execution failed: {str(e)}\nQuery: {sql}")

    def generate_answer(self, question, data, sql):
        chain = LLMChain(llm=self.llm, prompt=self.answer_prompt)
        return chain.run(
            question=question,
            data=data.to_json(orient='records'),
            sql=sql
        )

    def generate_visualization(self, question, data):
        """Generate appropriate visualization based on query results"""
        if data.empty:
            return None

        plt.figure(figsize=(10, 5))
        viz = None

        try:
            # Time series data
            if 'date' in data.columns:
                data['date'] = pd.to_datetime(data['date'])
                data = data.sort_values('date')

                if 'total_sales' in data.columns:
                    data.plot(x='date', y='total_sales', kind='line', title='Sales Trend')
                elif 'ad_sales' in data.columns:
                    data.plot(x='date', y='ad_sales', kind='line', title='Ad Performance')

            # Metric comparisons
            elif all(col in data.columns for col in ['item_id', 'ad_sales', 'ad_spend']):
                data['ROAS'] = data['ad_sales'] / data['ad_spend']
                data.plot.bar(x='item_id', y='ROAS', title='Return on Ad Spend')

            # Save visualization
            buf = io.BytesIO()
            plt.savefig(buf, format='png', bbox_inches='tight')
            viz = base64.b64encode(buf.getvalue()).decode('utf-8')

        except Exception as e:
            print(f"Visualization error: {str(e)}")
        finally:
            plt.close()

        return viz

    def query(self, question):
        """Main query handler with comprehensive error handling"""
        try:
            # Step 1: Generate and validate SQL
            sql = self.generate_sql(question)
            print(f"Generated SQL: {sql}")  # Debug logging

            # Step 2: Execute query
            data = self.execute_query(sql)

            # Step 3: Generate answer
            answer = self.generate_answer(question, data, sql)

            # Step 4: Generate visualization
            visualization = self.generate_visualization(question, data)

            return {
                "success": True,
                "answer": answer,
                "data": data.to_dict(orient='records'),
                "visualization": visualization,
                "sql": sql
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "suggestion": self._get_suggestion(question, str(e)),
                "sql": sql if 'sql' in locals() else None
            }

    def _get_suggestion(self, question, error):
        """Provide context-aware suggestions"""
        if "no such table" in error.lower():
            return "Try rephrasing to use sales_metrics, ad_metrics, or product_eligibility tables"
        elif "no such column" in error.lower():
            return "Available columns: date, item_id, total_sales, ad_sales, ad_spend, etc."
        elif "select" in error.lower():
            return "Ask about data queries like 'Show me sales for product X' or 'Compare ad performance'"
        return "Try being more specific about what data you need"