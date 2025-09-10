"""
DATA ANALYTICS INTERNSHIP - TASK 6: SQLite Sales Analysis
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
import os

print("="*70)
print("DATA ANALYTICS INTERNSHIP - TASK 6")
print("BASIC SALES SUMMARY FROM SQLite DATABASE USING PYTHON")
print("="*70)

# =============================================================================
# STEP 1: CREATE SQLITE DATABASE WITH SAMPLE SALES DATA
# =============================================================================

def create_sales_database():
    """Create a SQLite database with sample sales data"""
    print("\n🗃️ STEP 1: Creating SQLite Database 'sales_data.db'")
    print("-" * 50)
    
    # Connect to SQLite database (creates file if doesn't exist)
    conn = sqlite3.connect('sales_data.db')
    cursor = conn.cursor()
    
    # Create sales table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product TEXT NOT NULL,
            category TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            sale_date DATE NOT NULL,
            customer_id INTEGER,
            sales_rep TEXT
        )
    ''')
    
    # Insert sample sales data
    sample_data = [
        ('Laptop', 'Electronics', 5, 999.99, '2024-01-15', 101, 'John Smith'),
        ('Mouse', 'Electronics', 25, 29.99, '2024-01-15', 102, 'John Smith'),
        ('Keyboard', 'Electronics', 15, 79.99, '2024-01-16', 103, 'Sarah Johnson'),
        ('Monitor', 'Electronics', 8, 299.99, '2024-01-16', 104, 'John Smith'),
        ('Desk Chair', 'Furniture', 12, 199.99, '2024-01-17', 105, 'Mike Wilson'),
        ('Office Desk', 'Furniture', 6, 449.99, '2024-01-17', 106, 'Sarah Johnson'),
        ('Bookshelf', 'Furniture', 4, 129.99, '2024-01-18', 107, 'Mike Wilson'),
        ('Printer', 'Electronics', 3, 189.99, '2024-01-18', 108, 'John Smith'),
        ('Paper', 'Office Supplies', 50, 12.99, '2024-01-19', 109, 'Sarah Johnson'),
        ('Pens', 'Office Supplies', 100, 2.99, '2024-01-19', 110, 'Mike Wilson'),
        ('Notebook', 'Office Supplies', 75, 5.99, '2024-01-20', 111, 'John Smith'),
        ('Smartphone', 'Electronics', 7, 699.99, '2024-01-20', 112, 'Sarah Johnson'),
        ('Tablet', 'Electronics', 4, 399.99, '2024-01-21', 113, 'Mike Wilson'),
        ('Headphones', 'Electronics', 20, 89.99, '2024-01-21', 114, 'John Smith'),
        ('Coffee Table', 'Furniture', 3, 299.99, '2024-01-22', 115, 'Sarah Johnson'),
        ('Lamp', 'Furniture', 8, 79.99, '2024-01-22', 116, 'Mike Wilson'),
        ('Calculator', 'Office Supplies', 30, 19.99, '2024-01-23', 117, 'John Smith'),
        ('Stapler', 'Office Supplies', 25, 24.99, '2024-01-23', 118, 'Sarah Johnson'),
        ('Webcam', 'Electronics', 10, 129.99, '2024-01-24', 119, 'Mike Wilson'),
        ('Speaker', 'Electronics', 15, 149.99, '2024-01-24', 120, 'John Smith')
    ]
    
    # Insert data into the table
    cursor.executemany('''
        INSERT INTO sales (product, category, quantity, price, sale_date, customer_id, sales_rep)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', sample_data)
    
    # Commit changes and close connection
    conn.commit()
    conn.close()
    
    print("✅ Database 'sales_data.db' created successfully!")
    print(f"✅ Inserted {len(sample_data)} sample sales records")
    return True

# =============================================================================
# STEP 2: CONNECT TO DATABASE AND EXPLORE DATA
# =============================================================================

def connect_to_database():
    """Connect to the SQLite database and explore basic structure"""
    print("\n🔌 STEP 2: Connecting to SQLite Database")
    print("-" * 50)
    
    try:
        # Connect to SQLite database
        conn = sqlite3.connect('sales_data.db')
        print("✅ Successfully connected to 'sales_data.db'")
        
        # Check if database file exists and show size
        if os.path.exists('sales_data.db'):
            file_size = os.path.getsize('sales_data.db')
            print(f"📊 Database file size: {file_size:,} bytes")
        
        # Get table information
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"📋 Tables in database: {[table[0] for table in tables]}")
        
        # Get column information for sales table
        cursor.execute("PRAGMA table_info(sales);")
        columns = cursor.fetchall()
        print("\n📊 Sales table structure:")
        for column in columns:
            print(f"   {column[1]} ({column[2]})")
        
        return conn
        
    except sqlite3.Error as e:
        print(f"❌ Error connecting to database: {e}")
        return None

# =============================================================================
# STEP 3: RUN SQL QUERIES FOR SALES ANALYSIS
# =============================================================================

def run_sales_queries(conn):
    """Run various SQL queries to analyze sales data"""
    print("\n📊 STEP 3: Running SQL Queries for Sales Analysis")
    print("-" * 50)
    
    # Query 1: Basic sales summary by product
    print("\n🔍 QUERY 1: Sales Summary by Product")
    print("SQL: SELECT product, SUM(quantity) AS total_qty, SUM(quantity * price) AS revenue FROM sales GROUP BY product ORDER BY revenue DESC")
    
    query1 = """
        SELECT 
            product,
            SUM(quantity) AS total_qty,
            SUM(quantity * price) AS revenue,
            ROUND(AVG(price), 2) AS avg_price
        FROM sales 
        GROUP BY product 
        ORDER BY revenue DESC
    """
    
    df_products = pd.read_sql_query(query1, conn)
    print("\n📋 Results:")
    print(df_products.to_string(index=False))
    
    # Query 2: Sales summary by category
    print("\n\n🔍 QUERY 2: Sales Summary by Category")
    print("SQL: SELECT category, COUNT(*) AS num_products, SUM(quantity) AS total_qty, SUM(quantity * price) AS revenue FROM sales GROUP BY category")
    
    query2 = """
        SELECT 
            category,
            COUNT(*) AS num_transactions,
            COUNT(DISTINCT product) AS num_products,
            SUM(quantity) AS total_qty,
            SUM(quantity * price) AS revenue,
            ROUND(AVG(quantity * price), 2) AS avg_transaction_value
        FROM sales 
        GROUP BY category 
        ORDER BY revenue DESC
    """
    
    df_categories = pd.read_sql_query(query2, conn)
    print("\n📋 Results:")
    print(df_categories.to_string(index=False))
    
    # Query 3: Sales rep performance
    print("\n\n🔍 QUERY 3: Sales Representative Performance")
    print("SQL: SELECT sales_rep, COUNT(*) AS transactions, SUM(quantity * price) AS total_revenue FROM sales GROUP BY sales_rep")
    
    query3 = """
        SELECT 
            sales_rep,
            COUNT(*) AS transactions,
            SUM(quantity) AS total_items_sold,
            SUM(quantity * price) AS total_revenue,
            ROUND(AVG(quantity * price), 2) AS avg_sale_value
        FROM sales 
        GROUP BY sales_rep 
        ORDER BY total_revenue DESC
    """
    
    df_reps = pd.read_sql_query(query3, conn)
    print("\n📋 Results:")
    print(df_reps.to_string(index=False))
    
    # Query 4: Daily sales trend
    print("\n\n🔍 QUERY 4: Daily Sales Trend")
    print("SQL: SELECT sale_date, SUM(quantity * price) AS daily_revenue FROM sales GROUP BY sale_date ORDER BY sale_date")
    
    query4 = """
        SELECT 
            sale_date,
            COUNT(*) AS transactions,
            SUM(quantity) AS items_sold,
            SUM(quantity * price) AS daily_revenue
        FROM sales 
        GROUP BY sale_date 
        ORDER BY sale_date
    """
    
    df_daily = pd.read_sql_query(query4, conn)
    print("\n📋 Results:")
    print(df_daily.to_string(index=False))
    
    # Query 5: Overall summary statistics
    print("\n\n🔍 QUERY 5: Overall Business Summary")
    print("SQL: SELECT COUNT(*) AS total_transactions, SUM(quantity) AS total_items, SUM(quantity * price) AS total_revenue FROM sales")
    
    query5 = """
        SELECT 
            COUNT(*) AS total_transactions,
            COUNT(DISTINCT product) AS unique_products,
            COUNT(DISTINCT customer_id) AS unique_customers,
            SUM(quantity) AS total_items_sold,
            SUM(quantity * price) AS total_revenue,
            ROUND(AVG(quantity * price), 2) AS avg_transaction_value,
            MIN(sale_date) AS first_sale_date,
            MAX(sale_date) AS last_sale_date
        FROM sales
    """
    
    df_summary = pd.read_sql_query(query5, conn)
    print("\n📋 Results:")
    for column in df_summary.columns:
        print(f"   {column}: {df_summary[column].iloc[0]}")
    
    return df_products, df_categories, df_reps, df_daily, df_summary

# =============================================================================
# STEP 4: CREATE VISUALIZATIONS
# =============================================================================

def create_visualizations(df_products, df_categories, df_reps, df_daily):
    """Create various charts to visualize sales data"""
    print("\n📊 STEP 4: Creating Sales Visualizations")
    print("-" * 50)
    
    # Set up the plotting style
    plt.style.use('default')
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Sales Analysis Dashboard - SQLite + Python', fontsize=16, fontweight='bold')
    
    # Chart 1: Revenue by Product (Top 10)
    top_products = df_products.head(10)
    axes[0,0].barh(top_products['product'], top_products['revenue'], color='skyblue')
    axes[0,0].set_title('Top 10 Products by Revenue', fontweight='bold')
    axes[0,0].set_xlabel('Revenue ($)')
    axes[0,0].tick_params(axis='y', labelsize=8)
    
    # Add value labels on bars
    for i, v in enumerate(top_products['revenue']):
        axes[0,0].text(v + 100, i, f'${v:,.0f}', va='center', fontsize=8)
    
    # Chart 2: Revenue by Category (Pie Chart)
    axes[0,1].pie(df_categories['revenue'], labels=df_categories['category'], autopct='%1.1f%%', startangle=90)
    axes[0,1].set_title('Revenue Distribution by Category', fontweight='bold')
    
    # Chart 3: Sales Rep Performance
    axes[1,0].bar(df_reps['sales_rep'], df_reps['total_revenue'], color='lightgreen')
    axes[1,0].set_title('Sales Representative Performance', fontweight='bold')
    axes[1,0].set_xlabel('Sales Representative')
    axes[1,0].set_ylabel('Total Revenue ($)')
    axes[1,0].tick_params(axis='x', rotation=45)
    
    # Add value labels on bars
    for i, v in enumerate(df_reps['total_revenue']):
        axes[1,0].text(i, v + 200, f'${v:,.0f}', ha='center', va='bottom', fontsize=9)
    
    # Chart 4: Daily Sales Trend
    axes[1,1].plot(range(len(df_daily)), df_daily['daily_revenue'], marker='o', linewidth=2, markersize=6)
    axes[1,1].set_title('Daily Sales Trend', fontweight='bold')
    axes[1,1].set_xlabel('Days')
    axes[1,1].set_ylabel('Daily Revenue ($)')
    axes[1,1].grid(True, alpha=0.3)
    
    # Add trend line
    x = np.arange(len(df_daily))
    z = np.polyfit(x, df_daily['daily_revenue'], 1)
    p = np.poly1d(z)
    axes[1,1].plot(x, p(x), "r--", alpha=0.8, linewidth=1, label='Trend')
    axes[1,1].legend()
    
    plt.tight_layout()
    plt.savefig('sales_analysis_charts.png', dpi=300, bbox_inches='tight')
    print("✅ Charts saved as 'sales_analysis_charts.png'")
    plt.show()
    
    # Create individual simple bar chart as requested
    print("\n📊 Creating Simple Bar Chart (as requested in task)")
    plt.figure(figsize=(10, 6))
    
    # Simple bar chart - Revenue by Product
    df_products.head(8).plot(kind='bar', x='product', y='revenue', 
                            color='steelblue', alpha=0.8, legend=False)
    plt.title('Sales Revenue by Product - Simple Bar Chart', fontsize=14, fontweight='bold')
    plt.xlabel('Product')
    plt.ylabel('Revenue ($)')
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', alpha=0.3)
    
    # Add value labels
    for i, v in enumerate(df_products.head(8)['revenue']):
        plt.text(i, v + 200, f'${v:,.0f}', ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    plt.savefig('simple_sales_chart.png', dpi=300, bbox_inches='tight')
    print("✅ Simple bar chart saved as 'simple_sales_chart.png'")
    plt.show()

# =============================================================================
# STEP 5: ANSWER INTERVIEW QUESTIONS
# =============================================================================

def answer_interview_questions():
    """Provide comprehensive answers to interview questions"""
    print("\n🎯 STEP 5: Interview Questions & Answers")
    print("-" * 50)
    
    questions_answers = [
        {
            "question": "1. How did you connect Python to a database?",
            "answer": """
I connected Python to SQLite database using the sqlite3 module:

```python
import sqlite3
conn = sqlite3.connect('sales_data.db')
```

This creates a connection object that allows Python to:
- Execute SQL queries
- Fetch results
- Manage transactions
- Close connections properly

SQLite is built into Python, so no additional installation required.
            """
        },
        {
            "question": "2. What SQL query did you run?",
            "answer": """
I ran several SQL queries for comprehensive analysis:

Main query:
```sql
SELECT 
    product,
    SUM(quantity) AS total_qty,
    SUM(quantity * price) AS revenue,
    ROUND(AVG(price), 2) AS avg_price
FROM sales 
GROUP BY product 
ORDER BY revenue DESC
```

This query:
- Selects product names
- Calculates total quantity sold per product
- Calculates total revenue (quantity × price)
- Finds average price per product
- Groups results by product
- Orders by revenue (highest first)
            """
        },
        {
            "question": "3. What does GROUP BY do?",
            "answer": """
GROUP BY aggregates rows with the same values into summary rows.

Example: Without GROUP BY:
| product | quantity | price |
|---------|----------|-------|
| Laptop  | 2        | 999.99|
| Laptop  | 3        | 999.99|
| Mouse   | 10       | 29.99 |

With GROUP BY product:
| product | total_qty | revenue |
|---------|-----------|---------|
| Laptop  | 5         | 4999.95 |
| Mouse   | 10        | 299.90  |

GROUP BY combines multiple rows into single summary rows,
enabling aggregate functions like SUM(), COUNT(), AVG().
            """
        },
        {
            "question": "4. How did you calculate revenue?",
            "answer": """
Revenue was calculated using SQL multiplication:

```sql
SUM(quantity * price) AS revenue
```

Process:
1. For each row: multiply quantity × price
2. SUM() aggregates all individual revenues
3. GROUP BY ensures summation per product/category

Example calculation:
- Laptop: 5 units × $999.99 = $4,999.95
- Mouse: 25 units × $29.99 = $749.75
- Total Laptop revenue: $4,999.95

This gives accurate revenue per product or category.
            """
        },
        {
            "question": "5. How did you visualize the result?",
            "answer": """
I created visualizations using matplotlib:

```python
import matplotlib.pyplot as plt

# Simple bar chart (as requested)
df_products.plot(kind='bar', x='product', y='revenue')
plt.title('Sales Revenue by Product')
plt.savefig('sales_chart.png')
plt.show()
```

Advanced visualizations included:
- Horizontal bar chart for top products
- Pie chart for category distribution
- Line chart for daily trends
- Multiple subplots for dashboard view

Each chart includes titles, labels, and value annotations.
            """
        },
        {
            "question": "6. What does pandas do in your code?",
            "answer": """
Pandas serves as a bridge between SQL and Python:

```python
import pandas as pd
df = pd.read_sql_query(query, conn)
```

Pandas functions:
1. **Data Import**: pd.read_sql_query() converts SQL results to DataFrame
2. **Data Structure**: Provides table-like structure for analysis
3. **Data Manipulation**: Easy filtering, sorting, calculations
4. **Visualization**: Direct plotting with .plot() method
5. **Display**: Clean data presentation with print(df)

Benefits:
- Seamless SQL-to-Python data flow
- Rich data analysis capabilities
- Built-in plotting functions
- Easy data export/import
            """
        },
        {
            "question": "7. What's the benefit of using SQL inside Python?",
            "answer": """
Combining SQL with Python provides powerful advantages:

**SQL Strengths:**
- Efficient data filtering and aggregation
- Standardized query language
- Optimized database operations
- Complex joins and grouping

**Python Strengths:**
- Advanced analytics and visualization
- Machine learning capabilities
- Flexible data manipulation
- Rich ecosystem of libraries

**Combined Benefits:**
1. **Data Extraction**: SQL efficiently queries databases
2. **Data Analysis**: Python processes results with pandas
3. **Visualization**: matplotlib/seaborn creates charts
4. **Automation**: Python scripts automate entire workflow
5. **Scalability**: Handle large datasets efficiently
6. **Integration**: Connect multiple data sources

This approach leverages the best of both technologies.
            """
        },
        {
            "question": "8. Could you run the same SQL query directly in DB Browser for SQLite?",
            "answer": """
Yes, absolutely! The same SQL queries work in DB Browser for SQLite:

**In DB Browser:**
1. Open DB Browser for SQLite
2. Open 'sales_data.db' file
3. Go to 'Execute SQL' tab
4. Paste query:
   ```sql
   SELECT product, SUM(quantity) AS total_qty, 
          SUM(quantity * price) AS revenue 
   FROM sales GROUP BY product ORDER BY revenue DESC
   ```
5. Click 'Execute' - same results appear

**Differences:**
- DB Browser: Manual, point-and-click interface
- Python: Automated, scriptable, programmable
- DB Browser: Good for quick exploration
- Python: Better for analysis, visualization, automation

**Advantages of Python approach:**
- Repeatable analysis
- Automated reporting
- Advanced visualizations
- Integration with other data sources
- Version control and documentation
            """
        }
    ]
    
    for qa in questions_answers:
        print(f"\n❓ {qa['question']}")
        print(f"✅ {qa['answer']}")
        print("-" * 70)

# =============================================================================
# STEP 6: GENERATE PROJECT SUMMARY
# =============================================================================

def generate_project_summary():
    """Generate comprehensive project summary"""
    print("\n📋 STEP 6: Project Summary & Documentation")
    print("-" * 50)
    
    summary = f"""
SQLITE + PYTHON SALES ANALYSIS - PROJECT SUMMARY
===============================================

Project Completed: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}

OBJECTIVES ACHIEVED:
✅ Created SQLite database with sales data
✅ Connected Python to SQLite database
✅ Executed multiple SQL queries for analysis
✅ Used pandas to handle query results
✅ Created visualizations with matplotlib
✅ Generated comprehensive insights
✅ Answered all interview questions

TECHNICAL IMPLEMENTATION:
------------------------
• Database: SQLite ('sales_data.db')
• Records: 20 sample sales transactions
• Tables: 1 sales table with 8 columns
• Queries: 5 analytical SQL queries
• Visualizations: 4 different chart types
• Libraries: sqlite3, pandas, matplotlib, numpy

SQL QUERIES EXECUTED:
-------------------
1. Product sales summary with revenue calculation
2. Category-wise performance analysis
3. Sales representative performance tracking
4. Daily sales trend analysis
5. Overall business summary statistics

KEY FINDINGS:
-----------
• Total Revenue: $30,000+ across all products
• Top Category: Electronics (highest revenue)
• Best Product: Laptop (highest individual revenue)
• Sales Period: 10 days of transaction data
• Top Sales Rep: Identified through performance metrics

VISUALIZATIONS CREATED:
---------------------
• Top 10 Products Revenue (Horizontal Bar Chart)
• Revenue by Category (Pie Chart)
• Sales Rep Performance (Bar Chart)
• Daily Sales Trend (Line Chart with Trend Line)
• Simple Bar Chart (As requested in task)

FILES GENERATED:
--------------
• sales_data.db - SQLite database file
• sales_analysis_charts.png - Comprehensive dashboard
• simple_sales_chart.png - Basic bar chart
• Python script with complete analysis

INTERVIEW READINESS:
------------------
All 8 interview questions answered with:
• Technical explanations
• Code examples
• Practical demonstrations
• Benefits and use cases

SKILLS DEMONSTRATED:
------------------
• Database connectivity and management
• SQL query writing and optimization
• Python-SQL integration
• Data analysis with pandas
• Data visualization with matplotlib
• Project documentation and presentation

READY FOR SUBMISSION: ✅
Complete solution with professional documentation.
    """
    
    # Save summary to file
    with open('project_summary.txt', 'w') as f:
        f.write(summary)
    
    print(summary)
    print("✅ Project summary saved as 'project_summary.txt'")

# =============================================================================
# MAIN EXECUTION FUNCTION
# =============================================================================

def main():
    """Main function to execute the complete analysis"""
    print("🚀 Starting SQLite + Python Sales Analysis...")
    
    try:
        # Step 1: Create database
        create_sales_database()
        
        # Step 2: Connect to database
        conn = connect_to_database()
        if conn is None:
            print("❌ Failed to connect to database. Exiting...")
            return
        
        # Step 3: Run SQL queries
        df_products, df_categories, df_reps, df_daily, df_summary = run_sales_queries(conn)
        
        # Step 4: Create visualizations
        create_visualizations(df_products, df_categories, df_reps, df_daily)
        
        # Close database connection
        conn.close()
        print("🔒 Database connection closed successfully")
        
        # Step 5: Answer interview questions
        answer_interview_questions()
        
        # Step 6: Generate project summary
        generate_project_summary()
        
        print("\n" + "="*70)
        print("🎉 TASK 6 COMPLETED SUCCESSFULLY!")
        print("="*70)
        print("📊 Analysis Summary:")
        print("   • SQLite database created with 20 sales records")
        print("   • 5 comprehensive SQL queries executed")
        print("   • Multiple visualizations generated")
        print("   • All interview questions answered")
        print("   • Professional documentation created")
        
        print(f"\n📁 Generated Files:")
        files = [
            "sales_data.db - SQLite database",
            "sales_analysis_charts.png - Comprehensive dashboard",
            "simple_sales_chart.png - Basic bar chart (as requested)",
            "project_summary.txt - Complete project documentation",
            "This Python script - Complete implementation"
        ]
        
        for i, filename in enumerate(files, 1):
            print(f"   {i}. {filename}")
        
        print(f"\n✅ Ready for GitHub submission!")
        print(f"✅ All requirements fulfilled!")
        print(f"✅ Interview questions answered!")
        print(f"✅ Visualizations created!")
        
    except Exception as e:
        print(f"❌ Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()