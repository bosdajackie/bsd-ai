"""
requirements: requests, pandas, matplotlib
"""
from typing import List, Union, Generator, Iterator
import pandas as pd
import matplotlib.pyplot as plt
import requests
import json
import re
from datetime import datetime
import os
import uuid
import io
import matplotlib.ticker as mticker

def stream(response):
    """Stream Ollama API response, extracting content from JSON"""
    if isinstance(response, str):
        yield response
    elif hasattr(response, 'iter_lines'):  # Ollama response
        for line in response.iter_lines():
            if line:
                line_str = line.decode('utf-8')
                if line_str.strip():
                    try:
                        data = json.loads(line_str)
                        if 'message' in data and 'content' in data['message']:
                            yield data['message']['content']
                    except json.JSONDecodeError:
                        continue
    elif isinstance(response, (Generator, Iterator)):
        for chunk in response:
            yield chunk
    elif hasattr(response, '__iter__'):
        for chunk in response:
            yield chunk
    else:
        yield str(response)

def auto_stream(func):
    """Decorator to automatically stream the result"""
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if hasattr(result, '__iter__') and not isinstance(result, str):
            for chunk in result:
                yield chunk
        else:
            yield result
    return wrapper

class Pipeline:
    def __init__(self):
        self.ollama_host = "http://10.10.12.30:11435/api/chat"
        self.name = "Plot Sales Data"
        self.api_base_url = "http://host.docker.internal:8001"  # For backend API calls
        self.image_base_url = "http://10.10.12.30:8001"  # For browser image links (LAN IP)
        
        # Create plots directory if it doesn't exist
        self.plots_dir = "plots"
        print(f"[DEBUG] Checking/creating plots directory at: {os.path.abspath(self.plots_dir)}")
        if not os.path.exists(self.plots_dir):
            os.makedirs(self.plots_dir)
            print(f"[DEBUG] Created plots directory at: {os.path.abspath(self.plots_dir)}")
        else:
            print(f"[DEBUG] Plots directory already exists at: {os.path.abspath(self.plots_dir)}")
        
        pass

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")
        pass

    async def on_shutdown(self):
        # This function is called when the server is shutdown.
        print(f"on_shutdown:{__name__}")
        pass

    def extract_part_number(self, user_message: str) -> str:
        """Extract part number from user message using regex patterns"""
        # Common patterns for part numbers (adjust based on your format)
        patterns = [
            r'\b\d{6,}\b',  # 6+ digit numbers
            r'\b[A-Z]{2,}\d{3,}\b',  # Letters followed by numbers
            r'\b\d{3,}[A-Z]{1,3}\b',  # Numbers followed by letters
            r'part[:\s]*([A-Z0-9-]+)',  # "part: ABC123" or "part ABC123"
            r'item[:\s]*([A-Z0-9-]+)',  # "item: ABC123" or "item ABC123"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, user_message, re.IGNORECASE)
            if match:
                # If the pattern has a group, use it, otherwise use the whole match
                return match.group(1) if match.groups() else match.group(0)
        
        return None

    def query_sales_dates(self, part_number: str) -> dict:
        """Query the database for sales order dates, quantities, and customer IDs for a given part number"""
        try:
            # Construct the SQL query to get so_date, qty, and cust_id
            query = f"SELECT so_date, qty, cust_id FROM SOJournal WHERE item_id = '{part_number}'"
            print(f"[DEBUG] Running query: {query}")
            # Make request to access_api.py endpoint
            response = requests.get(f"{self.api_base_url}/query", params={"q": query})
            print(f"[DEBUG] Query response status: {response.status_code}")
            if response.status_code == 200:
                result = response.json()
                print(f"[DEBUG] Query result: {result}")
                if "error" in result:
                    return {"error": f"Database error: {result['error']}"}
                return {"success": True, "data": result.get("result", [])}
            else:
                print(f"[DEBUG] API request failed with status {response.status_code}")
                return {"error": f"API request failed with status {response.status_code}"}
                
        except Exception as e:
            print(f"[DEBUG] Query exception: {str(e)}")
            return {"error": f"Query failed: {str(e)}"}

    def create_sales_plot(self, sales_data: dict, part_number: str) -> str:
        print(f"[DEBUG] Creating sales plot for part number: {part_number}")
        print(f"[DEBUG] Sales data received: {sales_data}")
        if "error" in sales_data:
            print(f"[DEBUG] Error in sales data: {sales_data['error']}")
            return f"❌ Error: {sales_data['error']}"
        
        data = sales_data.get("data", [])
        print(f"[DEBUG] Data for DataFrame: {data}")
        if not data:
            print(f"[DEBUG] No sales orders found for this part number.")
            return "📊 No sales orders found for this part number."
        
        # Convert data to DataFrame with so_date, qty, and cust_id columns
        df = pd.DataFrame(data, columns=['so_date', 'qty', 'cust_id'])
        print(f"[DEBUG] DataFrame created: {df}")
        
        # Convert dates to datetime, qty to numeric, and fill missing cust_id
        df['so_date'] = pd.to_datetime(df['so_date'], errors='coerce')
        df['qty'] = pd.to_numeric(df['qty'], errors='coerce').fillna(0)
        df['cust_id'] = df['cust_id'].fillna('Unknown').astype(str)
        print(f"[DEBUG] DataFrame after conversions: {df}")
        
        # Remove rows with invalid dates
        df = df.dropna(subset=['so_date'])
        print(f"[DEBUG] DataFrame after dropping NA: {df}")
        
        if df.empty:
            print(f"[DEBUG] No valid sales order dates found for this part number.")
            return "📊 No valid sales order dates found for this part number."
        
        # Sort by date
        df = df.sort_values('so_date')
        print(f"[DEBUG] DataFrame after sorting: {df}")
        
        # Group by date and sum qty for each date
        daily_qty = df.groupby('so_date')['qty'].sum()
        print(f"[DEBUG] Daily quantity grouped: {daily_qty}")
        
        # Group by customer and sum qty for each customer
        customer_qty = df.groupby('cust_id')['qty'].sum().sort_values(ascending=False)
        print(f"[DEBUG] Customer quantity grouped: {customer_qty}")
        
        # Top 5 customers by quantity
        top_customers = customer_qty.head(5)
        top_customers_str = ', '.join([f"{cust}: {int(qty)}" for cust, qty in top_customers.items()])
        
        # Prepare monthly, customer-segmented data for the stacked bar chart
        df['month'] = df['so_date'].dt.to_period('M').dt.to_timestamp()
        monthly_customer_qty = df.groupby(['month', 'cust_id'])['qty'].sum().unstack(fill_value=0)
        # Ensure all months in the range are present, even if there are no sales
        all_months = pd.date_range(df['month'].min(), df['month'].max(), freq='MS')
        monthly_customer_qty = monthly_customer_qty.reindex(all_months, fill_value=0)
        monthly_customer_qty.index.name = 'month'
        print(f"[DEBUG] Monthly customer quantity (with all months): {monthly_customer_qty}")
        
        # Format x labels as 'Mon YYYY'
        month_labels = [d.strftime('%b %Y') for d in monthly_customer_qty.index]
        
        # Prepare pie chart data (top 10 customers, group small percentages into 'Other')
        customer_qty_pct = customer_qty / customer_qty.sum()
        customer_qty_main = customer_qty_pct[customer_qty_pct >= 0.03]  # >=3%
        customer_qty_other = customer_qty_pct[customer_qty_pct < 0.03]
        pie_data = customer_qty_main.copy()
        if not customer_qty_other.empty:
            pie_data['Other'] = customer_qty_other.sum()
        pie_data = pie_data.sort_values(ascending=False)
        pie_labels = pie_data.index
        pie_values = pie_data.values
        
        # Create the plot in memory with the new layout
        fig = plt.figure(figsize=(16, 12))
        gs = plt.GridSpec(2, 2, height_ratios=[1, 1], width_ratios=[2, 1], hspace=0.45, wspace=0.25)  # Increased hspace for more vertical spacing
        # Top half: Stacked bar chart (histogram)
        ax1 = fig.add_subplot(gs[0, :])
        bars = monthly_customer_qty.plot(kind='bar', stacked=True, ax=ax1, colormap='tab20', width=1.0, legend=False)
        total_monthly_sales = monthly_customer_qty.sum(axis=1)
        ax1.plot(range(len(total_monthly_sales)), total_monthly_sales, color='black', marker='o', linestyle='-', linewidth=2, label='Total Sales')
        ax1.set_title(f'Monthly Sales Quantity Distribution by Customer for Part {part_number}', fontsize=16, fontweight='bold', pad=12)
        ax1.set_xlabel('Month', fontsize=13)
        ax1.set_ylabel('Total Quantity Ordered', fontsize=13)
        ax1.set_xticks(range(len(month_labels)))
        ax1.set_xticklabels(month_labels, rotation=30, ha='right')
        ax1.legend(title='Customer', bbox_to_anchor=(1.01, 1), loc='upper left')
        ax1.grid(True, alpha=0.3)
        ax1.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        for i, month in enumerate(monthly_customer_qty.index):
            bottom = 0
            for j, cust in enumerate(monthly_customer_qty.columns):
                qty = monthly_customer_qty.loc[month, cust]
                if qty > 0:
                    ax1.text(i, bottom + qty / 2, f'{int(qty)}', ha='center', va='center', fontsize=9, color='black', rotation=0)
                bottom += qty
        # Bottom left: Pie chart
        ax2 = fig.add_subplot(gs[1, 0])
        wedges, texts, autotexts = ax2.pie(
            pie_values,
            labels=pie_labels,
            autopct='%1.1f%%',
            startangle=90,
            colors=plt.get_cmap('tab20').colors,
            textprops={'fontsize': 10, 'fontfamily': 'DejaVu Sans', 'color': '#222'},
            labeldistance=1.15,   # Move labels outward
            pctdistance=0.75      # Move percentages closer to edge
        )
        for t in texts + autotexts:
            t.set_fontsize(11)
            t.set_fontfamily('DejaVu Sans')
            t.set_color('#222')
        ax2.set_title(f'Top Customers by Quantity for Part {part_number}', fontsize=14, fontweight='bold', pad=10)
        # Calculate repeat buyers vs. one-time buyers
        customer_order_counts = df.groupby('cust_id').size()
        repeat_buyers = (customer_order_counts > 1).sum()
        one_time_buyers = (customer_order_counts == 1).sum()
        repeat_buyer_str = f"Repeat Buyers: {repeat_buyers} | One-time Buyers: {one_time_buyers}"
        # Bottom right: Text summary
        ax3 = fig.add_subplot(gs[1, 1])
        ax3.axis('off')
        total_qty = daily_qty.sum()
        date_range = f"{df['so_date'].min().strftime('%Y-%m-%d')} to {df['so_date'].max().strftime('%Y-%m-%d')}"
        summary_lines = [
            f"Total Quantity: {total_qty}",
            f"Date Range: {date_range}",
            f"Top 5 Customers: {top_customers_str}",
            f"{repeat_buyer_str}",
        ]
        df['quarter'] = df['so_date'].dt.to_period('Q').dt.to_timestamp()
        df['year'] = df['so_date'].dt.year
        quarterly_sales = df.groupby('quarter')['qty'].sum()
        yearly_sales = df.groupby('year')['qty'].sum()
        # Format quarterly sales as 'YYYY-QN'
        def format_quarter(q):
            if hasattr(q, 'year') and hasattr(q, 'quarter'):
                return f"{q.year}-Q{q.quarter}"
            return str(q)
        quarterly_sales_str = ' | '.join([f"{format_quarter(q)}: {int(qty)}" for q, qty in quarterly_sales.items()])
        yearly_sales_str = ' | '.join([f"{int(y)}: {int(qty)}" for y, qty in yearly_sales.items()])
        summary_lines.append(f'Quarterly Sales: {quarterly_sales_str}')
        summary_lines.append(f'Yearly Sales: {yearly_sales_str}')
        # Join all lines into a single string, each on a new line
        summary_text = '\n'.join(summary_lines)
        import textwrap
        wrapped_lines = []
        for line in summary_lines:
            wrapped_lines.extend(textwrap.wrap(line, width=45))
        wrapped_text = '\n'.join(wrapped_lines)
        # Calculate right margin in axes coordinates for 1 inch
        fig_width_inch = fig.get_figwidth()
        right_margin_frac = 1.0 - (1.0 / fig_width_inch)
        ax3.set_xlim(0, right_margin_frac)
        bbox_props = dict(facecolor='white', alpha=0.8, edgecolor='none', boxstyle='round,pad=0.3')
        ax3.text(0, 1, wrapped_text, fontsize=13, fontfamily='DejaVu Sans', color='#222', ha='left', va='top', fontweight='bold', bbox=bbox_props, transform=ax3.transAxes, clip_on=False)
        plt.tight_layout(rect=[0, 0, 1, 1])
        filename = f"sales_plot_{part_number}_{uuid.uuid4().hex[:8]}.png"
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=300, bbox_inches='tight')
        plt.close()
        buf.seek(0)
        # POST to backend
        files = {'file': (filename, buf, 'image/png')}
        response = requests.post(f"{self.api_base_url}/save_plot", files=files)
        if response.status_code == 200:
            result = response.json()
            print(f"[DEBUG] Uploaded plot, backend returned filename: {result['filename']}")
            return result["filename"]
        else:
            print(f"[DEBUG] Failed to upload plot: {response.text}")
            return None

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        # This function is called when a new user_message is received.
        
        # First, try to extract a part number from the user message
        part_number = self.extract_part_number(user_message)
        
        if part_number:
            # Query the database for sales data
            yield f"🔍 Searching for sales data for part number: {part_number}\n\n"
            
            sales_data = self.query_sales_dates(part_number)
            plot_result = self.create_sales_plot(sales_data, part_number)
            
            if plot_result is None or plot_result.startswith("❌") or plot_result.startswith("📊"):
                # Error or no data message
                yield plot_result if plot_result else "❌ Failed to generate or upload plot."
            else:
                # Return the plot as a markdown image link using image_base_url
                image_url = f"{self.image_base_url}/plots/{plot_result}"
                yield f"📊 Sales Plot for Part {part_number}:\n"
                yield f"![sales_plot]({image_url})\n"
        else:
            # If no part number found, provide instructions
            yield "Please provide a part number to search for sales data.\n"
            yield "You can specify it in formats like:\n"
            yield "- Just the part number (e.g., 513001)\n"
            yield "- 'part: 513001' or 'item: 513001'\n"
            yield "- Include it in your question about sales data\n"
