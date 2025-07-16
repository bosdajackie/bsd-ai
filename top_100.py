import requests
import pandas as pd
from tqdm import tqdm

api_base_url = "http://localhost:8001"

query = '''
SELECT TOP 100 [item_id], SUM([qty]) as total_qty
FROM SOJournal
GROUP BY [item_id]
ORDER BY SUM([qty]) DESC
'''

print("Querying top 100 parts...")
response = requests.get(f"{api_base_url}/query", params={"q": query})
top_parts = response.json()['result']

results = []

print("Querying top 5 customers for each part...")
for part in tqdm(top_parts, desc="Parts"):
    item_id = part[0]
    qty = part[1]
    query = f'''
    SELECT TOP 5 [cust_id], SUM([qty]) as total_qty
    FROM SOJournal
    WHERE [item_id] = '{item_id}'
    GROUP BY [cust_id]
    ORDER BY SUM([qty]) DESC
    '''
    response = requests.get(f"{api_base_url}/query", params={"q": query})
    top_customers = response.json().get('result', [])
    for cust in top_customers:
        cust_id = cust[0]
        cust_qty = cust[1]
        results.append({
            'item_id': item_id,
            'item_total_qty': qty,
            'cust_id': cust_id,
            'cust_qty': cust_qty
        })

print("Saving results to top_100_parts_customers.xlsx...")
results_df = pd.DataFrame(results)
results_df.to_excel('top_100_parts_customers.xlsx', index=False)
print('Results saved to top_100_parts_customers.xlsx')


