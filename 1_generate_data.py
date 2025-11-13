# 1_generate_data.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
 
np.random.seed(42)
n_rows = 1000
 
data = {
    'user_id': np.random.randint(1000, 9999, n_rows),
    'timestamp': [datetime.now() - timedelta(days=np.random.randint(1, 30)) for _ in range(n_rows)],
    'duration_min': np.random.randint(1, 60, n_rows),
    'data_gb': np.round(np.random.uniform(0.1, 5.0, n_rows), 2),
    'location': np.random.choice(['Mumbai', 'Delhi', 'Pune', 'Bangalore'], n_rows)
}
 
df = pd.DataFrame(data)
df.to_csv('sample_cdrs.csv', index=False)
print("Your telecom data is ready: sample_cdrs.csv")