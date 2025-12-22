import pandas as pd

try:
    df = pd.read_csv('data/funds.csv', dtype={'基金代码': str}, encoding='utf-8-sig')
    print(f"Total rows: {len(df)}")
    print(f"Unique codes: {df['基金代码'].nunique()}")
    
    if len(df) != df['基金代码'].nunique():
        print("Duplicates found!")
        print(df['基金代码'].value_counts().head())
        
        # Show sample duplicates
        dup_code = df['基金代码'].value_counts().index[0]
        print(f"\nSample duplicate {dup_code}:")
        print(df[df['基金代码'] == dup_code])
    else:
        print("No duplicates in funds.csv")
        
except Exception as e:
    print(e)
