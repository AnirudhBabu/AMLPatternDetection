import duckdb
import pandas as pd

def verify_cycle_in_duckdb(csv_path, account_ids):
    """
    Verifies a chronological cycle in DuckDB using unified Date/Time columns.
    """
    # Using an in-memory connection to query the CSV directly
    con = duckdb.connect()
    
    # Unified timestamp expression: Combine Date and Time into a single value.
    # We cast them explicitly to ensure DuckDB treats them as a TIMESTAMP.
    ts_expr = '(CAST("Date" AS DATE) + CAST("Time" AS TIME))'
    
    ctes = []
    for i in range(len(account_ids)):
        src = account_ids[i]
        dst = account_ids[(i + 1) % len(account_ids)]
        
        if i == 0:
            # First hop: Create the 'ts' alias for the unified timestamp
            cte = f"""
            hop1 AS (
                SELECT Sender_account, Receiver_account, Amount, {ts_expr} AS ts
                FROM '{csv_path}'
                WHERE Sender_account = {src} AND Receiver_account = {dst}
            )"""
        else:
            # Subsequent hops: Compare 'ts' against the previous hop's 'ts'
            # This ensures the 'arrow of time' is respected across dates.
            cte = f"""
            hop{i+1} AS (
                SELECT t.Sender_account, t.Receiver_account, t.Amount, {ts_expr} AS ts
                FROM '{csv_path}' t, hop{i}
                WHERE t.Sender_account = {src} AND t.Receiver_account = {dst}
                AND {ts_expr} >= hop{i}.ts
            )"""
        ctes.append(cte)
    
    # Final assembly of the SQL string
    query = "WITH " + ",\n".join(ctes)
    query += "\nSELECT " + ", ".join([f"hop{i+1}.ts as t{i+1}" for i in range(len(account_ids))])
    query += "\nFROM " + ", ".join([f"hop{i+1}" for i in range(len(account_ids))]) + ";"
    
    print(f"--- Verifying Cycle across {len(account_ids)} accounts ---")
    try:
        df = con.execute(query).df()
        if df.empty:
            print("RESULT: No matching chronological flow found in DuckDB.")
        else:
            print(f"RESULT: Found {len(df)} valid chronological sequences!")
            print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"SQL Error: {e}")
        # Print the query if it fails to help debug the column names
        print("\nGenerated Query:\n", query)
        return None

# Use the target path from the cycle we identified
target_path = [2521152088, 718745407, 8657935466, 2361741456, 2299468667]

# Point to your local CSV
verify_cycle_in_duckdb('./data/SAML-D.csv', target_path)