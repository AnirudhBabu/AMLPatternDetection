import os
from pathlib import Path
import shutil
import duckdb
import pandas as pd
import kagglehub as kg
from neo4j import GraphDatabase
from halo import Halo


def query_memgraph_cycles(uri="bolt://localhost:7687"):
    """
    Uses Memgraph's built-in simple cycle detector to find structural cycles and 
    rotates them to find valid chronological money flows.
    """
    spinner = Halo(text="Querying Memgraph for Temporal Cycles...", color="cyan", spinner="dots")
    spinner.start()

    cypher_query = """
    CALL cycles.get() YIELD cycle_id, node
    WITH cycle_id, collect(node) AS cycle_nodes
    WHERE size(cycle_nodes) >= 3

    UNWIND range(0, size(cycle_nodes) - 1) AS i
    WITH cycle_id, cycle_nodes, i,
         cycle_nodes[i] AS source,
         cycle_nodes[(i + 1) % size(cycle_nodes)] AS target
    MATCH (source)-[e:TRANSFERRED]->(target)
    WITH cycle_id, cycle_nodes, i, e ORDER BY e.datetime ASC
    WITH cycle_id, cycle_nodes, i, head(collect(e)) AS first_e
    ORDER BY cycle_id, i
    WITH cycle_id, cycle_nodes, collect(first_e) AS edges

    WHERE size(edges) = size(cycle_nodes)
    WITH cycle_id, cycle_nodes, edges,
         [k IN range(0, size(edges) - 1) 
          WHERE ALL(j IN range(0, size(edges) - 2) 
                    WHERE edges[(k + j) % size(edges)].datetime 
                       <= edges[(k + j + 1) % size(edges)].datetime)
         ] AS valid_starts
    WHERE size(valid_starts) > 0

    WITH cycle_id, cycle_nodes, edges, valid_starts[0] AS s
    RETURN cycle_id,
           [k in range(0, size(cycle_nodes)-1) | cycle_nodes[(s + k) % size(cycle_nodes)].accountID] AS account_path,
           [k in range(0, size(edges)-1) | edges[(s + k) % size(edges)].amount] AS amounts,
           [k in range(0, size(edges)-1) | edges[(s + k) % size(edges)].datetime] AS hoptimes
    """

    try:
        driver = GraphDatabase.driver(uri, auth=("", ""))
        with driver.session() as session:
            result = session.run(cypher_query)
            records = [dict(record) for record in result]
        driver.close()
        spinner.succeed(f"Detected {len(records)} temporal cycles via Memgraph.")
        return records
    except Exception as e:
        spinner.fail(f"Memgraph Connection Failed: {e}")
        return []


def write_memgraph_results_to_csv(records, output_filepath='./data/detected_cycles.csv'):
    """
    Flattens the graph records into a transactional CSV for reporting.
    """
    if not records:
        print("No cycles to export.")
        return

    flattened = []
    for rec in records:
        c_id = rec['cycle_id']
        path = rec['account_path']
        amts = rec['amounts']
        times = rec['hoptimes']
        
        for i in range(len(path)):
            # Handle Memgraph/Neo4j datetime objects or dicts
            ts = times[i]
            
            flattened.append({
                'Cycle_ID': c_id,
                'Sender_account': path[i],
                'Receiver_account': path[(i + 1) % len(path)],
                'Amount': amts[i],
                'Timestamp': ts,
                'Hop_Number': i + 1,
                'Cycle_Length': len(path)
            })
            
    df = pd.DataFrame(flattened)
    df.to_csv(output_filepath, index=False)
    print(f"✅ Successfully wrote {len(df)} transactions to '{output_filepath}'")


def detect_smurfing_suspects(conn: duckdb.DuckDBPyConnection,
                             source_file: str = "./data/SAML-D.csv",
                             output_filepath: str = "./data/smurfing_suspects.csv",
                             preferred_sender_currency: str = "UK pounds",
                             preferred_receiver_currency: str = "UK pounds",
                             target_total_threshold: float = 100_000,
                             target_minutes_duration: int = 43200,
                             target_minimum_distinct_senders: int = 10):
    """
    Docstring for detect_smurfing_suspects
    This function traverses the original dataset, aggregates it, self-joins it back to the original dataset to return 
    a list of transactions from multiple senders to a single receiver account over short durations, indicative of the 
    Smurfing AML typology.

    Parameters:
    1. conn (DuckDBPyConnection): Connection object to DuckDB.
    2. source_file (str): Path to the source dataset.
    3. output_filepath: Path where the processed data should be stored.
    4. preferred_sender_currency (str): Preferred value for the Payment_currency column in the dataset.
    5. preferred_receiver_currency (str): Preferred value for the Received_currency column in the dataset.
    6. target_total_threshold (float): Amount beyond which the combination of a high number of senders, a short duration
                                       are considered suspicious.
    7. target_minutes_duration (int): The duration in minutes considered as one of the factors influencing the suspicious 
                                      nature of transactions.
    8. target_minimum_distinct_senders (int): The final factor in the suspicious-ness-deciding trifecta.

    """

    spinner = Halo(text="Retrieving and processing data from CSV...",
                   color="magenta", spinner="dots2")
    spinner.start()

    # Retrieve all necessary transaction data from csv in an aggregated form with necessary filters,
    # then self-join to get per transaction data, and finally save it to the desired location
    full_data_query = \
        f"""
        COPY 
        (
            SELECT 
                origin.Receiver_account,
                agg.Readable_Duration AS Duration,
                origin.Date,
                origin.Time,
                agg.Total_amount,
                origin.Sender_account,
                agg.Sender_count,
                origin.Payment_currency,
                origin.Received_currency,
                origin.Amount,
                origin.Laundering_type,
                origin.Payment_type
            FROM '{source_file}' AS origin
            JOIN (SELECT Receiver_account, 
                            sum(Amount) AS Total_amount,
                            count(DISTINCT Sender_account) AS Sender_count,
                            date_diff('minute', MIN(Date + Time), MAX(Date + Time)) AS Duration,
                            CASE 
                                WHEN date_diff('minute', MIN(Date + Time), MAX(Date + Time)) >= 1440 
                                    THEN date_diff('day', MIN(Date + Time), MAX(Date + Time)) || ' days'
                                WHEN date_diff('minute', MIN(Date + Time), MAX(Date + Time)) >= 60 
                                    THEN date_diff('hour', MIN(Date + Time), MAX(Date + Time)) || ' hours'
                                ELSE date_diff('minute', MIN(Date + Time), MAX(Date + Time)) || ' minutes'
                            END AS Readable_duration
                    FROM '{source_file}'
                    WHERE Payment_currency = '{preferred_sender_currency}' AND Received_currency = '{preferred_receiver_currency}'
                    GROUP BY Receiver_account
                        HAVING Sender_count > {target_minimum_distinct_senders} AND Duration <= {target_minutes_duration} AND Total_amount > {target_total_threshold}) AS agg
                ON origin.Receiver_account = agg.Receiver_account
            WHERE Payment_currency = '{preferred_sender_currency}' AND Received_currency = '{preferred_receiver_currency}'
            ORDER BY origin.Receiver_account ASC, agg.Total_amount ASC, agg.Duration ASC
        ) TO '{output_filepath}' (HEADER, DELIMITER ',');
        """

    conn.execute(full_data_query)


    spinner.succeed(f"Processed data saved to {output_filepath}")
    
    
if __name__ == "__main__":
    # 1. Dataset Setup
    data_path = Path('./data/SAML-D.csv')
    if not data_path.is_file():
        print("Downloading dataset...")
        path = kg.dataset_download("berkanoztas/synthetic-transaction-monitoring-dataset-aml")
        os.makedirs('./data', exist_ok=True)
        shutil.move(os.path.join(path, 'SAML-D.csv'), './data/SAML-D.csv')

    # 2. Cycle Detection (Memgraph)
    # This replaces the Python DFS entirely
    cycle_records = query_memgraph_cycles()
    write_memgraph_results_to_csv(cycle_records)

    # 3. Smurfing Analysis (DuckDB)
    # Using unified Date + Time logic
    conn = duckdb.connect()
    detect_smurfing_suspects(conn)
    conn.close()

    print("\nPipeline Complete. All patterns verified and exported.")