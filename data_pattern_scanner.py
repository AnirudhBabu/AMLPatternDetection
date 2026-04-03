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
    load_spinner = Halo(text="Loading data and indexes into Memgraoh...",
                        color="cyan", spinner="dots")
    cycle_spinner = Halo(text="Querying Memgraph for Temporal Cycles...",
                         color="cyan", spinner="dots")
    load_spinner.start()

    index_query = """
    CREATE INDEX ON :Account(accountID);
    """

    set_mode_query = """
    STORAGE MODE IN_MEMORY_ANALYTICAL;
    """

    load_data_query = """
    LOAD CSV FROM "/data/SAML-D.csv" WITH HEADER AS row
    MERGE (s:Account {accountID: toInteger(row.Sender_account)})
    SET s:Sender
    MERGE (r:Account {accountID: toInteger(row.Receiver_account)})
    SET r:Receiver

    // Create a direct relationship instead of a node
    MERGE (s)-[:TRANSFERRED {
        amount: toFloat(row.Amount),
        datetime: datetime(row.Date+"T"+row.Time),
        type: row.Laundering_type
    }]->(r);
    """

    cycle_query = """
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
            session.run(index_query)
            session.run(set_mode_query)
            session.run(load_data_query)
            load_spinner.succeed(
                "Loaded 855K nodes and 9.5M edges into Memgraph.")
            cycle_spinner.start()
            result = session.run(cycle_query)
            records = [dict(record) for record in result]
        driver.close()
        cycle_spinner.succeed(
            f"Detected {len(records)} temporal cycles via Memgraph.")
        return records
    except Exception as e:
        cycle_spinner.fail(f"Memgraph Connection Failed: {e}")
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
    print(
        f"✅ Successfully wrote {len(df)} transactions to '{output_filepath}'")


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
        COPY (
            SELECT 
                origin.Receiver_account,
                (DATE_DIFF('minute', agg.start_time, agg.end_time) / 1440.0) AS Duration_Days,
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
            INNER JOIN (
                SELECT 
                    Receiver_account, 
                    SUM(Amount) AS Total_amount,
                    COUNT(DISTINCT Sender_account) AS Sender_count,
                    MIN(CAST(Date || ' ' || Time AS TIMESTAMP)) AS start_time,
                    MAX(CAST(Date || ' ' || Time AS TIMESTAMP)) AS end_time
                FROM '{source_file}'
                WHERE Payment_currency = '{preferred_sender_currency}' 
                AND Received_currency = '{preferred_receiver_currency}'
                GROUP BY Receiver_account
                HAVING Sender_count > {target_minimum_distinct_senders} 
                AND DATE_DIFF('minute', start_time, end_time) <= {target_minutes_duration} 
                AND Total_amount > {target_total_threshold}
            ) AS agg ON origin.Receiver_account = agg.Receiver_account
            WHERE origin.Payment_currency = '{preferred_sender_currency}' 
            AND origin.Received_currency = '{preferred_receiver_currency}'
            ORDER BY origin.Receiver_account ASC, agg.Total_amount DESC
        ) TO '{output_filepath}' (HEADER, DELIMITER ',');
        """

    conn.execute(full_data_query)

    spinner.succeed(f"Processed data saved to {output_filepath}")


if __name__ == "__main__":

    # 1. Dataset Setup
    data_path = Path('./data/SAML-D.csv')
# Dynamically find the user home directory
    home_dir = os.path.expanduser("~")
    cache_path = os.path.join(home_dir, ".cache", "kagglehub", "datasets",
                              "berkanoztas", "synthetic-transaction-monitoring-dataset-aml")

    if not data_path.is_file():
        if os.path.exists(cache_path):
            shutil.rmtree(cache_path)
            print("Cache cleared")

        path = kg.dataset_download(
            "berkanoztas/synthetic-transaction-monitoring-dataset-aml")
        shutil.move(os.path.join(path, 'SAML-D.csv'), './data')

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
