import gc
import duckdb
from typing import List, Dict, Set
from duckdb import DuckDBPyConnection
from tqdm import tqdm
from halo import Halo
import pandas as pd


def trace_cycles(transactions: Dict[str, List], current_account: str, start_account: str, start_amount: float,
                 visited_accounts: Set, path: List[Dict], tx_date: str, tx_time: str, max_depth: int = 100) -> List[Dict]:
    """
    Docstring for trace_cycles
    This function traverses the transactions graph using a Depth-First-Search (DFS) algorithm to detect a cyclical 
    transaction pattern, one of the many typologies money launderers use to hide their tracks.

    Parameters:
    1. transactions (Dict[str, List]): The graph containing transactions as edges and senders as nodes.
    2. current_account (str): The receiver account of the first transaction; further transactions are searched for using this value.
    3. start_account (str): Sender account of the transaction, which, when matched with the final transaction's receiver, a cycle is formed.
    4. path (List): Contains all the edges of a traversal.
    5. tx_date (str): Date of `current_account` transaction.
    6. tx_time: Time of `current_account` transaction.
    """

    # Failure Base Case = No transactions for this sender in graph
    if current_account not in transactions:
        return None

    # Custom Failure Case = Decide length of cycles to be detected
    if len(path) > max_depth:
        return None

    # All transactions sent by the sender
    all_hops = transactions[current_account]

    for next_transaction in all_hops:
        # Ensure that the transaction being looked at happened after the current_account's transaction
        if next_transaction['Date'] > tx_date or \
           (next_transaction['Date'] == tx_date and next_transaction['Time'] > tx_time):
            new_path = path + [next_transaction]
            next_receiver = next_transaction['Receiver_account']
            next_amount = float(next_transaction['Amount'])

            if next_receiver != start_account and next_receiver in visited_accounts:
                continue  # Skip this hop, go to the next transaction in all_hops

            # Success Case: Cycle detected, transactions appended, end recursion
            if next_receiver == start_account and len(new_path) > 2:
                twenty_of_start_amount = (start_amount * 0.20)
                start_amount_twenty_plus = start_amount + twenty_of_start_amount
                start_amount_twenty_minus = start_amount - twenty_of_start_amount

                if start_amount_twenty_minus <= next_amount <= start_amount_twenty_plus:
                    return new_path
                else:
                    continue
            new_visited_accounts = visited_accounts.copy()
            new_visited_accounts.add(current_account)
            result = trace_cycles(
                transactions, next_receiver, start_account, start_amount, new_visited_accounts, new_path, next_transaction['Date'], next_transaction['Time'])

            if result:
                return result


def build_graph_cycle(conn: DuckDBPyConnection, source_file: str = "./data/SAML-D.csv") -> Dict[str, List]:
    """
    Docstring for build_graph
    This function queries the relevant CSV using DuckDB to get all transactions' data. It then builds
    a Dictionary using string keys and List[Dict] values indicating Senders-Transactions data that it returns.

    Parameters:
    1. conn (DuckDBPyConnection): The connection object to DuckDB

    """
    spinner = Halo(text="Retrieving data from CSV...",
                   color="magenta", spinner="dots2")
    spinner.start()
    # Retrieve all necessary transaction data from csv
    full_data_query = \
        f"""
        SELECT Sender_account, list({{
                                    'Date': Date,
                                    'Time': Time, 
                                    'Sender_account': Sender_account,
                                    'Receiver_account': Receiver_account, 
                                    'Amount': Amount, 
                                    'Laundering_type': Laundering_type}} ORDER BY Date ASC, Time ASC) as transactions
        FROM '{ source_file }'
        GROUP BY Sender_account;
    """

    data = conn.execute(full_data_query).fetchall()

    spinner.succeed("Retrieved data from CSV")

    spinner = Halo("Converting the data to a graph format...",
                   "yellow", spinner="dots12")
    spinner.start()

    full_graph: Dict[str, List] = {
        Sender_account: transactions for Sender_account, transactions in data
    }

    spinner.succeed(
        f"Graph built with {len(full_graph)} unique accounts as senders.")
    return full_graph


def write_cycles_to_csv(cycles: List[List[Dict]], output_filepath: str = './data/detected_cycles.csv'):
    """
    Flattens the nested cycles data structure and writes it to a CSV file.

    Parameters:
    1. cycles (List[List[Dict]]): The list of detected cycles.
    2. output_filepath (str): The name of the file to write the results to.
    """
    if not cycles:
        print("No cycles were detected. Skipping CSV export.")
        return

    spinner = Halo(text="Writing cycles to a file...",
                   color="red", spinner="dots4")
    spinner.start()

    flattened_data = []

    for cycle_index, cycle_path in enumerate(cycles):
        for transaction in cycle_path:
            # Create a copy of the transaction dictionary
            tx_row = transaction.copy()

            # Add metadata columns to identify the cycle and its position
            tx_row['Cycle_ID'] = cycle_index + 1
            tx_row['Cycle_Length'] = len(cycle_path)
            tx_row['Hop_Number'] = cycle_path.index(transaction) + 1

            flattened_data.append(tx_row)

    # 1. Convert the list of dictionaries into a Pandas DataFrame
    cycles_df = pd.DataFrame(flattened_data)

    # 2. Write the DataFrame to a CSV file
    cycles_df.to_csv(output_filepath, index=False)

    spinner.succeed(f"\n✅ Successfully wrote {len(cycles)} cycles "
                    f"({len(flattened_data)} total transactions) to '{output_filepath}'")


def detect_smurfing_suspects(conn: DuckDBPyConnection, source_file: str = "./data/SAML-D.csv", output_filepath: str = "./data/smurfing_suspects.csv") -> Dict[str, List]:
    spinner = Halo(text="Retrieving and processing data from CSV...",
                   color="magenta", spinner="dots2")
    spinner.start()
    # Retrieve all necessary transaction data from csv
    full_data_query = \
        f"""
        ;

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
            FROM '{ source_file }' AS origin
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
                    FROM '{ source_file }'
                    WHERE Payment_currency = 'UK pounds' AND Received_currency = 'UK pounds'
                    GROUP BY Receiver_account
                        HAVING Sender_count > 10 AND Duration <= 43200 AND Total_amount > 100000) AS agg
                ON origin.Receiver_account = agg.Receiver_account
            WHERE Payment_currency = 'UK pounds' AND Received_currency = 'UK pounds'
            ORDER BY origin.Receiver_account ASC, agg.Total_amount ASC, agg.Duration ASC
            ) TO '{ output_filepath }' (HEADER, DELIMITER ',');
    """

    conn.execute(full_data_query)

    spinner.succeed(f"Processed data saved to { output_filepath }")


if __name__ == "__main__":
    conn = duckdb.connect()
    graph = build_graph_cycle(conn)

    # cycles stores all detected cycles; discovered_transactions stores all sender_ids part of a cycle to avoid re-checking
    cycles = []
    discovered_transactions = set()

    for sender_id, transactions in tqdm(graph.items(), "Going through potential cycle starters"):
        if sender_id not in discovered_transactions:
            discovered_transactions.add(sender_id)
            cycle = trace_cycles(graph, transactions[0]['Receiver_account'], sender_id, float(transactions[0]['Amount']),
                                 {sender_id}, [transactions[0]], transactions[0]['Date'], transactions[0]['Time'])

            if cycle:
                print(
                    f"CYCLE FOUND: {cycle[0]['Sender_account']} -> ... -> {cycle[-1]['Receiver_account']} \
                        of length {len(cycle)}")
                cycles.append(cycle)

                for tx in cycle:
                    discovered_transactions.add(tx['Sender_account'])

    print(f"Total cycles found: {len(cycles)}")
    write_cycles_to_csv(cycles)

    del graph
    del discovered_transactions
    del cycles
    gc.collect()

    detect_smurfing_suspects(conn)
    conn.close()
