# AML Forensic Detection: Graph Based Typology Parsing
This project focuses on detecting sophisticated money laundering patterns like Cycling and Smurfing using graph traversal techniques on synthetic transaction data.

## Tech Stack and Evolution
* Database: DuckDB for high performance analytical queries on the SAML-D dataset.

* Engine: In-memory graph construction and a Depth First Search (DFS) algorithm.

* Visualization: Power BI Forensic Suite.

## Development Journey
Initially, I attempted to process this huge 9M+ rows dataset using PySpark. However, I found that the overhead of Spark’s distributed architecture was not the right tool for the specific recursive nature of "Cycle Detection" on this dataset.

I pivoted to a DuckDB + DFS approach. The script retrieves transactions into an in-memory graph and a DFS algorithm identifies paths that loop back to the originator.

Roadmap: To improve performance and handle deeper "hops" I am currently migrating the logic to a Neo4j implementation. This will replace the manual DFS with native graph query language (Cypher) for faster pattern matching.

The Smurfing analysis was unified within a DuckDB query highlighting the versatility and speed that DuckDB offers.

Forensic Visualizations
The dashboard consists of 2 distinct investigative views designed to switch between an analysis of Smurfing and Cycling typologies.

1. The Cycling Suite (Round Tripping)
Waterfall Cycle Chart: This visualizes the capital flow across intermediaries. It tracks how money is moved through multiple hops before returning to the source to obscure investigations regarding source of funds.

![Cycling Waterfall Chart](./images/Cycle.png)

Hover Detail Overlay: A dynamic tooltip view that reveals specific transaction details like Amounts, Senders, and Receivers when hovering over any bar in the waterfall.

![Cycling Hover details snapshot](./images/Cycle_Hover.png)

2. The Smurfing Suite (Structuring)
Decomposition Tree: A root cause analysis tool that breaks down a "Final Receiver" account to reveal the dozens of small fragmented "Smurf" accounts feeding it via multiple payment methods in short durations.

![Smurfing Analysis - Waterfall Chart](./images/Smurf.png)

Smurf Hover Analysis: An investigative overlay providing the payment type (ACH, Debit, etc.) and specific amounts for each branch in the decomposition tree.

![Smurfing Analysis - Hover Overview snapshot](./images/Smurf_Hover.png)

Dataset Credits
This project utilizes the [SAML-D (Synthetic Anti-Money Laundering) dataset from Kaggle](https://www.kaggle.com/datasets/berkanoztas/synthetic-transaction-monitoring-dataset-aml/data).

Citation: B. Oztas, D. Cetinkaya, F. Adedoyin, M. Budka, H. Dogan and G. Aksu, "Enhancing Anti-Money Laundering: Development of a Synthetic Transaction Monitoring Dataset," 2023 IEEE International Conference on e-Business Engineering (ICEBE), Sydney, Australia, 2023, pp. 47-54, doi: 10.1109/ICEBE59045.2023.00028.

How to use
Python Script: Run the DFS parser to generate the processed CSVs.

Power BI: Load the generated files into the .pbix template to refresh the Waterfall and Decomposition visuals.