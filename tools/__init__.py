"""tools — Dataset preprocessing utilities.

Scripts for normalizing, time-compressing, and analyzing datasets
for the Distributed Watermark Tracker engine.

Scripts:
    nyc_taxi_to_events.py  — Convert raw NYC TLC yellow_tripdata to engine schema (DIV=60)
    make_out_of_order.py   — Inject controlled out-of-order lateness into a CSV
    dataset_stats.py       — Print comprehensive statistics for a dataset CSV
"""
