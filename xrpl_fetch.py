import asyncio
import json
import csv
import websockets
import pandas as pd

XRP_WS_URL = "wss://s1.ripple.com/"
LEDGER_COUNT = 10
CSV_FILE = "xrp_transactions.csv"

async def fetch_ledger_transactions(ledger_index):
    """Fetch transactions from a given ledger index."""
    async with websockets.connect(XRP_WS_URL) as websocket:
        # Send ledger request
        ledger_request = {
            "command": "ledger",
            "ledger_index": ledger_index,
            "transactions": True,
            "expand": True
        }
        await websocket.send(json.dumps(ledger_request))
        response = await websocket.recv()
        ledger_data = json.loads(response)

        transactions = ledger_data.get("result", {}).get("ledger", {}).get("transactions", [])
        timestamp = ledger_data.get("result", {}).get("ledger", {}).get("close_time")

        # Convert Ripple timestamp (seconds since 2000) to readable format
        close_time = pd.to_datetime(timestamp + 946684800, unit='s')

        tx_list = []
        for tx in transactions:
            tx_hash = tx.get("hash")
            sender = tx.get("Account")
            receiver = tx.get("Destination", "XRPL")
            value = tx.get("Amount")
            status = 0 if tx.get("meta", {}).get("TransactionResult") == "tesSUCCESS" else 1

            # Normalize XRP value (some transactions might have nested Amount fields)
            if isinstance(value, dict):
                value = value.get("value", "0")

            tx_list.append([tx_hash, ledger_index, close_time, sender, receiver, value, status])

        return tx_list

async def main():
    """Fetch transactions from multiple ledgers and save to CSV."""
    start_ledger = 94678883  # Replace this with the latest ledger index dynamically
    all_transactions = []

    for i in range(LEDGER_COUNT):
        ledger_index = start_ledger + i
        print(f"Fetching transactions from Ledger {ledger_index}...")
        tx_data = await fetch_ledger_transactions(ledger_index)
        all_transactions.extend(tx_data)

    # Save data to CSV
    with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["TxHash", "BlockHeight", "TimeStamp", "From", "To", "Value", "isError"])
        writer.writerows(all_transactions)

    print(f"✅ XRP transaction dataset saved to {CSV_FILE} with {len(all_transactions)} transactions.")

if __name__ == "__main__":
    asyncio.run(main())
