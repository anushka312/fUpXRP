import asyncio
import json
import csv
import websockets
import pandas as pd

XRP_WS_URL = "wss://s1.ripple.com/"
LEDGER_COUNT = 10
CSV_FILE = "xrp_dataset.csv"

async def get_latest_ledger_index():
    async with websockets.connect(XRP_WS_URL) as websocket:
        await websocket.send(json.dumps({"command": "ledger_current"}))
        response = await websocket.recv()
        data = json.loads(response)
        return data.get("result", {}).get("ledger_current_index")

async def fetch_ledger_transactions(ledger_index):
    async with websockets.connect(XRP_WS_URL) as websocket:
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

        if timestamp is not None:
            close_time = pd.to_datetime(timestamp + 946684800, unit='s')
        else:
            close_time = "Unknown"

        tx_list = []
        for tx in transactions:
            tx_hash = tx.get("hash")
            sender = tx.get("Account")
            receiver = tx.get("Destination", "XRPL")
            value = tx.get("Amount", 0)

            meta = tx.get("meta") or tx.get("metaData")
            tx_result = meta.get("TransactionResult") if meta else None
            status = 0 if tx_result == "tesSUCCESS" else 1

            if isinstance(value, dict):
                value = value.get("value", "0")

            try:
                value = float(str(value).replace(",", ""))
            except ValueError:
                value = 0.0

            tx_list.append([tx_hash, ledger_index, close_time, sender, receiver, value, status])

        return tx_list

async def main():
    latest_ledger = await get_latest_ledger_index()
    print(f"Latest validated ledger index: {latest_ledger}")

    all_transactions = []
    for i in range(LEDGER_COUNT):
        ledger_index = latest_ledger - i  # Fetch the latest N ledgers dynamically
        print(f" Fetching transactions from Ledger {ledger_index}...")

        try:
            tx_data = await fetch_ledger_transactions(ledger_index)
            all_transactions.extend(tx_data)
        except Exception as e:
            print(f"Error fetching transactions for Ledger {ledger_index}: {e}")

    # Save data to CSV
    with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["TxHash", "BlockHeight", "TimeStamp", "From", "To", "Value", "isError"])
        writer.writerows(all_transactions)

    print(f" XRP transaction dataset saved to {CSV_FILE} with {len(all_transactions)} transactions.")

if __name__ == "__main__":
    asyncio.run(main())
