import requests
import csv
import time
from datetime import datetime
import os
from dotenv import load_dotenv

# ========= 配置 =========
load_dotenv()

# 从环境变量获取 API Key
API_KEY = os.getenv("API_KEY")

# 输入的地址 CSV（每行一个地址，带表头 "addressName"）
input_csv = "tron_address_withdraw.csv"

# 输出交易 CSV
output_csv = "tron_txs.csv"

# 时间窗口
start_time = datetime(2025, 8, 9, 0, 0)
end_time = datetime(2025, 8, 12, 0, 0)

start_ts = int(start_time.timestamp() * 1000)  # TronGrid 用毫秒
end_ts = int(end_time.timestamp() * 1000)

PAGE_SIZE = 200  # TronGrid 单页上限

# 查询模式： "trx" or "trc20"
mode = "trc20"  # 默认 TRC20


# ========= 工具函数 =========

def fetch_trongrid_txs(addr, start_ts, end_ts, mode="trc20"):
    """
    从 TronGrid API 抓取指定地址的交易（支持翻页）
    """
    if mode == "trx":
        url = f"https://api.trongrid.io/v1/accounts/{addr}/transactions"
        parse_func = parse_trx
    else:
        url = f"https://api.trongrid.io/v1/accounts/{addr}/transactions/trc20"
        parse_func = parse_trc20

    headers = {"TRON-PRO-API-KEY": API_KEY}
    params = {"limit": PAGE_SIZE, "min_timestamp": start_ts, "max_timestamp": end_ts}

    all_txs = []
    while True:
        try:
            r = requests.get(url, params=params, headers=headers, timeout=10)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"[Error] Fetch {addr} failed: {e}")
            break

        txs = data.get("data", [])
        if not txs:
            break

        for tx in txs:
            parsed = parse_func(tx)
            if parsed:
                all_txs.append(parsed)

        # 翻页用 fingerprint
        meta = data.get("meta", {})
        if "fingerprint" in meta and meta["fingerprint"]:
            params["fingerprint"] = meta["fingerprint"]
            time.sleep(0.2)  # 防止过快
        else:
            break

    return all_txs


def format_time(ts):
    """毫秒时间戳 → yyyy-mm-dd HH:MM:SS"""
    return datetime.utcfromtimestamp(int(ts) / 1000).strftime("%Y-%m-%d %H:%M:%S")


def parse_trx(tx):
    try:
        contract = tx["raw_data"]["contract"][0]
        val = contract["parameter"]["value"]
        from_addr = val.get("owner_address")
        to_addr = val.get("to_address")
        amount_sun = val.get("amount", 0)  # Sun
        amount_trx = int(amount_sun) / 1e6  # 转换成 TRX
        ts = tx["block_timestamp"]
        txid = tx.get("txID", "")
        return from_addr, to_addr, amount_trx, format_time(ts), "TRX", txid
    except Exception as e:
        print(f"[ParseError][TRX] {tx.get('txID', '?')} skipped: {e}")
        return None


def parse_trc20(tx):
    try:
        from_addr = tx.get("from")
        to_addr = tx.get("to")
        value = tx.get("value", "0")
        ts = tx.get("block_timestamp")
        txid = tx.get("transaction_id", "")

        token_info = tx.get("token_info", {})
        decimals = int(token_info.get("decimals", 6))  # 默认 6
        symbol = token_info.get("symbol", "UNKNOWN")

        # 转换成人类可读金额
        amount = int(value) / (10 ** decimals)

        return from_addr, to_addr, amount, format_time(ts), symbol, txid
    except Exception as e:
        print(f"[ParseError][TRC20] {tx.get('transaction_id', '?')} skipped: {e}")
        return None


# ========= 主程序 =========

def main():
    with open(input_csv, "r") as f:
        reader = csv.DictReader(f)
        addresses = [row["addressName"].strip() for row in reader]

    print(f"Loaded {len(addresses)} addresses from {input_csv}")

    with open(output_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["from", "to", "amount", "timestamp", "token", "txid"])

        for addr in addresses:
            print(f"Fetching {mode.upper()} txs for {addr} ...")
            txs = fetch_trongrid_txs(addr, start_ts, end_ts, mode=mode)

            for from_addr, to_addr, amount, ts, token, txid in txs:
                writer.writerow([from_addr, to_addr, amount, ts, token, txid])

    print(f"✅ Done! All {mode.upper()} transactions saved in {output_csv}")


if __name__ == "__main__":
    main()
