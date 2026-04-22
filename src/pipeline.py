from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from contracts import migrate_v1_to_v2, utc_now, validate_record

BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_DIR = BASE_DIR / "data" / "input"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def read_cards() -> List[Dict]:
    df = pd.read_csv(INPUT_DIR / "cards.csv")
    records = []
    for _, r in df.iterrows():
        records.append({
            "event_id": f"card_{r['txn_id']}",
            "contract_version": "v2",
            "payment_type": "CARD",
            "customer_id": r["cust_id"],
            "amount": r["amt_aed"],
            "currency": r["ccy"],
            "event_timestamp": r["txn_ts"],
            "status": str(r["txn_status"]).upper(),
            "payment_method": "DEBIT_CARD" if r["card_type"] == "debit" else "CREDIT_CARD",
            "counterparty_reference": r["merchant_ref"],
            "merchant_name": r["merchant_name"],
            "biller_name": None,
            "source_account_id": r["account_id"],
            "destination_account_id": None,
            "card_last4": str(r["card_pan_masked"])[-4:],
            "channel": str(r["channel"]).upper(),
            "country_code": r["country"],
            "is_international": str(r["country"]).upper() != "AE",
            "raw_source": "cards",
            "ingestion_ts": utc_now(),
        })
    return records


def read_transfers() -> List[Dict]:
    df = pd.read_csv(INPUT_DIR / "transfers.csv")
    records = []
    for _, r in df.iterrows():
        v1 = {
            "event_id": f"trf_{r['transfer_uuid']}",
            "contract_version": "v1",
            "payment_type": "TRANSFER",
            "customer_id": r["user_id"],
            "amount": r["amount_value"],
            "currency": r["currency_code"],
            "event_timestamp": r["created_at"],
            "status": str(r["state"]).upper(),
            "payment_method": str(r["rail"]).upper(),
            "counterparty_reference": r["beneficiary_ref"],
            "merchant_name": None,
            "biller_name": None,
            "source_account_id": r["from_account"],
            "destination_account_id": r["to_account"],
            "card_last4": None,
            "raw_source": "transfers",
            "ingestion_ts": utc_now(),
        }
        records.append(migrate_v1_to_v2(v1))
    return records


def read_bills() -> List[Dict]:
    df = pd.read_csv(INPUT_DIR / "bill_payments.csv")
    records = []
    for _, r in df.iterrows():
        records.append({
            "event_id": f"bill_{r['payment_id']}",
            "contract_version": "v2",
            "payment_type": "BILL_PAYMENT",
            "customer_id": r["customer_no"],
            "amount": r["bill_amount"],
            "currency": r["currency"],
            "event_timestamp": r["paid_on"],
            "status": str(r["payment_status"]).upper(),
            "payment_method": str(r["payment_method"]).upper(),
            "counterparty_reference": r["invoice_no"],
            "merchant_name": None,
            "biller_name": r["biller"],
            "source_account_id": r["debit_account"],
            "destination_account_id": r["creditor_account"],
            "card_last4": None,
            "channel": str(r["channel_name"]).upper(),
            "country_code": "AE",
            "is_international": False,
            "raw_source": "bill_payments",
            "ingestion_ts": utc_now(),
        })
    return records


def validate_and_split(records: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    good, bad = [], []
    for idx, record in enumerate(records, start=1):
        errors = validate_record(record, idx, record["raw_source"])
        if errors:
            for e in errors:
                bad.append({
                    "source": e.source,
                    "row_number": e.row_number,
                    "reason": e.reason,
                    "record": e.record,
                })
        else:
            good.append(record)
    return good, bad


def write_outputs(valid_records: List[Dict], errors: List[Dict]) -> None:
    df = pd.DataFrame(valid_records).sort_values(["event_timestamp", "event_id"])
    df.to_parquet(OUTPUT_DIR / "unified_payments.parquet", index=False)
    df.to_json(OUTPUT_DIR / "unified_payments.json", orient="records", indent=2)
    summary = {
        "valid_record_count": len(valid_records),
        "error_count": len(errors),
        "by_payment_type": df["payment_type"].value_counts().to_dict(),
        "by_status": df["status"].value_counts().to_dict(),
    }
    (OUTPUT_DIR / "run_summary.json").write_text(json.dumps(summary, indent=2))
    with open(OUTPUT_DIR / "validation_errors.jsonl", "w", encoding="utf-8") as f:
        for err in errors:
            f.write(json.dumps(err) + "\n")


def main() -> None:
    all_records = read_cards() + read_transfers() + read_bills()
    valid_records, errors = validate_and_split(all_records)
    write_outputs(valid_records, errors)
    print(f"Loaded {len(all_records)} records | valid={len(valid_records)} | errors={len(errors)}")
    print(f"Outputs written to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
