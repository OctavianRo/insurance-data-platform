"""Transactional insurance reporting reference implementation. Synthetic data only."""
import argparse
import hashlib
import hmac
import json
import os
import sqlite3
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

SCHEMA = '''
CREATE TABLE IF NOT EXISTS batches (batch_id TEXT PRIMARY KEY, digest TEXT NOT NULL, accepted INTEGER, rejected INTEGER);
CREATE TABLE IF NOT EXISTS bronze (batch_id TEXT, row_no INTEGER, payload TEXT, PRIMARY KEY(batch_id,row_no));
CREATE TABLE IF NOT EXISTS quarantine (batch_id TEXT, row_no INTEGER, reason TEXT, PRIMARY KEY(batch_id,row_no));
CREATE TABLE IF NOT EXISTS policies (policy_id TEXT PRIMARY KEY, entity TEXT, currency TEXT, as_of TEXT, reserve_cents INTEGER, customer_token TEXT, batch_id TEXT);
CREATE VIEW IF NOT EXISTS reporting AS SELECT entity,currency,as_of,COUNT(*) policy_count,SUM(reserve_cents) reserve_cents FROM policies GROUP BY entity,currency,as_of;
'''
FIELDS = {'policy_id','entity','currency','as_of','reserve','customer_id'}


def validate(row):
    if not isinstance(row, dict) or set(row) != FIELDS:
        raise ValueError('schema_mismatch')
    if any(not isinstance(v,str) or not v.strip() for v in row.values()):
        raise ValueError('nonempty_strings_required')
    if row['currency'] not in {'EUR','GBP','USD'}:
        raise ValueError('unsupported_currency')
    try:
        if date.fromisoformat(row['as_of']).isoformat() != row['as_of']:
            raise ValueError()
    except ValueError:
        raise ValueError('invalid_date')
    try:
        reserve = Decimal(row['reserve'])
        if not reserve.is_finite() or reserve < 0 or reserve > Decimal('1000000000000') or reserve != reserve.quantize(Decimal('.01')):
            raise ValueError()
    except (InvalidOperation, ValueError):
        raise ValueError('invalid_reserve')
    return int(reserve * 100)


def ingest(db, source, batch_id, key):
    if len(key) < 32:
        raise ValueError('token key must contain at least 32 bytes')
    raw = Path(source).read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    rows = json.loads(raw)
    if not isinstance(rows,list):
        raise ValueError('source must be an array')
    db.executescript(SCHEMA)
    db.execute('BEGIN IMMEDIATE')
    try:
        existing = db.execute('SELECT digest,accepted,rejected FROM batches WHERE batch_id=?',(batch_id,)).fetchone()
        if existing:
            if existing[0] != digest:
                raise ValueError('batch_id already used with different content')
            db.rollback()
            return {'accepted':existing[1], 'rejected':existing[2], 'replayed':True}
        accepted = rejected = 0
        seen = set()
        for index,row in enumerate(rows):
            db.execute('INSERT INTO bronze VALUES (?,?,?)',(batch_id,index,json.dumps(row,sort_keys=True)))
            try:
                cents = validate(row)
                if row['policy_id'] in seen:
                    raise ValueError('duplicate_policy_in_batch')
                current = db.execute('SELECT as_of FROM policies WHERE policy_id=?',(row['policy_id'],)).fetchone()
                if current and row['as_of'] <= current[0]:
                    raise ValueError('non_increasing_snapshot')
                seen.add(row['policy_id'])
            except ValueError as exc:
                db.execute('INSERT INTO quarantine VALUES (?,?,?)',(batch_id,index,str(exc)))
                rejected += 1
                continue
            token = hmac.new(key,row['customer_id'].encode(),hashlib.sha256).hexdigest()
            db.execute('INSERT OR REPLACE INTO policies VALUES (?,?,?,?,?,?,?)',
                       (row['policy_id'],row['entity'],row['currency'],row['as_of'],cents,token,batch_id))
            accepted += 1
        db.execute('INSERT INTO batches VALUES (?,?,?,?)',(batch_id,digest,accepted,rejected))
        db.commit()
        return {'accepted':accepted,'rejected':rejected,'replayed':False}
    except Exception:
        db.rollback()
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source'); parser.add_argument('--batch',required=True)
    parser.add_argument('--db',default='warehouse.db')
    args = parser.parse_args()
    key = os.environ.get('TOKEN_KEY','').encode()
    with sqlite3.connect(args.db) as db:
        print(json.dumps(ingest(db,args.source,args.batch,key),sort_keys=True))
        print(json.dumps(db.execute('SELECT * FROM reporting ORDER BY entity,currency,as_of').fetchall()))

if __name__ == '__main__':
    main()
