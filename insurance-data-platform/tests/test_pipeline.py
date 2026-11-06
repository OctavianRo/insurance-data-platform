import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from pipeline import ingest, validate

class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.db=sqlite3.connect(':memory:')
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)/'input.json'
        self.row=dict(policy_id='P1',entity='IE',currency='EUR',as_of='2026-10-31',reserve='12.34',customer_id='private')
    def tearDown(self):
        self.db.close(); self.tmp.cleanup()
    def run_batch(self,rows,batch='b1'):
        self.path.write_text(json.dumps(rows))
        return ingest(self.db,self.path,batch,b'x'*32)
    def test_replay_does_not_double_count(self):
        self.run_batch([self.row]); result=self.run_batch([self.row])
        self.assertTrue(result['replayed'])
        self.assertEqual(self.db.execute('SELECT reserve_cents FROM reporting').fetchone()[0],1234)
    def test_changed_replay_rejected(self):
        self.run_batch([self.row])
        with self.assertRaises(ValueError):self.run_batch([])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM policies').fetchone()[0],1)
    def test_quarantine_and_reconciliation(self):
        result=self.run_batch([self.row,{**self.row,'policy_id':'P2','reserve':'NaN'},self.row])
        self.assertEqual((result['accepted'],result['rejected']),(1,2))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM bronze').fetchone()[0],3)
    def test_pseudonymisation(self):
        self.run_batch([self.row])
        token=self.db.execute('SELECT customer_token FROM policies').fetchone()[0]
        self.assertNotIn('private',token); self.assertEqual(len(token),64)
    def test_snapshot_order(self):
        self.run_batch([self.row])
        self.assertEqual(self.run_batch([{**self.row,'as_of':'2026-09-30'}],'b2')['rejected'],1)
        self.run_batch([{**self.row,'as_of':'2026-11-30','reserve':'15.00'}],'b3')
        self.assertEqual(self.db.execute('SELECT reserve_cents FROM reporting').fetchone()[0],1500)
    def test_invalid_contracts(self):
        for change in ({'reserve':'1.001'},{'currency':'ZZZ'},{'as_of':'2026-02-30'},{'reserve':'Infinity'},{'reserve':'-1'}):
            with self.subTest(change=change),self.assertRaises(ValueError):validate({**self.row,**change})
    def test_transaction_rollback(self):
        self.run_batch([self.row])
        self.db.execute("CREATE TRIGGER fail_insert BEFORE INSERT ON policies BEGIN SELECT RAISE(ABORT,'failure'); END;")
        with self.assertRaises(sqlite3.IntegrityError):self.run_batch([{**self.row,'policy_id':'P2'}],'b2')
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM bronze WHERE batch_id='b2'").fetchone()[0],0)

if __name__=='__main__':unittest.main()
