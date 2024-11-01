import pytest
import pandas as pd
import io
import time
import logging
import pickletools
import pickle

from distributed_notebook.sync.object import SyncObjectWrapper
from distributed_notebook.sync.log_referer import SyncLogReferer as SyncReferer
# from distributed_notebook.sync.log_referer import SyncReferer
from distributed_notebook.sync.object import SyncObjectMeta
from distributed_notebook.sync.log import SyncValue, OP_SYNC_ADD

example_data = {
    'Name': ['Alice', 'Bob', 'Charlie', 'Dave'],
    'Age': [25, 32, 22, 45],
    'City': ['New York', 'Los Angeles', 'Chicago', 'Miami'],
    'Salary': [55000, 74000, 48000, 66000],
}

class TestSyncObjectWrapper:
  @pytest.fixture(autouse=True)
  def setup_class(self):
    self.sync_object = SyncObjectWrapper(SyncReferer())
    print("setup")

  # def test_init_dataframe(self):
  #   raw = pd.DataFrame(example_data)
  #   meta = SyncObjectMeta(str(1))
  #   self.sync_object.config_pickler(profiling=True)
  #   result = self.sync_object.diff(raw, meta)
  #   # Check that the result is as expected
  #   assert result != None
  #   assert result.op == OP_SYNC_ADD
  #   assert self.sync_object.raw is not None
  #   # result.val = pickletools.optimize(result.val)
  #   # logging.info(result.val)

  def test_restore_dataframe(self):
    sync_object = SyncObjectWrapper(SyncReferer())
    sync_object.config_pickler(profiling=True)
    val = sync_object.diff(pd.DataFrame(example_data), SyncObjectMeta(str(1)))
    assert val != None
    # val.val = pickletools.optimize(val.val)

    result = self.sync_object.update(val)
    # Check that the result is as expected
    assert result is not None
    assert isinstance(result, pd.DataFrame)

  # def test_diff_dataframe(self):
  #   df = pd.read_csv("distributed_notebook/demo/script/data1.csv")
  #   self.sync_object.config_pickler(profiling = True)
  #   val = self.sync_object.diff(df, SyncObjectMeta(str(1)))
  #   assert val != None

  # def test_dump_dataframe(self, capfd):
  #   raw = pd.DataFrame(example_data)
  #   batch = 1
  #   meta = SyncObjectMeta(str(1))
  #   initialized = self.sync_object.diff(raw, meta)
  #   assert initialized != None
  #   iterations = 10000
  #   result: bytes = b''

  #   batch = batch + 1
  #   meta.batch = str(batch)
  #   dumped = self.sync_object.dump(meta)
  #   assert len(initialized.val) == len(dumped.val)
  #   assert initialized.val == dumped.val

  #   csvBuff = io.BytesIO()
  #   raw.to_csv(csvBuff, index=True)
  #   csv = csvBuff.getvalue()

  #   print("benchmarking {} iterations:".format(iterations))

  #   start = time.time()
  #   for i in range(iterations):
  #     buff = io.BytesIO()
  #     raw.to_csv(buff, index=True)
  #     result = buff.getvalue()
  #   print("to_csv got {} ops, size {}".format(iterations/(time.time() - start), len(result)))

  #   start = time.time()
  #   for i in range(iterations):
  #     df = pd.read_csv(io.BytesIO(csv))
  #   print("read_csv got {} ops".format(iterations/(time.time() - start)))

  #   start = time.time()
  #   for i in range(iterations):
  #     batch = batch + 1
  #     meta.batch = str(batch)
  #     ret = self.sync_object.dump(meta)
  #     result = ret.val
  #   print("dump with CPython got {} ops, size {}".format(iterations/(time.time() - start), len(result)))

  #   start = time.time()
  #   for i in range(iterations):
  #     sync_object = SyncObjectWrapper(SyncReferer())
  #     sync_object.update(dumped)
  #   print("restore with CPython got {} ops".format(iterations/(time.time() - start)))

  #   # Configure pure Python pickler
  #   self.sync_object.config_pickler(pickle._Pickler, pickle._Unpickler)
  #   start = time.time()
  #   for i in range(iterations):
  #     batch = batch + 1
  #     meta.batch = str(batch)
  #     ret = self.sync_object.dump(meta)
  #     result = ret.val
  #   print("dump with Python got {} ops, size {}".format(iterations/(time.time() - start), len(result)))

  #   start = time.time()
  #   for i in range(iterations):
  #     sync_object = SyncObjectWrapper(SyncReferer())
  #     sync_object.update(dumped)
  #   print("restore with Python got {} ops".format(iterations/(time.time() - start)))

  #   # Capture the output
  #   out, _ = capfd.readouterr()
  #   logging.info(out)

  # def test_dump(self):
  #   meta = None  # substitute this with actual metadata if needed
  #   result = self.sync_object.dump(meta)
  #   # Check that the result is as expected
  #   assert result == expected_result  # substitute expected_result with the actual expected result

