from distributed_notebook.kernel.train.trainer import Trainer
from torch.profiler import profile, record_function, ProfilerActivity

import gc
import torch
import time

torch.cuda.memory._record_memory_history(
    max_entries=500_000
)

t = Trainer(batch_size = 64)
t.run(num_epochs=2)

try:
    torch.cuda.memory._dump_snapshot(f"before_deleting_t_first_time.pickle")
except Exception as e:
    print(f"[ERROR] Failed to capture memory snapshot {e}")

del t
gc.collect()
with torch.no_grad():
    torch.cuda.empty_cache()
torch.cuda.synchronize()

try:
    torch.cuda.memory._dump_snapshot(f"after_deleting_t_first_time.pickle")
except Exception as e:
    print(f"[ERROR] Failed to capture memory snapshot {e}")

t = Trainer(batch_size = 128)
t.run(num_epochs=2)
del t
gc.collect()
with torch.no_grad():
    torch.cuda.empty_cache()
torch.cuda.synchronize()

try:
    torch.cuda.memory._dump_snapshot(f"torch_cuda_snapshot_1.pickle")
except Exception as e:
    print(f"[ERROR] Failed to capture memory snapshot {e}")

t = Trainer(batch_size = 64)
t.run(num_epochs=2)

del t
gc.collect()
with torch.no_grad():
    torch.cuda.empty_cache()
torch.cuda.synchronize()

try:
    torch.cuda.memory._dump_snapshot(f"torch_cuda_snapshot_2.pickle")
except Exception as e:
    print(f"[ERROR] Failed to capture memory snapshot {e}")

# Stop recording memory snapshot history.
torch.cuda.memory._record_memory_history(enabled=None)

time.sleep(2)

print("\n\n\n\nDone.\n\n\n\n\n\n\n\n")

# Find all GPU tensors
gpu_tensors = [obj for obj in gc.get_objects() if isinstance(obj, torch.Tensor) and obj.is_cuda]
print(f"GPU Tensors: {[t.shape for t in gpu_tensors]}")

print(torch.cuda.memory_summary())
stats: dict = torch.cuda.memory_stats()
print("torch.cuda.memory_stats()")
for k,v in stats.items():
    print(f"{k}: {v}")

gc.collect()
with torch.no_grad():
    torch.cuda.empty_cache()
torch.cuda.synchronize()

print("\n\nEmptied cache and synchronized.\n\n\n\n\n\n")

# Find all GPU tensors
gpu_tensors = [obj for obj in gc.get_objects() if isinstance(obj, torch.Tensor) and obj.is_cuda]
print(f"GPU Tensors: {[t.shape for t in gpu_tensors]}")

print(torch.cuda.memory_summary())
stats: dict = torch.cuda.memory_stats()
print("torch.cuda.memory_stats()")
for k,v in stats.items():
    print(f"{k}: {v}")