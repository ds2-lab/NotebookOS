import torch
import torch.nn as nn
from torch.nn.parallel import DataParallel
import pytest


class MyModel(nn.Module):
    def __init__(self):
        super().__init__()
        self.fc1 = nn.Linear(10, 10)

    def forward(self, x):
        return self.fc1(x)


@pytest.mark.skipif(not torch.cuda.is_available() or torch.cuda.device_count() <= 1, reason="requires at least 2 GPUs")
@pytest.mark.parametrize("num_gpus", [1, 2])
def test_pytorch_data_parallel(num_gpus):
    device = torch.device("cuda:0")

    model = MyModel()

    if num_gpus > 1:
        model = DataParallel(model, device_ids=[0, 1])

        torch.cuda.synchronize(device)

    model = model.to(device)

    x = torch.randn(10, 10).to(device)
    output = model(x)

    assert output.shape == (10, 10)
