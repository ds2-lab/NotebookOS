import io

import torch
import torch.nn as nn
import torch.optim as optim
import boto3
import os

# Define a simple PyTorch model
class SimpleModel(nn.Module):
    def __init__(self):
        super(SimpleModel, self).__init__()
        self.fc = nn.Linear(10, 1)

    def forward(self, x):
        return self.fc(x)

# Initialize the model, optimizer, and some dummy data
model = SimpleModel()
optimizer = optim.SGD(model.parameters(), lr=0.01)

# Dummy training loop
epoch = 0
loss = None
for epoch in range(3):  # Train for 3 epochs
    inputs = torch.randn(5, 10)  # Dummy input
    labels = torch.randn(5, 1)  # Dummy labels
    outputs = model(inputs)
    loss = nn.MSELoss()(outputs, labels)

    optimizer.zero_grad()
    loss.backward()
    optimizer.step()

    print(f"Epoch {epoch + 1}, Loss: {loss.item()}")

buffer: io.BytesIO = io.BytesIO()
torch.save({
    'epoch': epoch + 1,
    'model_state_dict': model.state_dict(),
    'optimizer_state_dict': optimizer.state_dict(),
    'loss': loss.item(),
}, buffer)

s3 = boto3.client('s3')
bucket_name = "distributed-notebook-storage"
s3_key = "model_checkpoints/model_checkpoint.pth"

try:
    s3.upload_fileobj(Fileobj=io.BytesIO(buffer.getvalue()), Bucket=bucket_name, Key=s3_key)
    print(f"Checkpoint uploaded to S3 at s3://{bucket_name}/{s3_key}")
except Exception as e:
    print(f"Failed to upload checkpoint to S3: {e}")

# Loading the checkpoint from S3
buffer: io.BytesIO = io.BytesIO()
try:
    s3.download_fileobj(bucket_name, s3_key, buffer)
    print(f"Checkpoint downloaded from S3 to buffer")
except Exception as e:
    print(f"Failed to download checkpoint from S3: {e}")

buffer.seek(0)

# Restoring the model and optimizer state
checkpoint = torch.load(buffer)
model.load_state_dict(checkpoint['model_state_dict'])
optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
epoch = checkpoint['epoch']
loss = checkpoint['loss']

print(f"Model and optimizer restored from checkpoint. Last epoch: {epoch}, Loss: {loss}")