from distributed_notebook.datasets import CoLA
from distributed_notebook.models.nlp.bert import Bert
# from transformers import GPT2Tokenizer, GPT2LMHeadModel, Trainer, TrainingArguments

# dataset: CoLA = CoLA(model_name = "bert")
# model: Bert = Bert(out_features = 2)
# model.train(dataset.train_loader, 500)

# def test_train_bert_on_cola():
#     """
#     Train the BERT model on the CoLA dataset.
#     """
#     dataset: CoLA = CoLA(model_name = "bert")
#
#     model: Bert = Bert(out_features = 2)
#
#     # initial_weights = model.model.fc.weight.clone()
#
#     model.train(dataset.train_loader, 500)
#
#     # updated_weights = model.model.fc.weight
#
#     # assert initial_weights.equal(updated_weights) == False

from distributed_notebook.datasets import CoLA
dataset: CoLA = CoLA(model_name = "bert")
model: Bert = Bert(out_features = 2)

model.train(dataset.train_loader, 500)