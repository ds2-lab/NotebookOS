from typing import Union

from transformers import GPT2Tokenizer, BertTokenizer

def get_tokenizer(model_name: str = "")->Union[BertTokenizer, GPT2Tokenizer]:
    if model_name == "gpt2":
        return GPT2Tokenizer.from_pretrained("gpt2")
    elif model_name == "bert":
        return BertTokenizer.from_pretrained("bert-base-uncased")
    else:
        raise ValueError(f'Unknown or unsupported model name: "{model_name}"')