from typing import Union

from transformers import GPT2Tokenizer, BertTokenizer

def get_tokenizer(model_name: str = "")->Union[BertTokenizer, GPT2Tokenizer]:
    model_name = model_name.lower()

    if model_name == "gpt2" or model_name == "gpt-2":
        tokenizer: GPT2Tokenizer = GPT2Tokenizer.from_pretrained("gpt2")
        tokenizer.pad_token = tokenizer.eos_token  # GPT-2 doesn't have a pad token
        return tokenizer
    elif model_name == "bert":
        return BertTokenizer.from_pretrained("bert-base-uncased")
    else:
        raise ValueError(f'Unknown or unsupported model name: "{model_name}"')