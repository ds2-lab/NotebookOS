import os

from typing import Union

from transformers import GPT2Tokenizer, BertTokenizer

from distributed_notebook.deep_learning import Bert, GPT2


def get_tokenizer(model_name: str = "")->Union[BertTokenizer, GPT2Tokenizer]:
    model_name = model_name.lower()

    if model_name == "gpt2" or model_name == "gpt-2":
        # GPT2.static_download_from_s3()
        tokenizer: GPT2Tokenizer = GPT2Tokenizer.from_pretrained("gpt2")
        tokenizer.pad_token = tokenizer.eos_token  # GPT-2 doesn't have a pad token
        return tokenizer
    elif model_name == "bert":
        # Bert.static_download_from_s3()
        return BertTokenizer.from_pretrained("bert-base-uncased")
    else:
        raise ValueError(f'Unknown or unsupported model name: "{model_name}"')

def get_username():
    """
    Get and return the username of the current user.

    This should work on both Windows and Linux systems (with WSL/WSL2 treated as Linux).

    :return: the username of the current user.
    """
    if os.name == 'nt':
        try:
            return os.environ['USERNAME']
        except KeyError:
            return os.getlogin()
    else:
        try:
            return os.environ['USER']
        except KeyError:
            import pwd

            return pwd.getpwuid(os.getuid())[0]