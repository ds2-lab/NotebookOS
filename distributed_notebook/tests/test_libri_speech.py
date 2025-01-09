from distributed_notebook.datasets import LibriSpeechDataset
from distributed_notebook.models.speech.deepspeech2.config import SpectConfig

dataset: LibriSpeechDataset = LibriSpeechDataset(
    audio_conf = SpectConfig(),
    input_path = "./data/libri_speech",
    labels = None,
)