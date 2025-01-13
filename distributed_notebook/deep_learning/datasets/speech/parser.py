import json
import math
import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import librosa
import numpy as np
import sox
import torch
from torch.utils.data import Dataset, Sampler, DistributedSampler, DataLoader
import torchaudio

from distributed_notebook.deep_learning.models.speech.deepspeech2.config import SpectConfig, AugmentationConfig

torchaudio.set_audio_backend("sox_io")


def load_audio(path):
    sound, sample_rate = torchaudio.load(path)
    if sound.shape[0] == 1:
        sound = sound.squeeze()
    else:
        sound = sound.mean(axis=0)  # multiple channels, average
    return sound.numpy()


class AudioParser(object):
    def __init__(self,
                 audio_conf: SpectConfig,
                 normalize: bool = False):
        """
        Parses audio file into spectrogram with optional normalization and various augmentations
        :param audio_conf: Dictionary containing the sample rate, window and the window length/stride in seconds
        :param normalize(default False):  Apply standard mean and deviation normalization to audio tensor
        """
        self.window_stride = audio_conf.window_stride
        self.window_size = audio_conf.window_size
        self.sample_rate = audio_conf.sample_rate
        self.window = audio_conf.window.value
        self.normalize = normalize

    def parse_transcript(self, transcript_path):
        """
        :param transcript_path: Path where transcript is stored from the manifest file
        :return: Transcript in training/testing format
        """
        raise NotImplementedError

    def parse_audio(self, audio_path):
        """
        :param audio_path: Path where audio is stored from the manifest file
        :return: Audio in training/testing format
        """
        raise NotImplementedError

    def get_chunks(self, y, chunk_size_seconds=-1):
        """
        :param y: Audio signal as an array of float numbers
        :param chunk_size_seconds: Chunk size in seconds
        :return: Chunk from the give audio signal of duration `chunk_size_seconds`
        """
        total_duration_seconds = math.ceil(len(y) / self.sample_rate)
        chunk_size_seconds = total_duration_seconds if chunk_size_seconds <= 0 else chunk_size_seconds
        num_of_chunks = math.ceil(total_duration_seconds / chunk_size_seconds)
        for i in range(num_of_chunks):
            chunk_start = int(i * chunk_size_seconds * self.sample_rate)
            chunk_end = chunk_start + int(chunk_size_seconds * self.sample_rate)
            chunk = y[chunk_start:chunk_end]
            yield chunk

    def compute_spectrogram(self, y):
        """
        :param y: Audio signal as an array of float numbers
        :return: Spectrogram of the signal
        """
        n_fft = int(self.sample_rate * self.window_size)
        win_length = n_fft
        hop_length = int(self.sample_rate * self.window_stride)
        # STFT
        D = librosa.stft(y, n_fft=n_fft, hop_length=hop_length,
                         win_length=win_length, window=self.window)
        spect, phase = librosa.magphase(D)
        # S = log(S+1)
        spect = np.log1p(spect)
        spect = torch.FloatTensor(spect)
        if self.normalize:
            mean = spect.mean()
            std = spect.std()
            spect.add_(-mean)
            spect.div_(std)

        return spect


class NoiseInjection(object):
    def __init__(self,
                 path=None,
                 sample_rate=16000,
                 noise_levels=(0, 0.5)):
        """
        Adds noise to an input signal with specific SNR. Higher the noise level, the more noise added.
        Modified code from https://github.com/willfrey/audio/blob/master/torchaudio/transforms.py
        """
        if not os.path.exists(path):
            print("Directory doesn't exist: {}".format(path))
            raise IOError
        self.paths = path is not None and librosa.util.find_files(path)
        self.sample_rate = sample_rate
        self.noise_levels = noise_levels

    def inject_noise(self, data):
        noise_path = np.random.choice(self.paths)
        noise_level = np.random.uniform(*self.noise_levels)
        return self.inject_noise_sample(data, noise_path, noise_level)

    def inject_noise_sample(self, data, noise_path, noise_level):
        noise_len = sox.file_info.duration(noise_path)
        data_len = len(data) / self.sample_rate
        noise_start = np.random.rand() * (noise_len - data_len)
        noise_end = noise_start + data_len
        noise_dst = audio_with_sox(noise_path, self.sample_rate, noise_start, noise_end)
        assert len(data) == len(noise_dst)
        noise_energy = np.sqrt(noise_dst.dot(noise_dst) / noise_dst.size)
        data_energy = np.sqrt(data.dot(data) / data.size)
        data += noise_level * noise_dst * data_energy / noise_energy
        return data


class SpectrogramParser(AudioParser):
    def __init__(self,
                 audio_conf: SpectConfig,
                 normalize: bool = False,
                 augmentation_conf: AugmentationConfig = None):
        """
        Parses audio file into spectrogram with optional normalization and various augmentations
        :param audio_conf: Dictionary containing the sample rate, window and the window length/stride in seconds
        :param normalize(default False):  Apply standard mean and deviation normalization to audio tensor
        :param augmentation_conf(Optional): Config containing the augmentation parameters
        """
        super(SpectrogramParser, self).__init__(audio_conf, normalize)
        self.aug_conf = augmentation_conf
        if augmentation_conf and augmentation_conf.noise_dir:
            self.noise_injector = NoiseInjection(path=augmentation_conf.noise_dir,
                                                 sample_rate=self.sample_rate,
                                                 noise_levels=augmentation_conf.noise_levels)
        else:
            self.noise_injector = None

    def parse_audio(self, audio_path):
        if self.aug_conf and self.aug_conf.speed_volume_perturb:
            y = load_randomly_augmented_audio(audio_path, self.sample_rate)
        else:
            y = load_audio(audio_path)
        if self.noise_injector:
            add_noise = np.random.binomial(1, self.aug_conf.noise_prob)
            if add_noise:
                y = self.noise_injector.inject_noise(y)

        spect = self.compute_spectrogram(y)
        if self.aug_conf and self.aug_conf.spec_augment:
            spect = spec_augment(spect)

        return spect

    def parse_transcript(self, transcript_path):
        raise NotImplementedError


class ChunkSpectrogramParser(AudioParser):
    def __init__(self,
                 audio_conf: SpectConfig,
                 normalize: bool = False):
        """
        Parses audio file into spectrogram with optional normalization and various augmentations
        :param audio_conf: Dictionary containing the sample rate, window and the window length/stride in seconds
        :param normalize(default False):  Apply standard mean and deviation normalization to audio tensor
        """
        super(ChunkSpectrogramParser, self).__init__(audio_conf, normalize)

    def parse_audio(self, audio_path, chunk_size_seconds=-1):
        y = load_audio(audio_path)
        for y_chunk in self.get_chunks(y, chunk_size_seconds):
            spect = self.compute_spectrogram(y_chunk)
            yield spect

def audio_with_sox(path, sample_rate, start_time, end_time):
    """
    crop and resample the recording with sox and loads it.
    """
    with NamedTemporaryFile(suffix=".wav") as tar_file:
        tar_filename = tar_file.name
        sox_params = "sox \"{}\" -r {} -c 1 -b 16 -e si {} trim {} ={} >/dev/null 2>&1".format(path, sample_rate,
                                                                                               tar_filename, start_time,
                                                                                               end_time)
        os.system(sox_params)
        y = load_audio(tar_filename)
        return y

def augment_audio_with_sox(path, sample_rate, tempo, gain):
    """
    Changes tempo and gain of the recording with sox and loads it.
    """
    with NamedTemporaryFile(suffix=".wav") as augmented_file:
        augmented_filename = augmented_file.name
        sox_augment_params = ["tempo", "{:.3f}".format(tempo), "gain", "{:.3f}".format(gain)]
        sox_params = "sox \"{}\" -r {} -c 1 -b 16 -e si {} {} >/dev/null 2>&1".format(path, sample_rate,
                                                                                      augmented_filename,
                                                                                      " ".join(sox_augment_params))
        os.system(sox_params)
        y = load_audio(augmented_filename)
        return y

def load_randomly_augmented_audio(path, sample_rate=16000, tempo_range=(0.85, 1.15),
                                  gain_range=(-6, 8)):
    """
    Picks tempo and gain uniformly, applies it to the utterance by using sox utility.
    Returns the augmented utterance.
    """
    low_tempo, high_tempo = tempo_range
    tempo_value = np.random.uniform(low=low_tempo, high=high_tempo)
    low_gain, high_gain = gain_range
    gain_value = np.random.uniform(low=low_gain, high=high_gain)
    audio = augment_audio_with_sox(path=path, sample_rate=sample_rate,
                                   tempo=tempo_value, gain=gain_value)
    return audio
