import json
import os
import shutil
import subprocess
import tarfile
import time
from abc import ABC
from multiprocessing import Pool
from pathlib import Path
from typing import Optional, Dict, List, Union

import sox
import wget
from torch.utils.data import Dataset
from torchvision.datasets.utils import check_integrity

from distributed_notebook.datasets.base import CustomDataset
from distributed_notebook.datasets.speech.loader import AudioDataLoader
from distributed_notebook.datasets.speech.parser import SpectrogramParser
from distributed_notebook.models.speech.deepspeech2.config import SpectConfig, AugmentationConfig

LibriSpeechName: str = "CIFAR-10"

LIBRI_SPEECH_URLS: Dict[str, List[str]] = {
    "train": ["https://www.openslr.org/resources/12/train-clean-100.tar.gz"],

    "val": ["http://www.openslr.org/resources/12/dev-clean.tar.gz"],

    "test_clean": ["http://www.openslr.org/resources/12/test-clean.tar.gz"],
    "test_other": ["http://www.openslr.org/resources/12/test-other.tar.gz"]
}

LIBRI_SPEECH_MD5_CHECKSUMS: Dict[str, str] = {
    "about.html": "c989c596e76fe53fabca29d30805bfb4",
    "dev-clean.tar.gz": "42e2234ba48799c1f50f24a7926300a1",
    "dev-other.tar.gz": "c8d0bcc9cca99d4f8b62fcc847357931",
    "info.txt": "951cbdf929053c642e0be46c84b728b6",
    "intro-disclaimers.tar.gz": "92ba57a9611a70fd7d34b73249ae48cf",
    "md5sum.txt": "5311866a1582672ac30785e61cdb0af7",
    "original-books.tar.gz": "9da96b465573c8d1ee1d5ad3d01c08e3",
    "original-mp3.tar.gz": "7e14b6df14f1c04a852a50ba5f7be915",
    "raw-metadata.tar.gz": "25eced105e10f4081585af89b8d27cd2",
    "test-clean.tar.gz": "32fa31d27d2e1cad72775fee3f4849a9",
    "test-other.tar.gz": "fb5a50374b501bb3bac4815ee91d3135",
    "train-clean-100.tar.gz": "2a93770f6d5c6c964bc36631d331a522",
    "train-clean-360.tar.gz": "c0e676e450a7ff2f54aeade5171606fa",
    "train-other-500.tar.gz": "d1a0fd59409feb2c614ce4d30c387708",
}

LIBRI_SPEECH_MD5_SUMS: Dict[str, str] = {
    "about.html": "c989c596e76fe53fabca29d30805bfb4",
    "dev-clean.tar.gz": "42e2234ba48799c1f50f24a7926300a1",
    "dev-other.tar.gz": "c8d0bcc9cca99d4f8b62fcc847357931",
    "info.txt": "951cbdf929053c642e0be46c84b728b6",
    "intro-disclaimers.tar.gz": "92ba57a9611a70fd7d34b73249ae48cf",
    "md5sum.txt": "5311866a1582672ac30785e61cdb0af7",
    "original-books.tar.gz": "9da96b465573c8d1ee1d5ad3d01c08e3",
    "original-mp3.tar.gz": "7e14b6df14f1c04a852a50ba5f7be915",
    "raw-metadata.tar.gz": "25eced105e10f4081585af89b8d27cd2",
    "test-clean.tar.gz": "32fa31d27d2e1cad72775fee3f4849a9",
    "test-other.tar.gz": "fb5a50374b501bb3bac4815ee91d3135",
    "train-clean-100.tar.gz": "2a93770f6d5c6c964bc36631d331a522",
    "train-clean-360.tar.gz": "c0e676e450a7ff2f54aeade5171606fa",
    "train-other-500.tar.gz": "d1a0fd59409feb2c614ce4d30c387708",
}


def _duration_file_path(path):
    return path, sox.file_info.duration(path)


def order_and_prune_files(
        file_paths,
        min_duration,
        max_duration,
        num_workers):
    print("Gathering durations...")
    with Pool(processes=num_workers) as p:
        duration_file_paths = list(p.imap(_duration_file_path, file_paths))
    print("Sorting manifests...")
    if min_duration and max_duration:
        print("Pruning manifests between %d and %d seconds" % (min_duration, max_duration))
        duration_file_paths = [(path, duration) for path, duration in duration_file_paths if
                               min_duration <= duration <= max_duration]

    total_duration = sum([x[1] for x in duration_file_paths])
    print(f"Total duration of split: {total_duration:.4f}s")
    return [x[0] for x in duration_file_paths]  # Remove durations


def create_manifest(
        data_path: str,
        output_name: str,
        manifest_path: str,
        num_workers: int,
        min_duration: Optional[float] = None,
        max_duration: Optional[float] = None,
        file_extension: str = "wav"):
    data_path = os.path.abspath(data_path)
    file_paths = list(Path(data_path).rglob(f"*.{file_extension}"))
    file_paths = order_and_prune_files(
        file_paths=file_paths,
        min_duration=min_duration,
        max_duration=max_duration,
        num_workers=num_workers
    )

    output_path = Path(manifest_path) / output_name
    output_path.parent.mkdir(exist_ok=True, parents=True)

    manifest = {
        'root_path': data_path,
        'samples': []
    }
    for wav_path in file_paths:
        wav_path = wav_path.relative_to(data_path)
        transcript_path = wav_path.parent.with_name("txt") / wav_path.with_suffix(".txt").name
        manifest['samples'].append({
            'wav_path': wav_path.as_posix(),
            'transcript_path': transcript_path.as_posix()
        })

    output_path.write_text(json.dumps(manifest), encoding='utf8')


def _preprocess_transcript(phrase):
    return phrase.strip().upper()


def _process_file(wav_dir, txt_dir, base_filename, root_dir, sample_rate: int = 16_000):
    full_recording_path = os.path.join(root_dir, base_filename)
    assert os.path.exists(full_recording_path) and os.path.exists(root_dir)

    wav_recording_path = os.path.join(wav_dir, base_filename.replace(".flac", ".wav"))
    subprocess.call(["sox {}  -r {} -b 16 -c 1 {}".format(full_recording_path, str(sample_rate),
                                                          wav_recording_path)], shell=True)
    # Process transcript
    txt_transcript_path = os.path.join(txt_dir, base_filename.replace(".flac", ".txt"))
    transcript_file = os.path.join(root_dir, "-".join(base_filename.split('-')[:-1]) + ".trans.txt")

    assert os.path.exists(transcript_file), "Transcript file {} does not exist.".format(transcript_file)

    transcriptions = open(transcript_file).read().strip().split("\n")
    transcriptions = {t.split()[0].split("-")[-1]: " ".join(t.split()[1:]) for t in transcriptions}

    with open(txt_transcript_path, "w") as f:
        key = base_filename.replace(".flac", "").split("-")[-1]
        assert key in transcriptions, "{} is not in the transcriptions".format(key)
        f.write(_preprocess_transcript(transcriptions[key]))
        f.flush()


def download_libri(
        root_directory: str,
        manifest_directory: str,
        sample_rate: int = 16_000,
        max_duration: int = 15,
        min_duration: int = 1,
        num_workers: int = 4,
):
    target_dl_dir = root_directory
    if not os.path.exists(target_dl_dir):
        print(f'Creating target download directory for LibriSpeech dataset: "{target_dl_dir}"')
        os.makedirs(target_dl_dir)

    for split_type, lst_libri_urls in LIBRI_SPEECH_URLS.items():
        split_dir = os.path.join(target_dl_dir, split_type)
        if not os.path.exists(split_dir):
            print(f'Creating directory: "{split_dir}"')
            os.makedirs(split_dir)

        split_wav_dir = os.path.join(split_dir, "wav")
        if not os.path.exists(split_wav_dir):
            print(f'Creating directory: "{split_wav_dir}"')
            os.makedirs(split_wav_dir)

        split_txt_dir = os.path.join(split_dir, "txt")
        if not os.path.exists(split_txt_dir):
            print(f'Creating directory: "{split_txt_dir}"')
            os.makedirs(split_txt_dir)

        extracted_dir = os.path.join(split_dir, "LibriSpeech")
        if os.path.exists(extracted_dir):
            print(f'Removing existing directory tree rooted at "{extracted_dir}"')
            shutil.rmtree(extracted_dir)

        for url in lst_libri_urls:
            filename = url.split("/")[-1]
            target_filename = os.path.join(split_dir, filename)
            if not os.path.exists(target_filename):
                print(f'File "{filename}" does not exist in directory "{split_dir}". '
                      f'Downloading from URL "{url}" to file "{target_filename}" now.')
                wget.download(url, split_dir)

            print(f"Unpacking archive '{filename}' now...")
            tar = tarfile.open(target_filename)
            tar.extractall(split_dir)
            tar.close()
            os.remove(target_filename)
            print("Converting flac files to wav and extracting transcripts...")
            assert os.path.exists(extracted_dir), "Archive {} was not properly uncompressed.".format(filename)
            for root, subdirectories, files in os.walk(extracted_dir):
                for f in files:
                    if f.find(".flac") != -1:
                        _process_file(wav_dir=split_wav_dir,
                                      txt_dir=split_txt_dir,
                                      base_filename=f,
                                      root_dir=root,
                                      sample_rate=sample_rate)

            print(f"Finished downloading and processing files from URL '{url}'")
            shutil.rmtree(extracted_dir)

        if split_type == 'train':  # Prune to min/max duration
            create_manifest(
                data_path=split_dir,
                output_name='libri_' + split_type + '_manifest.json',
                manifest_path=manifest_directory,
                min_duration=min_duration,
                max_duration=max_duration,
                num_workers=num_workers
            )
        else:
            create_manifest(
                data_path=split_dir,
                output_name='libri_' + split_type + '_manifest.json',
                manifest_path=manifest_directory,
                num_workers=num_workers
            )


def parse_input(input_path):
    ids = []
    if os.path.isdir(input_path):
        for wav_path in Path(input_path).rglob('*.wav'):
            transcript_path = str(wav_path).replace('/wav/', '/txt/').replace('.wav', '.txt')
            ids.append((wav_path, transcript_path))
    else:
        # Assume it is a manifest file
        with open(input_path) as f:
            manifest = json.load(f)
        for sample in manifest['samples']:
            wav_path = os.path.join(manifest['root_path'], sample['wav_path'])
            transcript_path = os.path.join(manifest['root_path'], sample['transcript_path'])
            ids.append((wav_path, transcript_path))
    return ids


class LibriSpeechDataset(Dataset, SpectrogramParser):
    """
    Dataset that loads tensors via a csv containing file paths to audio files and transcripts separated by
    a comma. Each new line is a different sample. Example below:

    /path/to/audio.wav,/path/to/audio.txt
    """
    base_folder: str = "LibriSpeech"

    archive_filenames: Dict[str, Union[Dict[str, str], str]] = {
        "train": "train-clean-100.tar.gz",
        "val": "dev-clean.tar.gz",
        "test_clean": "test-clean.tar.gz",
    }

    train_list: List[List[str]] = [
        ["train_batch", "train-clean-100"]
    ]
    test_list: List[List[str]] = [
        ["test_batch", "test-clean"]
    ]

    default_labels: List[str] = ["_", "'", "A", "B", "C", "D", "E", "F", "G", "H",
                                 "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R",
                                 "S", "T", "U", "V", "W", "X", "Y", "Z", " "]

    def __init__(self,
                 audio_conf: SpectConfig,
                 input_path: str,
                 labels: Optional[List[str]] = None,
                 normalize: bool = False,
                 max_duration: int = 15,
                 min_duration: int = 1,
                 num_workers: int = 4,
                 aug_cfg: AugmentationConfig = None):
        """
        Dataset that loads tensors via a csv containing file paths to audio files and transcripts separated by
        a comma. Each new line is a different sample. Example below:

        /path/to/audio.wav,/path/to/audio.txt
        ...
        You can also pass the directory of dataset.
        :param audio_conf: Config containing the sample rate, window and the window length/stride in seconds
        :param input_path: Path to input.
        :param labels: List containing all the possible characters to map to
        :param normalize: Apply standard mean and deviation normalization to audio tensor
        :param augmentation_conf(Optional): Config containing the augmentation parameters
        """
        if labels is None:
            labels = LibriSpeechDataset.default_labels

        self.input_path: str = input_path
        super(Dataset, self).__init__(audio_conf, normalize, aug_cfg)

        self.download(
            sample_rate=audio_conf.sample_rate,
            max_duration=max_duration,
            min_duration=min_duration,
            num_workers=num_workers,
        )

        self.ids = parse_input(input_path)
        self.size = len(self.ids)
        self.labels_map = dict([(labels[i], i) for i in range(len(labels))])

        if not self._check_integrity():
            raise RuntimeError("LibriSpeech dataset not found or is corrupted.")

    def __len__(self) -> int:
        return self.size

    def _check_integrity(self) -> bool:
        for filename, md5 in self.train_list + self.test_list:
            fpath = os.path.join(self.input_path, self.base_folder, filename)
            if not check_integrity(fpath, md5):
                return False
        return True

    def download(
            self,
            sample_rate: int = 16_000,
            max_duration: int = 15,
            min_duration: int = 1,
            num_workers: int = 4,
    ) -> None:
        if self._check_integrity():
            print("Files already downloaded and verified")
            return

        download_libri(
            root_directory=self.input_path,
            manifest_directory=os.path.join(self.input_path, "manifest"),
            sample_rate=sample_rate,
            max_duration=max_duration,
            min_duration=min_duration,
            num_workers=num_workers,
        )

    def __getitem__(self, index):
        sample = self.ids[index]
        audio_path, transcript_path = sample[0], sample[1]
        spect = self.parse_audio(audio_path)
        transcript = self.parse_transcript(transcript_path)
        return spect, transcript

    def parse_transcript(self, transcript_path):
        with open(transcript_path, 'r', encoding='utf8') as transcript_file:
            transcript = transcript_file.read().replace('\n', '')
        transcript = list(filter(None, [self.labels_map.get(x) for x in list(transcript)]))
        return transcript

    @property
    def requires_tokenization(self)->bool:
        return False

    @property
    def tokenization_start(self)->float:
        return -1.0

    @property
    def tokenization_end(self)->float:
        return -1.0

    @property
    def tokenization_duration_sec(self)->float:
        return -1.0

class LibriSpeech(CustomDataset, ABC):
    def __init__(
            self,
            root_dir: str = 'data',
            batch_size: int = 256,
            shuffle: bool = True,
            num_workers: int = 2,
            **kwargs):
        super().__init__(name=LibriSpeechName, root_dir=root_dir, shuffle=shuffle, num_workers=num_workers)

        self._dataset_already_downloaded: bool = self._check_if_downloaded(
            filenames=LibriSpeechDataset.train_list + LibriSpeechDataset.test_list,
            base_folder=LibriSpeechDataset.base_folder
        )

        self._download_start = time.time()
        self._train_dataset = LibriSpeechDataset(input_path=root_dir)
        self._test_dataset = LibriSpeechDataset(input_path=root_dir)
        self._download_end = time.time()
        self._download_duration_sec = self._download_end - self._download_start

        self._train_loader = AudioDataLoader(self._train_dataset, batch_size=batch_size, shuffle=shuffle,
                                             num_workers=num_workers)
        self._test_loader = AudioDataLoader(self._test_dataset, batch_size=batch_size, shuffle=shuffle,
                                            num_workers=num_workers)

        if self._dataset_already_downloaded:
            print(f"LibriSpeech dataset was already downloaded. Root directory: \"{root_dir}\"")
        else:
            print(
                f"LibriSpeech was downloaded to root directory \"{root_dir}\" in {self._download_duration_sec} seconds.")

    @property
    def requires_tokenization(self)->bool:
        return False

    @property
    def tokenization_start(self)->float:
        return -1.0

    @property
    def tokenization_end(self)->float:
        return -1.0

    @property
    def tokenization_duration_sec(self)->float:
        return -1.0