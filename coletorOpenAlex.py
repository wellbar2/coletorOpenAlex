# -*- coding: utf-8 -*-
"""
coletorOpenAlex
===============

Coletor de metadados de Works da OpenAlex por DOI ou ID OpenAlex.

Requisitos:
    Python 3.10+
    requests

Instalação:
    pip install requests

Execução:
    python coletorOpenAlex.py

Gerar EXE no Windows:
    pip install pyinstaller requests

    pyinstaller --clean --noconfirm --onefile --windowed ^
        --name coletorOpenAlex ^
        coletorOpenAlex.py

Estrutura portátil:
    coletorOpenAlex.exe
    data/
        coletorOpenAlex_cache.sqlite3
        logs/
            coletorOpenAlex.log

Características:
- Tkinter
- API key OpenAlex
- entrada CSV/TXT
- uma única coluna identificadora
- detecção automática da coluna de DOI / Work ID
- DOI como tipo padrão
- consulta por DOI ou ID OpenAlex
- normalização automática:
      2975492377 -> W2975492377
      W2975492377 -> W2975492377
      https://openalex.org/W2975492377 -> W2975492377

      10.1007/xxxx -> 10.1007/xxxx
      doi:10.1007/xxxx -> 10.1007/xxxx
      https://doi.org/10.1007/xxxx -> 10.1007/xxxx

- cache SQLite
- alias DOI <-> Work ID no cache
- retomada independente da estratégia de consulta
- batch de até 100 identificadores
- singleton gratuito
- multithreading
- rate limiter global
- retries + exponential backoff
- três barras:
      trabalhos
      orçamento diário
      saldo pré-pago
- SQLite ao lado do EXE
- exportação: 1 Work = 1 linha
- autores consolidados em JSON na coluna autores
- textos sanitizados para impedir quebra física de linhas no CSV
- IDs OpenAlex de autores/instituições/sources preservados corretamente
"""

from __future__ import annotations

import csv
import io
import json
import math
import os
import queue
import random
import re
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import zlib

from collections import deque
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote, unquote, urlsplit

import tkinter as tk
from tkinter import ttk, filedialog, messagebox


# ============================================================
# REQUESTS
# ============================================================

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    requests = None
    HTTPAdapter = None


# ============================================================
# CONFIGURAÇÃO
# ============================================================

APP_NAME = "coletorOpenAlex"
APP_VERSION = "1.3.0"

BASE_URL = "https://api.openalex.org"

DEFAULT_THREADS = 12
DEFAULT_RPS = 50

MAX_THREADS = 32
MAX_RPS = 100

BATCH_SIZE = 100

DEFAULT_LIST_COST_USD = 0.0001

MAX_RETRIES = 5

CONNECT_TIMEOUT = 10
READ_TIMEOUT = 60

RATE_REFRESH_SECONDS = 10

CACHE_PROFILE = "coletorOpenAlex-work-profile-2026-08-v3"


# ============================================================
# CAMPOS SOLICITADOS À OPENALEX
# ============================================================

SELECT_FIELDS = ",".join(
    [
        "id",
        "doi",
        "title",
        "display_name",
        "publication_year",
        "publication_date",
        "type",
        "language",
        "abstract_inverted_index",
        "cited_by_count",
        "is_retracted",
        "is_paratext",
        "primary_location",
        "open_access",
        "authorships",
        "referenced_works_count",
        "topics",
        "primary_topic",
        "keywords",
        "funders",
        "awards",
        "fwci",
        "citation_normalized_percentile",
        "created_date",
        "updated_date",
    ]
)


# ============================================================
# REGEX
# ============================================================

OPENALEX_W_RE = re.compile(
    r"(?i)\bW(\d+)\b"
)

BARE_OPENALEX_RE = re.compile(
    r"^\s*(\d+)(?:\.0+)?\s*$"
)

DOI_RE = re.compile(
    r"^10\.\d{4,9}/\S+$",
    re.IGNORECASE,
)


# ============================================================
# DIRETÓRIOS PORTÁTEIS
# ============================================================

def application_directory() -> Path:
    """
    Se for EXE PyInstaller:
        retorna a pasta real do EXE.

    Se for .py:
        retorna a pasta do arquivo.
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent

    return Path(__file__).resolve().parent


APP_DIR = application_directory()

DATA_DIR = APP_DIR / "data"

LOG_DIR = DATA_DIR / "logs"

DB_PATH = DATA_DIR / "coletorOpenAlex_cache.sqlite3"


def ensure_data_directories() -> None:
    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Teste explícito de permissão de escrita.
    test_path = DATA_DIR / ".write_test"

    test_path.write_text(
        "ok",
        encoding="utf-8",
    )

    try:
        test_path.unlink()
    except OSError:
        pass


# ============================================================
# CSV
# ============================================================

def configure_csv_field_limit() -> None:
    limit = sys.maxsize

    while True:
        try:
            csv.field_size_limit(limit)
            break
        except OverflowError:
            limit //= 10


configure_csv_field_limit()


# ============================================================
# FUNÇÕES GERAIS
# ============================================================

def utc_now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def as_float(
    value: Any,
    default: float = 0.0,
) -> float:

    try:
        if value is None:
            return default

        return float(value)

    except (TypeError, ValueError):
        return default


def safe_string(value: Any) -> str:
    """
    Converte um valor em texto seguro para uma célula CSV.

    Regra importante desta versão: nenhuma string exportada pode
    conter quebra física de linha. CR, LF, TAB e sequências de
    whitespace são convertidos em um único espaço.
    """
    if value is None:
        return ""

    if isinstance(value, bool):
        return "True" if value else "False"

    text = str(value)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def sanitize_nested(value: Any) -> Any:
    """Sanitiza recursivamente strings antes de serializar JSON."""
    if value is None:
        return None

    if isinstance(value, str):
        return safe_string(value)

    if isinstance(value, list):
        return [sanitize_nested(item) for item in value]

    if isinstance(value, tuple):
        return [sanitize_nested(item) for item in value]

    if isinstance(value, dict):
        return {
            safe_string(key): sanitize_nested(item)
            for key, item in value.items()
        }

    return value


def normalize_openalex_entity_id(value: Any) -> str:
    """
    Compacta IDs OpenAlex de qualquer entidade quando possível.

    Exemplos:
        https://openalex.org/W123 -> W123
        https://openalex.org/A123 -> A123
        https://openalex.org/I123 -> I123
        https://openalex.org/S123 -> S123
        https://openalex.org/T123 -> T123

    URLs hierárquicas como /subfields/2705 são preservadas, em vez
    de serem apagadas como ocorria quando o normalizador de Work ID
    era usado para todas as entidades.
    """
    text = safe_string(value)

    if not text:
        return ""

    match = re.search(
        r"(?i)(?:^|openalex\.org/)([A-Z]\d+)$",
        text,
    )

    if match:
        raw = match.group(1)
        return raw[0].upper() + raw[1:]

    return text


def format_int(value: int) -> str:
    return f"{value:,}".replace(
        ",",
        ".",
    )


def format_usd(value: Any) -> str:
    number = as_float(value)

    text = f"{number:,.4f}"

    text = (
        text
        .replace(",", "X")
        .replace(".", ",")
        .replace("X", ".")
    )

    return f"US$ {text}"


def chunks(
    values: list[str],
    size: int,
) -> Iterable[list[str]]:

    for index in range(
        0,
        len(values),
        size,
    ):
        yield values[
            index:index + size
        ]


def join_values(
    values: Iterable[Any],
    separator: str = " | ",
) -> str:

    output = []
    seen = set()

    for value in values:
        if value is None:
            continue

        text = safe_string(value)

        if not text:
            continue

        if text in seen:
            continue

        seen.add(text)
        output.append(text)

    return separator.join(output)


def compact_json(value: Any) -> str:
    if value in (
        None,
        "",
        [],
        {},
    ):
        return ""

    return json.dumps(
        sanitize_nested(value),
        ensure_ascii=False,
        separators=(",", ":"),
    )


def normalize_header(value: str) -> str:
    text = str(value).strip().lower()

    text = re.sub(
        r"[^a-z0-9]+",
        "_",
        text,
    )

    return text.strip("_")


def open_folder(path: Path) -> None:
    try:
        if sys.platform.startswith("win"):
            os.startfile(str(path))

        elif sys.platform == "darwin":
            subprocess.Popen(
                ["open", str(path)]
            )

        else:
            subprocess.Popen(
                ["xdg-open", str(path)]
            )

    except Exception:
        pass


# ============================================================
# NORMALIZAÇÃO DE DOI
# ============================================================

def normalize_doi(
    value: Any,
) -> str | None:
    """
    Exemplos aceitos:

        10.1007/abc
        DOI:10.1007/abc
        doi:10.1007/abc
        https://doi.org/10.1007/abc
        http://dx.doi.org/10.1007/abc

    Retorno:
        10.1007/abc
    """

    if value is None:
        return None

    text = str(value).strip()

    if not text:
        return None

    text = unquote(text)

    # Remove aspas externas.
    text = text.strip()

    if (
        len(text) >= 2
        and text[0] == text[-1]
        and text[0] in "\"'"
    ):
        text = text[1:-1].strip()

    lower = text.lower()

    # DOI como URL.
    if lower.startswith(
        ("http://", "https://")
    ):
        try:
            parsed = urlsplit(text)

            host = (
                parsed.netloc
                .lower()
                .split(":")[0]
            )

            if host in {
                "doi.org",
                "www.doi.org",
                "dx.doi.org",
                "www.dx.doi.org",
            }:
                text = unquote(
                    parsed.path.lstrip("/")
                )

            else:
                # Talvez seja uma URL OpenAlex contendo DOI?
                # Não inferimos DOI de URLs arbitrárias.
                return None

        except Exception:
            return None

    text = text.strip()

    lower = text.lower()

    prefixes = (
        "doi:",
        "doi ",
        "doi.org/",
        "dx.doi.org/",
    )

    for prefix in prefixes:
        if lower.startswith(prefix):
            text = text[
                len(prefix):
            ].strip()

            break

    # Novo lower depois da remoção.
    text = text.strip()

    if not DOI_RE.fullmatch(text):
        return None

    # DOI é normalizado para minúsculas para deduplicação/cache.
    return text.lower()


# ============================================================
# NORMALIZAÇÃO DE ID OPENALEX
# ============================================================

def normalize_openalex_id(
    value: Any,
    allow_bare_numeric: bool = True,
) -> str | None:
    """
    Exemplos:

        2975492377
        2975492377.0
        W2975492377
        w2975492377
        https://openalex.org/W2975492377
        https://api.openalex.org/works/W2975492377

    Retorno:

        W2975492377
    """

    if value is None:
        return None

    text = str(value).strip()

    if not text:
        return None

    match = OPENALEX_W_RE.search(
        text
    )

    if match:
        return "W" + match.group(1)

    if allow_bare_numeric:
        match = BARE_OPENALEX_RE.fullmatch(
            text
        )

        if match:
            return "W" + match.group(1)

    return None


def normalize_identifier(
    value: Any,
    identifier_type: str,
) -> str | None:

    if identifier_type == "doi":
        return normalize_doi(value)

    if identifier_type == "openalex":
        return normalize_openalex_id(
            value,
            allow_bare_numeric=True,
        )

    raise ValueError(
        f"Tipo de identificador inválido: {identifier_type}"
    )


# ============================================================
# DETECÇÃO DE ID PARA AMOSTRAGEM
# ============================================================

def looks_like_openalex_id(
    value: Any,
) -> bool:
    """
    Mais conservador que normalize_openalex_id(), pois é usado
    na detecção automática de coluna.

    Números muito curtos (ex.: ano 2024) não são classificados
    como Work ID.
    """

    if value is None:
        return False

    text = str(value).strip()

    if not text:
        return False

    if OPENALEX_W_RE.search(text):
        return True

    match = BARE_OPENALEX_RE.fullmatch(
        text
    )

    if not match:
        return False

    digits = match.group(1)

    # Evita confundir anos, páginas etc.
    return len(digits) >= 7


# ============================================================
# LOG
# ============================================================

class SimpleLogger:

    def __init__(self) -> None:
        self.lock = threading.Lock()

    @property
    def path(self) -> Path:
        return LOG_DIR / "coletorOpenAlex.log"

    def write(
        self,
        text: str,
    ) -> None:

        try:
            ensure_data_directories()

            stamp = datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            with self.lock:
                with self.path.open(
                    "a",
                    encoding="utf-8",
                ) as file:

                    file.write(
                        f"[{stamp}] {text}\n"
                    )

        except Exception:
            pass


LOGGER = SimpleLogger()


# ============================================================
# EXCEÇÕES
# ============================================================

class OpenAlexError(Exception):
    pass


class AuthenticationError(OpenAlexError):
    pass


class APIRequestError(OpenAlexError):
    pass


class RateLimitError(OpenAlexError):
    pass


class CancelledByUser(OpenAlexError):
    pass


# ============================================================
# ESTRUTURA DE DETECÇÃO DA ENTRADA
# ============================================================

@dataclass
class ColumnProfile:
    index: int
    name: str

    detected_type: str | None

    doi_hits: int
    openalex_hits: int
    nonempty: int

    doi_score: float
    openalex_score: float

    best_score: float


@dataclass
class InputInspection:
    path: Path

    encoding: str

    delimiter: str | None

    has_header: bool

    headers: list[str]

    column_count: int

    profiles: list[ColumnProfile]

    suggested_column_index: int | None

    suggested_identifier_type: str | None


@dataclass
class InputLoadResult:
    identifiers: list[str]

    rows_read: int

    valid_values: int

    invalid_values: int

    blank_values: int

    duplicates: int

    normalized_values: int

    added_w_values: int


# ============================================================
# LEITOR DE ENTRADA
# ============================================================

class InputReader:

    ENCODINGS = (
        "utf-8-sig",
        "utf-8",
        "cp1252",
        "latin-1",
    )

    HEADER_DOI_NAMES = {
        "doi",
        "dois",
        "digital_object_identifier",
        "digital_object_identifiers",
    }

    HEADER_OPENALEX_NAMES = {
        "id",
        "work_id",
        "workid",
        "openalex",
        "openalex_id",
        "openalex_work_id",
        "work_openalex_id",
        "id_openalex",
    }

    @classmethod
    def read_sample(
        cls,
        path: Path,
        size: int = 262144,
    ) -> tuple[str, str]:

        raw = path.read_bytes()[:size]

        for encoding in cls.ENCODINGS:
            try:
                return (
                    raw.decode(encoding),
                    encoding,
                )

            except UnicodeDecodeError:
                continue

        return (
            raw.decode(
                "latin-1",
                errors="replace",
            ),
            "latin-1",
        )

    @classmethod
    def detect_delimiter(
        cls,
        sample: str,
    ) -> str:

        try:
            dialect = csv.Sniffer().sniff(
                sample,
                delimiters=",;\t|",
            )

            return dialect.delimiter

        except csv.Error:
            candidates = {
                ",": sample.count(","),
                ";": sample.count(";"),
                "\t": sample.count("\t"),
            }

            return max(
                candidates,
                key=candidates.get,
            )

    @classmethod
    def _header_hint(
        cls,
        first_row: list[str],
    ) -> bool:

        for cell in first_row:
            header = normalize_header(cell)

            if header in cls.HEADER_DOI_NAMES:
                return True

            if header in cls.HEADER_OPENALEX_NAMES:
                return True

            if "doi" in header:
                return True

            if (
                "openalex" in header
                and "id" in header
            ):
                return True

        return False

    @classmethod
    def _score_column(
        cls,
        index: int,
        name: str,
        data_rows: list[list[str]],
    ) -> ColumnProfile:

        header = normalize_header(name)

        doi_header_score = 0.0
        openalex_header_score = 0.0

        if header in cls.HEADER_DOI_NAMES:
            doi_header_score += 1500

        elif "doi" in header:
            doi_header_score += 1000

        if header in cls.HEADER_OPENALEX_NAMES:
            openalex_header_score += 1300

        elif "openalex" in header:
            openalex_header_score += 1000

        elif (
            "work" in header
            and "id" in header
        ):
            openalex_header_score += 900

        doi_hits = 0
        openalex_hits = 0
        nonempty = 0

        for row in data_rows[:100]:
            if index >= len(row):
                continue

            value = row[index].strip()

            if not value:
                continue

            nonempty += 1

            if normalize_doi(value):
                doi_hits += 1

            if looks_like_openalex_id(value):
                openalex_hits += 1

        if nonempty > 0:
            doi_ratio = (
                doi_hits / nonempty
            )

            openalex_ratio = (
                openalex_hits / nonempty
            )

        else:
            doi_ratio = 0.0
            openalex_ratio = 0.0

        doi_score = (
            doi_header_score
            + doi_ratio * 1200
            + min(doi_hits, 20) * 3
        )

        openalex_score = (
            openalex_header_score
            + openalex_ratio * 1200
            + min(openalex_hits, 20) * 3
        )

        if (
            doi_score == 0
            and openalex_score == 0
        ):
            detected_type = None

        elif doi_score >= openalex_score:
            detected_type = "doi"

        else:
            detected_type = "openalex"

        return ColumnProfile(
            index=index,
            name=name,
            detected_type=detected_type,
            doi_hits=doi_hits,
            openalex_hits=openalex_hits,
            nonempty=nonempty,
            doi_score=doi_score,
            openalex_score=openalex_score,
            best_score=max(
                doi_score,
                openalex_score,
            ),
        )

    @classmethod
    def inspect(
        cls,
        path: str | Path,
    ) -> InputInspection:

        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(path)

        sample, encoding = cls.read_sample(
            path
        )

        # TXT: uma única coluna.
        if path.suffix.lower() in {
            ".txt",
            ".list",
        }:
            lines = [
                [line]
                for line in sample.splitlines()
                if line.strip()
            ]

            profile = cls._score_column(
                index=0,
                name="Identificador",
                data_rows=lines,
            )

            return InputInspection(
                path=path,
                encoding=encoding,
                delimiter=None,
                has_header=False,
                headers=[
                    "Identificador"
                ],
                column_count=1,
                profiles=[profile],
                suggested_column_index=0,
                suggested_identifier_type=(
                    profile.detected_type
                ),
            )

        delimiter = cls.detect_delimiter(
            sample
        )

        reader = csv.reader(
            io.StringIO(sample),
            delimiter=delimiter,
        )

        rows = []

        try:
            for _ in range(101):
                rows.append(
                    next(reader)
                )

        except StopIteration:
            pass

        if not rows:
            raise ValueError(
                "O arquivo está vazio."
            )

        column_count = max(
            len(row)
            for row in rows
        )

        first_row = rows[0]

        has_header = cls._header_hint(
            first_row
        )

        if not has_header:
            try:
                has_header = csv.Sniffer().has_header(
                    sample
                )

            except csv.Error:
                has_header = False

        if has_header:
            headers = []

            for index in range(
                column_count
            ):
                if index < len(first_row):
                    name = first_row[
                        index
                    ].strip()

                else:
                    name = ""

                if not name:
                    name = (
                        f"Coluna {index + 1}"
                    )

                headers.append(name)

            data_rows = rows[1:]

        else:
            headers = [
                f"Coluna {index + 1}"
                for index in range(
                    column_count
                )
            ]

            data_rows = rows

        profiles = []

        for index, name in enumerate(
            headers
        ):
            profiles.append(
                cls._score_column(
                    index=index,
                    name=name,
                    data_rows=data_rows,
                )
            )

        suggested = None

        if profiles:
            best = max(
                profiles,
                key=lambda profile:
                    profile.best_score,
            )

            if (
                best.best_score > 0
                or len(profiles) == 1
            ):
                suggested = best.index

        if suggested is None:
            suggested_type = None
        else:
            suggested_type = (
                profiles[
                    suggested
                ].detected_type
            )

        return InputInspection(
            path=path,
            encoding=encoding,
            delimiter=delimiter,
            has_header=has_header,
            headers=headers,
            column_count=column_count,
            profiles=profiles,
            suggested_column_index=suggested,
            suggested_identifier_type=(
                suggested_type
            ),
        )

    @classmethod
    def load_identifiers(
        cls,
        inspection: InputInspection,
        column_index: int,
        identifier_type: str,
    ) -> InputLoadResult:

        ordered = []
        seen = set()

        rows_read = 0
        valid_values = 0
        invalid_values = 0
        blank_values = 0
        normalized_values = 0
        added_w_values = 0

        def process(
            raw_value: Any,
        ) -> None:

            nonlocal valid_values
            nonlocal invalid_values
            nonlocal blank_values
            nonlocal normalized_values
            nonlocal added_w_values

            raw_text = (
                ""
                if raw_value is None
                else str(raw_value).strip()
            )

            if not raw_text:
                blank_values += 1
                return

            normalized = normalize_identifier(
                raw_text,
                identifier_type,
            )

            if normalized is None:
                invalid_values += 1
                return

            valid_values += 1

            if identifier_type == "openalex":
                # Conta quantos IDs receberam W automaticamente.
                bare_match = (
                    BARE_OPENALEX_RE
                    .fullmatch(raw_text)
                )

                if bare_match:
                    added_w_values += 1

            comparable = (
                raw_text
                .strip()
                .lower()
            )

            if identifier_type == "doi":
                if comparable != normalized:
                    normalized_values += 1

            else:
                if comparable != normalized.lower():
                    normalized_values += 1

            if normalized not in seen:
                seen.add(normalized)
                ordered.append(normalized)

        if inspection.delimiter is None:
            with inspection.path.open(
                "r",
                encoding=inspection.encoding,
                errors="replace",
            ) as file:

                for line in file:
                    rows_read += 1
                    process(line)

        else:
            with inspection.path.open(
                "r",
                encoding=inspection.encoding,
                errors="replace",
                newline="",
            ) as file:

                reader = csv.reader(
                    file,
                    delimiter=inspection.delimiter,
                )

                if inspection.has_header:
                    try:
                        next(reader)
                    except StopIteration:
                        pass

                for row in reader:
                    rows_read += 1

                    if column_index >= len(row):
                        blank_values += 1
                        continue

                    process(
                        row[column_index]
                    )

        duplicates = max(
            0,
            valid_values - len(ordered),
        )

        return InputLoadResult(
            identifiers=ordered,
            rows_read=rows_read,
            valid_values=valid_values,
            invalid_values=invalid_values,
            blank_values=blank_values,
            duplicates=duplicates,
            normalized_values=normalized_values,
            added_w_values=added_w_values,
        )


# ============================================================
# RATE LIMITER GLOBAL
# ============================================================

class RequestRateLimiter:

    def __init__(
        self,
        requests_per_second: float,
        stop_event: threading.Event,
    ) -> None:

        self.interval = (
            1.0
            / max(
                1.0,
                requests_per_second,
            )
        )

        self.stop_event = stop_event

        self.lock = threading.Lock()

        self.next_slot = time.monotonic()

    def wait(self) -> None:

        with self.lock:
            now = time.monotonic()

            slot = max(
                now,
                self.next_slot,
            )

            self.next_slot = (
                slot
                + self.interval
            )

            delay = max(
                0.0,
                slot - now,
            )

        if delay > 0:
            if self.stop_event.wait(
                delay
            ):
                raise CancelledByUser()


# ============================================================
# CONTROLE LOCAL CONSERVADOR DO ORÇAMENTO
# ============================================================

class BudgetGuard:

    def __init__(self) -> None:
        self.lock = threading.Lock()

        self.initialized = False

        self.daily_budget = 0.0
        self.daily_remaining = 0.0

        self.prepaid_remaining = 0.0

        self.initial_prepaid_remaining = 0.0

        self.list_cost = (
            DEFAULT_LIST_COST_USD
        )

        self.estimated_prepaid_spent = 0.0

    def initialize(
        self,
        rate_status: dict[str, Any],
    ) -> None:

        rate = (
            rate_status.get(
                "rate_limit"
            )
            or {}
        )

        endpoint_costs = (
            rate.get(
                "endpoint_costs_usd"
            )
            or {}
        )

        with self.lock:
            self.daily_budget = max(
                0.0,
                as_float(
                    rate.get(
                        "daily_budget_usd"
                    )
                ),
            )

            self.daily_remaining = max(
                0.0,
                as_float(
                    rate.get(
                        "daily_remaining_usd"
                    )
                ),
            )

            self.prepaid_remaining = max(
                0.0,
                as_float(
                    rate.get(
                        "prepaid_remaining_usd"
                    )
                ),
            )

            self.initial_prepaid_remaining = (
                self.prepaid_remaining
            )

            actual_list_cost = (
                as_float(
                    endpoint_costs.get(
                        "list"
                    )
                )
            )

            if actual_list_cost > 0:
                self.list_cost = (
                    actual_list_cost
                )

            self.estimated_prepaid_spent = (
                0.0
            )

            self.initialized = True

    def conservative_sync(
        self,
        rate_status: dict[str, Any],
    ) -> None:

        if not self.initialized:
            self.initialize(
                rate_status
            )

            return

        rate = (
            rate_status.get(
                "rate_limit"
            )
            or {}
        )

        endpoint_costs = (
            rate.get(
                "endpoint_costs_usd"
            )
            or {}
        )

        real_daily = max(
            0.0,
            as_float(
                rate.get(
                    "daily_remaining_usd"
                )
            ),
        )

        real_prepaid = max(
            0.0,
            as_float(
                rate.get(
                    "prepaid_remaining_usd"
                )
            ),
        )

        with self.lock:
            # Não aumenta o saldo interno durante a execução.
            # Isto protege contra reservas ainda "em voo".
            self.daily_remaining = min(
                self.daily_remaining,
                real_daily,
            )

            self.prepaid_remaining = min(
                self.prepaid_remaining,
                real_prepaid,
            )

            new_cost = as_float(
                endpoint_costs.get(
                    "list"
                )
            )

            if new_cost > 0:
                self.list_cost = new_cost

            observed_prepaid_spent = max(
                0.0,
                self.initial_prepaid_remaining
                - real_prepaid,
            )

            self.estimated_prepaid_spent = max(
                self.estimated_prepaid_spent,
                observed_prepaid_spent,
            )

    def reserve_free_batch(
        self,
    ) -> bool:

        with self.lock:
            cost = self.list_cost

            # Pequena margem para evitar atravessar
            # o limite gratuito com várias threads.
            safety = max(
                cost * 5,
                0.0005,
            )

            if (
                self.daily_remaining
                >= cost + safety
            ):
                self.daily_remaining -= cost

                return True

            return False

    def reserve_fast_batch(
        self,
        allow_prepaid: bool,
        prepaid_cap_usd: float,
    ) -> bool:

        if self.reserve_free_batch():
            return True

        if not allow_prepaid:
            return False

        with self.lock:
            cost = self.list_cost

            if (
                self.prepaid_remaining
                < cost
            ):
                return False

            if (
                prepaid_cap_usd >= 0
                and (
                    self.estimated_prepaid_spent
                    + cost
                    > prepaid_cap_usd
                )
            ):
                return False

            self.prepaid_remaining -= cost

            self.estimated_prepaid_spent += cost

            return True

    def snapshot(
        self,
    ) -> dict[str, float]:

        with self.lock:
            return {
                "daily_budget":
                    self.daily_budget,

                "daily_remaining":
                    self.daily_remaining,

                "prepaid_remaining":
                    self.prepaid_remaining,

                "list_cost":
                    self.list_cost,

                "estimated_prepaid_spent":
                    self.estimated_prepaid_spent,
            }


# ============================================================
# RESULTADO DE UM IDENTIFICADOR
# ============================================================

@dataclass
class ItemResult:
    requested_identifier: str

    identifier_type: str

    status: str

    work: dict[str, Any] | None = None

    canonical_work_id: str | None = None

    http_status: int | None = None

    error: str | None = None


# ============================================================
# CLIENTE OPENALEX
# ============================================================

class OpenAlexClient:

    def __init__(
        self,
        api_key: str,
        threads: int,
        requests_per_second: int,
        stop_event: threading.Event,
    ) -> None:

        self.api_key = (
            api_key.strip()
        )

        self.threads = threads

        self.stop_event = (
            stop_event
        )

        self.rate_limiter = (
            RequestRateLimiter(
                requests_per_second=
                    requests_per_second,

                stop_event=
                    stop_event,
            )
        )

        self.local = (
            threading.local()
        )

        self.stats_lock = (
            threading.Lock()
        )

        self.http_requests = 0

        self.singleton_requests = 0

        self.batch_requests = 0

        self.rate_requests = 0

        self.retry_requests = 0

        self.reported_cost_usd = 0.0

    def _session(self):
        if not hasattr(
            self.local,
            "session",
        ):
            session = requests.Session()

            adapter = HTTPAdapter(
                pool_connections=max(
                    10,
                    self.threads * 2,
                ),

                pool_maxsize=max(
                    10,
                    self.threads * 2,
                ),

                max_retries=0,
            )

            session.mount(
                "https://",
                adapter,
            )

            session.mount(
                "http://",
                adapter,
            )

            session.headers.update(
                {
                    "User-Agent":
                        (
                            f"{APP_NAME}/"
                            f"{APP_VERSION}"
                        ),

                    "Accept":
                        "application/json",
                }
            )

            self.local.session = (
                session
            )

        return self.local.session

    def _increment_request(
        self,
        kind: str,
        retry: bool,
    ) -> None:

        with self.stats_lock:
            self.http_requests += 1

            if kind == "singleton":
                self.singleton_requests += 1

            elif kind == "batch":
                self.batch_requests += 1

            elif kind == "rate":
                self.rate_requests += 1

            if retry:
                self.retry_requests += 1

    def _add_cost(
        self,
        value: float,
    ) -> None:

        if value <= 0:
            return

        with self.stats_lock:
            self.reported_cost_usd += value

    def stats(
        self,
    ) -> dict[str, Any]:

        with self.stats_lock:
            return {
                "http_requests":
                    self.http_requests,

                "singleton_requests":
                    self.singleton_requests,

                "batch_requests":
                    self.batch_requests,

                "rate_requests":
                    self.rate_requests,

                "retry_requests":
                    self.retry_requests,

                "reported_cost_usd":
                    self.reported_cost_usd,
            }

    def _request(
        self,
        url: str,
        params: dict[str, Any],
        kind: str,
    ):
        params = dict(params)

        params["api_key"] = (
            self.api_key
        )

        last_error = None

        for attempt in range(
            MAX_RETRIES + 1
        ):
            if self.stop_event.is_set():
                raise CancelledByUser()

            self.rate_limiter.wait()

            self._increment_request(
                kind=kind,
                retry=attempt > 0,
            )

            try:
                response = (
                    self._session().get(
                        url,
                        params=params,
                        timeout=(
                            CONNECT_TIMEOUT,
                            READ_TIMEOUT,
                        ),
                        allow_redirects=True,
                    )
                )

            except requests.RequestException as exc:
                last_error = exc

                if attempt >= MAX_RETRIES:
                    raise APIRequestError(
                        str(exc)
                    ) from exc

                delay = min(
                    30.0,
                    (2 ** attempt)
                    + random.uniform(
                        0.0,
                        0.5,
                    ),
                )

                if self.stop_event.wait(
                    delay
                ):
                    raise CancelledByUser()

                continue

            if response.status_code in (
                401,
                403,
            ):
                raise AuthenticationError(
                    "A OpenAlex recusou a API key "
                    f"(HTTP {response.status_code})."
                )

            if response.status_code == 429:
                if attempt >= MAX_RETRIES:
                    raise RateLimitError(
                        "A OpenAlex continuou retornando "
                        "HTTP 429 após várias tentativas."
                    )

                retry_after = as_float(
                    response.headers.get(
                        "Retry-After"
                    ),
                    0.0,
                )

                if retry_after <= 0:
                    retry_after = min(
                        30.0,
                        (2 ** attempt)
                        + random.uniform(
                            0.0,
                            0.5,
                        ),
                    )

                if self.stop_event.wait(
                    retry_after
                ):
                    raise CancelledByUser()

                continue

            if response.status_code in (
                500,
                502,
                503,
                504,
            ):
                if attempt >= MAX_RETRIES:
                    raise APIRequestError(
                        "A OpenAlex retornou "
                        f"HTTP {response.status_code}."
                    )

                delay = min(
                    30.0,
                    (2 ** attempt)
                    + random.uniform(
                        0.0,
                        0.5,
                    ),
                )

                if self.stop_event.wait(
                    delay
                ):
                    raise CancelledByUser()

                continue

            return response

        raise APIRequestError(
            str(last_error)
            if last_error
            else "Falha desconhecida."
        )

    def get_rate_limit(
        self,
    ) -> dict[str, Any]:

        response = self._request(
            f"{BASE_URL}/rate-limit",
            params={},
            kind="rate",
        )

        if response.status_code != 200:
            raise APIRequestError(
                "Não foi possível consultar "
                "o saldo da chave: "
                f"HTTP {response.status_code}."
            )

        try:
            data = response.json()

        except ValueError as exc:
            raise APIRequestError(
                "Resposta inválida do endpoint "
                "/rate-limit."
            ) from exc

        if "rate_limit" not in data:
            raise APIRequestError(
                "A OpenAlex não retornou "
                "informações de rate limit."
            )

        return data

    @staticmethod
    def _singleton_path(
        identifier_type: str,
        identifier: str,
    ) -> str:

        if identifier_type == "openalex":
            return identifier

        if identifier_type == "doi":
            # Mantém '/' legível, mas protege caracteres como ? e #.
            return quote(
                f"doi:{identifier}",
                safe=":/()[];,+-._~",
            )

        raise ValueError(
            "Tipo de identificador inválido."
        )

    def fetch_single(
        self,
        identifier_type: str,
        identifier: str,
    ) -> ItemResult:

        path_identifier = (
            self._singleton_path(
                identifier_type,
                identifier,
            )
        )

        response = self._request(
            f"{BASE_URL}/works/{path_identifier}",
            params={
                "select": SELECT_FIELDS,
            },
            kind="singleton",
        )

        if response.status_code == 404:
            return ItemResult(
                requested_identifier=
                    identifier,

                identifier_type=
                    identifier_type,

                status="not_found",

                http_status=404,
            )

        if response.status_code != 200:
            raise APIRequestError(
                f"{identifier}: "
                f"HTTP {response.status_code}"
            )

        try:
            work = response.json()

        except ValueError as exc:
            raise APIRequestError(
                f"{identifier}: JSON inválido."
            ) from exc

        canonical_work_id = (
            normalize_openalex_id(
                work.get("id"),
                allow_bare_numeric=False,
            )
        )

        return ItemResult(
            requested_identifier=
                identifier,

            identifier_type=
                identifier_type,

            status="ok",

            work=work,

            canonical_work_id=
                canonical_work_id,

            http_status=200,
        )

    def fetch_batch(
        self,
        identifier_type: str,
        identifiers: list[str],
    ) -> list[ItemResult]:

        if not identifiers:
            return []

        if identifier_type == "doi":
            filter_name = "doi"

        elif identifier_type == "openalex":
            filter_name = "openalex"

        else:
            raise ValueError(
                "Tipo de identificador inválido."
            )

        filter_value = "|".join(
            identifiers
        )

        response = self._request(
            f"{BASE_URL}/works",
            params={
                "filter":
                    (
                        f"{filter_name}:"
                        f"{filter_value}"
                    ),

                "per_page":
                    BATCH_SIZE,

                "select":
                    SELECT_FIELDS,
            },
            kind="batch",
        )

        # Caso algum identificador torne o filtro inválido,
        # faz fallback gratuito por singleton.
        if response.status_code == 400:
            return [
                self.fetch_single(
                    identifier_type,
                    identifier,
                )
                for identifier in identifiers
            ]

        if response.status_code != 200:
            raise APIRequestError(
                "Batch retornou "
                f"HTTP {response.status_code}."
            )

        try:
            data = response.json()

        except ValueError as exc:
            raise APIRequestError(
                "Batch retornou JSON inválido."
            ) from exc

        meta = (
            data.get("meta")
            or {}
        )

        self._add_cost(
            as_float(
                meta.get(
                    "cost_usd"
                )
            )
        )

        returned: dict[
            str,
            dict[str, Any]
        ] = {}

        for work in (
            data.get("results")
            or []
        ):
            if identifier_type == "doi":
                key = normalize_doi(
                    work.get("doi")
                )

            else:
                key = normalize_openalex_id(
                    work.get("id"),
                    allow_bare_numeric=False,
                )

            if key:
                returned[key] = work

        results = []

        for identifier in identifiers:
            work = returned.get(
                identifier
            )

            if work is not None:
                canonical_work_id = (
                    normalize_openalex_id(
                        work.get("id"),
                        allow_bare_numeric=False,
                    )
                )

                results.append(
                    ItemResult(
                        requested_identifier=
                            identifier,

                        identifier_type=
                            identifier_type,

                        status="ok",

                        work=work,

                        canonical_work_id=
                            canonical_work_id,

                        http_status=200,
                    )
                )

            else:
                # Resolve casos não retornados pelo filtro.
                # Singleton é gratuito.
                results.append(
                    self.fetch_single(
                        identifier_type,
                        identifier,
                    )
                )

        return results


# ============================================================
# CACHE SQLITE
# ============================================================

class SQLiteCache:

    def __init__(
        self,
        path: Path,
    ) -> None:

        ensure_data_directories()

        self.path = path

        self.conn = sqlite3.connect(
            str(path),
            timeout=60,
        )

        self.conn.execute(
            "PRAGMA journal_mode=WAL"
        )

        self.conn.execute(
            "PRAGMA synchronous=NORMAL"
        )

        self.conn.execute(
            "PRAGMA temp_store=MEMORY"
        )

        self._create_schema()

    def _create_schema(
        self,
    ) -> None:

        # JSON do Work armazenado uma única vez.
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS work_payloads (
                work_id TEXT NOT NULL,
                profile TEXT NOT NULL,
                doi TEXT,
                payload BLOB NOT NULL,
                fetched_at TEXT NOT NULL,
                PRIMARY KEY (
                    work_id,
                    profile
                )
            )
            """
        )

        # Mapeamento DOI / OpenAlex ID -> Work.
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS identifier_map (
                identifier_type TEXT NOT NULL,
                identifier TEXT NOT NULL,
                profile TEXT NOT NULL,
                status TEXT NOT NULL,
                work_id TEXT,
                fetched_at TEXT NOT NULL,
                last_error TEXT,
                http_status INTEGER,
                PRIMARY KEY (
                    identifier_type,
                    identifier,
                    profile
                )
            )
            """
        )

        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS
                idx_identifier_map_status
            ON identifier_map (
                profile,
                identifier_type,
                status
            )
            """
        )

        self.conn.execute(
            """
            CREATE INDEX IF NOT EXISTS
                idx_identifier_map_work
            ON identifier_map (
                work_id,
                profile
            )
            """
        )

        self.conn.commit()

    @staticmethod
    def _compress(
        work: dict[str, Any],
    ) -> bytes:

        raw = json.dumps(
            work,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        return zlib.compress(
            raw,
            level=6,
        )

    @staticmethod
    def _decompress(
        payload: bytes,
    ) -> dict[str, Any]:

        raw = zlib.decompress(
            payload
        )

        return json.loads(
            raw.decode("utf-8")
        )

    def completed_map(
        self,
        identifier_type: str,
        identifiers: list[str],
    ) -> dict[str, str]:

        completed = {}

        for batch in chunks(
            identifiers,
            350,
        ):
            placeholders = ",".join(
                "?"
                for _ in batch
            )

            sql = f"""
                SELECT
                    m.identifier,
                    m.status
                FROM identifier_map AS m
                LEFT JOIN work_payloads AS w
                    ON w.work_id = m.work_id
                    AND w.profile = m.profile
                WHERE
                    m.profile = ?
                    AND m.identifier_type = ?
                    AND m.identifier
                        IN ({placeholders})
                    AND (
                        m.status = 'not_found'
                        OR (
                            m.status = 'ok'
                            AND w.work_id IS NOT NULL
                        )
                    )
            """

            params = [
                CACHE_PROFILE,
                identifier_type,
                *batch,
            ]

            cursor = self.conn.execute(
                sql,
                params,
            )

            for identifier, status in cursor:
                completed[
                    identifier
                ] = status

        return completed

    def _upsert_mapping(
        self,
        identifier_type: str,
        identifier: str,
        status: str,
        work_id: str | None,
        fetched_at: str,
        last_error: str | None,
        http_status: int | None,
    ) -> None:

        self.conn.execute(
            """
            INSERT INTO identifier_map (
                identifier_type,
                identifier,
                profile,
                status,
                work_id,
                fetched_at,
                last_error,
                http_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)

            ON CONFLICT (
                identifier_type,
                identifier,
                profile
            )
            DO UPDATE SET
                status = excluded.status,
                work_id = excluded.work_id,
                fetched_at = excluded.fetched_at,
                last_error = excluded.last_error,
                http_status = excluded.http_status
            """,
            (
                identifier_type,
                identifier,
                CACHE_PROFILE,
                status,
                work_id,
                fetched_at,
                last_error,
                http_status,
            ),
        )

    def save_results(
        self,
        results: list[ItemResult],
    ) -> None:

        now = utc_now()

        with self.conn:
            for result in results:

                if (
                    result.status == "ok"
                    and result.work is not None
                ):
                    work = result.work

                    canonical_work_id = (
                        result.canonical_work_id
                        or normalize_openalex_id(
                            work.get("id"),
                            allow_bare_numeric=False,
                        )
                    )

                    if not canonical_work_id:
                        # Resposta inesperada.
                        self._upsert_mapping(
                            identifier_type=
                                result.identifier_type,

                            identifier=
                                result.requested_identifier,

                            status="error",

                            work_id=None,

                            fetched_at=now,

                            last_error=
                                (
                                    "Resposta sem "
                                    "OpenAlex Work ID."
                                ),

                            http_status=
                                result.http_status,
                        )

                        continue

                    normalized_doi = (
                        normalize_doi(
                            work.get("doi")
                        )
                    )

                    payload = self._compress(
                        work
                    )

                    self.conn.execute(
                        """
                        INSERT INTO work_payloads (
                            work_id,
                            profile,
                            doi,
                            payload,
                            fetched_at
                        )
                        VALUES (?, ?, ?, ?, ?)

                        ON CONFLICT (
                            work_id,
                            profile
                        )
                        DO UPDATE SET
                            doi = excluded.doi,
                            payload = excluded.payload,
                            fetched_at =
                                excluded.fetched_at
                        """,
                        (
                            canonical_work_id,
                            CACHE_PROFILE,
                            normalized_doi,
                            payload,
                            now,
                        ),
                    )

                    # Identificador usado nesta execução.
                    self._upsert_mapping(
                        identifier_type=
                            result.identifier_type,

                        identifier=
                            result.requested_identifier,

                        status="ok",

                        work_id=
                            canonical_work_id,

                        fetched_at=now,

                        last_error=None,

                        http_status=
                            result.http_status,
                    )

                    # Alias OpenAlex sempre salvo.
                    self._upsert_mapping(
                        identifier_type=
                            "openalex",

                        identifier=
                            canonical_work_id,

                        status="ok",

                        work_id=
                            canonical_work_id,

                        fetched_at=now,

                        last_error=None,

                        http_status=200,
                    )

                    # Alias DOI, quando houver.
                    if normalized_doi:
                        self._upsert_mapping(
                            identifier_type=
                                "doi",

                            identifier=
                                normalized_doi,

                            status="ok",

                            work_id=
                                canonical_work_id,

                            fetched_at=now,

                            last_error=None,

                            http_status=200,
                        )

                else:
                    self._upsert_mapping(
                        identifier_type=
                            result.identifier_type,

                        identifier=
                            result.requested_identifier,

                        status=
                            result.status,

                        work_id=None,

                        fetched_at=now,

                        last_error=
                            result.error,

                        http_status=
                            result.http_status,
                    )

    def iter_records(
        self,
        identifier_type: str,
        identifiers: list[str],
    ) -> Iterable[
        tuple[
            str,
            str,
            dict[str, Any] | None,
        ]
    ]:

        for batch in chunks(
            identifiers,
            300,
        ):
            placeholders = ",".join(
                "?"
                for _ in batch
            )

            sql = f"""
                SELECT
                    m.identifier,
                    m.status,
                    w.payload
                FROM identifier_map AS m
                LEFT JOIN work_payloads AS w
                    ON w.work_id = m.work_id
                    AND w.profile = m.profile
                WHERE
                    m.profile = ?
                    AND m.identifier_type = ?
                    AND m.identifier
                        IN ({placeholders})
            """

            params = [
                CACHE_PROFILE,
                identifier_type,
                *batch,
            ]

            found = {}

            cursor = self.conn.execute(
                sql,
                params,
            )

            for (
                identifier,
                status,
                payload,
            ) in cursor:

                found[
                    identifier
                ] = (
                    status,
                    payload,
                )

            for identifier in batch:
                record = found.get(
                    identifier
                )

                if record is None:
                    continue

                status, payload = record

                work = None

                if (
                    status == "ok"
                    and payload is not None
                ):
                    try:
                        work = self._decompress(
                            payload
                        )

                    except Exception:
                        continue

                yield (
                    identifier,
                    status,
                    work,
                )

    def close(
        self,
    ) -> None:

        try:
            self.conn.close()

        except Exception:
            pass


# ============================================================
# EXPORTADOR
# ============================================================

class WorkExporter:
    """
    Exportador tabular no formato 1 Work = 1 linha.

    A saída segue a lógica do coletor OpenAlex por país usado no
    Projeto Uruguay, mas preserva os recursos do coletorOpenAlex:
    cache SQLite, retomada, consulta por DOI/Work ID e auditoria do
    identificador de entrada.

    Regras centrais:
    - um Work gera exatamente uma linha lógica e física no CSV;
    - autoria completa fica consolidada na coluna ``autores`` em JSON;
    - strings dentro e fora do JSON são sanitizadas para remover CR/LF/TAB;
    - nomes de colunas principais seguem o padrão já usado nos scripts
      do Projeto Uruguay (id_trabalho, titulo, ano, citacoes etc.).
    """

    HEADERS = [
        "input_identifier_type",
        "input_identifier",

        "id_trabalho",
        "doi",
        "titulo",
        "resumo",
        "ano",
        "data_publicacao",
        "tipo",
        "idioma",
        "citacoes",
        "is_retracted",
        "is_paratext",

        "dominio",
        "campo",
        "subcampo",
        "topico_primario",
        "score_topico_primario",

        "topico_secundario_1",
        "score_topico_secundario_1",
        "subcampo_secundario_1",
        "campo_secundario_1",
        "dominio_secundario_1",

        "topico_secundario_2",
        "score_topico_secundario_2",
        "subcampo_secundario_2",
        "campo_secundario_2",
        "dominio_secundario_2",

        "topico_secundario_3",
        "score_topico_secundario_3",
        "subcampo_secundario_3",
        "campo_secundario_3",
        "dominio_secundario_3",

        "source_id",
        "local_publicacao",
        "issn_l",
        "issn",
        "source_type",
        "host_organization",
        "host_organization_name",

        "acesso_aberto",
        "oa_status",
        "oa_url",
        "landing_page_url",
        "pdf_url",

        "paises_distintos",
        "instituicoes_distintas",
        "quantidade_autores",

        "fwci",
        "citation_normalized_percentile",
        "referenced_works_count",

        "autores",

        "topicos_json",
        "keywords",
        "funders",
        "awards",

        "created_date",
        "updated_date",
    ]

    @staticmethod
    def _abstract_from_inverted_index(work: dict[str, Any]) -> str:
        """Reconstrói o resumo OpenAlex e garante texto em uma linha."""
        inverted = work.get("abstract_inverted_index")

        if not isinstance(inverted, dict) or not inverted:
            return "-"

        max_position = -1

        for positions in inverted.values():
            if not isinstance(positions, list):
                continue

            for position in positions:
                if isinstance(position, int) and position > max_position:
                    max_position = position

        if max_position < 0:
            return "-"

        words = [""] * (max_position + 1)

        for token, positions in inverted.items():
            if not isinstance(positions, list):
                continue

            token_text = safe_string(token)

            for position in positions:
                if (
                    isinstance(position, int)
                    and 0 <= position <= max_position
                    and not words[position]
                ):
                    words[position] = token_text

        abstract = safe_string(" ".join(word for word in words if word))
        return abstract if abstract else "-"

    @staticmethod
    def _secondary_topics(work: dict[str, Any]) -> list[dict[str, Any]]:
        """Retorna os três primeiros topics exatamente na ordem da API."""
        output: list[dict[str, Any]] = []

        for topic in (work.get("topics") or [])[:3]:
            topic = topic or {}
            subfield = topic.get("subfield") or {}
            field = topic.get("field") or {}
            domain = topic.get("domain") or {}

            output.append(
                {
                    "nome": safe_string(topic.get("display_name")),
                    "score": safe_string(topic.get("score")),
                    "subcampo": safe_string(subfield.get("display_name")),
                    "campo": safe_string(field.get("display_name")),
                    "dominio": safe_string(domain.get("display_name")),
                }
            )

        while len(output) < 3:
            output.append(
                {
                    "nome": "",
                    "score": "",
                    "subcampo": "",
                    "campo": "",
                    "dominio": "",
                }
            )

        return output

    @staticmethod
    def _build_authors(work: dict[str, Any]) -> list[dict[str, Any]]:
        authors_out: list[dict[str, Any]] = []

        for authorship in (work.get("authorships") or []):
            author = authorship.get("author") or {}
            institutions_out: list[dict[str, Any]] = []

            for institution in (authorship.get("institutions") or []):
                lineage = [
                    normalize_openalex_entity_id(value)
                    for value in (institution.get("lineage") or [])
                    if safe_string(value)
                ]

                institutions_out.append(
                    {
                        "id": normalize_openalex_entity_id(
                            institution.get("id")
                        ),
                        "nome": safe_string(
                            institution.get("display_name")
                        ),
                        "pais": safe_string(
                            institution.get("country_code")
                        ),
                        "tipo": safe_string(
                            institution.get("type")
                        ),
                        "ror": safe_string(
                            institution.get("ror")
                        ),
                        "lineage": lineage,
                    }
                )

            authors_out.append(
                {
                    # Chaves mantidas por compatibilidade com os scripts
                    # anteriores do Projeto Uruguay.
                    "id_autor": normalize_openalex_entity_id(
                        author.get("id")
                    ),
                    "nome": safe_string(
                        author.get("display_name")
                    ),
                    "paises": [
                        safe_string(value)
                        for value in (authorship.get("countries") or [])
                        if safe_string(value)
                    ],
                    "instituicoes": institutions_out,

                    # Campos adicionais para auditoria.
                    "orcid": safe_string(author.get("orcid")),
                    "posicao": safe_string(
                        authorship.get("author_position")
                    ),
                    "correspondente": authorship.get(
                        "is_corresponding"
                    ),
                    "nome_bruto": safe_string(
                        authorship.get("raw_author_name")
                    ),
                    "afiliacoes_brutas": [
                        safe_string(value)
                        for value in (
                            authorship.get("raw_affiliation_strings")
                            or []
                        )
                        if safe_string(value)
                    ],
                }
            )

        return authors_out

    @staticmethod
    def _distinct_count(
        work: dict[str, Any],
        field_name: str,
        fallback_values: Iterable[str],
    ) -> str:
        value = work.get(field_name)

        if value is not None and safe_string(value) != "":
            return safe_string(value)

        fallback = {
            safe_string(item)
            for item in fallback_values
            if safe_string(item)
        }

        return safe_string(len(fallback))

    @classmethod
    def _row(
        cls,
        input_identifier: str,
        identifier_type: str,
        work: dict[str, Any],
    ) -> dict[str, Any]:

        primary_location = work.get("primary_location") or {}
        source = primary_location.get("source") or {}
        open_access = work.get("open_access") or {}

        primary_topic = work.get("primary_topic") or {}
        subfield = primary_topic.get("subfield") or {}
        field = primary_topic.get("field") or {}
        domain = primary_topic.get("domain") or {}

        secondary = cls._secondary_topics(work)
        authors = cls._build_authors(work)

        countries = []
        institution_ids = []

        for authorship in (work.get("authorships") or []):
            countries.extend(authorship.get("countries") or [])

            for institution in (authorship.get("institutions") or []):
                institution_id = normalize_openalex_entity_id(
                    institution.get("id")
                )
                if institution_id:
                    institution_ids.append(institution_id)

        return {
            "input_identifier_type": safe_string(identifier_type),
            "input_identifier": safe_string(input_identifier),

            "id_trabalho": normalize_openalex_id(
                work.get("id"),
                allow_bare_numeric=False,
            ) or "",
            "doi": normalize_doi(work.get("doi")) or "",
            "titulo": safe_string(
                work.get("title") or work.get("display_name")
            ),
            "resumo": cls._abstract_from_inverted_index(work),
            "ano": safe_string(work.get("publication_year")),
            "data_publicacao": safe_string(work.get("publication_date")),
            "tipo": safe_string(work.get("type")),
            "idioma": safe_string(work.get("language")),
            "citacoes": safe_string(work.get("cited_by_count")),
            "is_retracted": safe_string(work.get("is_retracted")),
            "is_paratext": safe_string(work.get("is_paratext")),

            "dominio": safe_string(domain.get("display_name")),
            "campo": safe_string(field.get("display_name")),
            "subcampo": safe_string(subfield.get("display_name")),
            "topico_primario": safe_string(
                primary_topic.get("display_name")
            ),
            "score_topico_primario": safe_string(
                primary_topic.get("score")
            ),

            "topico_secundario_1": secondary[0]["nome"],
            "score_topico_secundario_1": secondary[0]["score"],
            "subcampo_secundario_1": secondary[0]["subcampo"],
            "campo_secundario_1": secondary[0]["campo"],
            "dominio_secundario_1": secondary[0]["dominio"],

            "topico_secundario_2": secondary[1]["nome"],
            "score_topico_secundario_2": secondary[1]["score"],
            "subcampo_secundario_2": secondary[1]["subcampo"],
            "campo_secundario_2": secondary[1]["campo"],
            "dominio_secundario_2": secondary[1]["dominio"],

            "topico_secundario_3": secondary[2]["nome"],
            "score_topico_secundario_3": secondary[2]["score"],
            "subcampo_secundario_3": secondary[2]["subcampo"],
            "campo_secundario_3": secondary[2]["campo"],
            "dominio_secundario_3": secondary[2]["dominio"],

            "source_id": normalize_openalex_entity_id(source.get("id")),
            "local_publicacao": safe_string(source.get("display_name")),
            "issn_l": safe_string(source.get("issn_l")),
            "issn": compact_json(source.get("issn") or []),
            "source_type": safe_string(source.get("type")),
            "host_organization": normalize_openalex_entity_id(
                source.get("host_organization")
            ),
            "host_organization_name": safe_string(
                source.get("host_organization_name")
            ),

            "acesso_aberto": safe_string(open_access.get("is_oa")),
            "oa_status": safe_string(open_access.get("oa_status")),
            "oa_url": safe_string(open_access.get("oa_url")),
            "landing_page_url": safe_string(
                primary_location.get("landing_page_url")
            ),
            "pdf_url": safe_string(primary_location.get("pdf_url")),

            "paises_distintos": cls._distinct_count(
                work,
                "countries_distinct_count",
                countries,
            ),
            "instituicoes_distintas": cls._distinct_count(
                work,
                "institutions_distinct_count",
                institution_ids,
            ),
            "quantidade_autores": safe_string(len(authors)),

            "fwci": safe_string(work.get("fwci")),
            "citation_normalized_percentile": compact_json(
                work.get("citation_normalized_percentile")
            ),
            "referenced_works_count": safe_string(
                work.get("referenced_works_count")
            ),

            "autores": compact_json(authors),

            "topicos_json": compact_json(work.get("topics") or []),
            "keywords": compact_json(work.get("keywords") or []),
            "funders": compact_json(work.get("funders") or []),
            "awards": compact_json(work.get("awards") or []),

            "created_date": safe_string(work.get("created_date")),
            "updated_date": safe_string(work.get("updated_date")),
        }

    @classmethod
    def export(
        cls,
        cache: SQLiteCache,
        identifier_type: str,
        identifiers: list[str],
        output_path: Path,
        delimiter: str,
    ) -> int:

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temp_path = output_path.with_suffix(
            output_path.suffix + ".tmp"
        )

        rows_written = 0
        seen_work_ids: set[str] = set()

        with temp_path.open(
            "w",
            encoding="utf-8-sig",
            newline="",
        ) as file:

            writer = csv.DictWriter(
                file,
                fieldnames=cls.HEADERS,
                delimiter=delimiter,
                quoting=csv.QUOTE_ALL,
                quotechar='"',
                doublequote=True,
                lineterminator="\n",
                extrasaction="ignore",
            )

            writer.writeheader()

            for (
                input_identifier,
                status,
                work,
            ) in cache.iter_records(
                identifier_type=identifier_type,
                identifiers=identifiers,
            ):
                if status != "ok" or work is None:
                    continue

                work_id = normalize_openalex_id(
                    work.get("id"),
                    allow_bare_numeric=False,
                ) or safe_string(work.get("id"))

                # Um mesmo Work pode ser alcançado por DOI e/ou aliases;
                # ele deve aparecer somente uma vez na saída.
                if work_id and work_id in seen_work_ids:
                    continue

                if work_id:
                    seen_work_ids.add(work_id)

                row = cls._row(
                    input_identifier=input_identifier,
                    identifier_type=identifier_type,
                    work=work,
                )

                # Defesa final: nenhuma string exportada pode conter
                # quebra física de linha, inclusive JSON serializado.
                for key, value in list(row.items()):
                    if isinstance(value, str):
                        row[key] = safe_string(value)

                writer.writerow(row)
                rows_written += 1

        os.replace(
            str(temp_path),
            str(output_path),
        )

        return rows_written


# ============================================================
# CONFIGURAÇÃO DA EXECUÇÃO
# ============================================================

@dataclass
class ProcessingConfig:

    input_path: Path

    output_path: Path

    inspection: InputInspection

    column_index: int

    identifier_type: str

    api_key: str

    mode: str

    threads: int

    requests_per_second: int

    allow_prepaid: bool

    prepaid_cap_usd: float

    output_delimiter: str


# ============================================================
# CONTROLADOR
# ============================================================

class ProcessingController:

    def __init__(
        self,
        event_queue: queue.Queue,
    ) -> None:

        self.event_queue = (
            event_queue
        )

        self.thread = None

        self.stop_event = (
            threading.Event()
        )

        self.pause_event = (
            threading.Event()
        )

        self.running = False

    def emit(
        self,
        event: str,
        payload: Any = None,
    ) -> None:

        self.event_queue.put(
            (
                event,
                payload,
            )
        )

    def log(
        self,
        text: str,
    ) -> None:

        LOGGER.write(text)

        self.emit(
            "log",
            text,
        )

    def start(
        self,
        config: ProcessingConfig,
    ) -> None:

        if (
            self.thread
            and self.thread.is_alive()
        ):
            raise RuntimeError(
                "Já existe um processamento "
                "em andamento."
            )

        self.stop_event.clear()

        self.pause_event.clear()

        self.thread = threading.Thread(
            target=self._run,
            args=(config,),
            daemon=True,
        )

        self.thread.start()

    def toggle_pause(
        self,
    ) -> bool:

        if self.pause_event.is_set():
            self.pause_event.clear()

            self.emit(
                "state",
                "running",
            )

            return False

        self.pause_event.set()

        self.emit(
            "state",
            "paused",
        )

        return True

    def stop(
        self,
    ) -> None:

        self.stop_event.set()

        self.pause_event.clear()

    def _safe_single(
        self,
        client: OpenAlexClient,
        identifier_type: str,
        identifier: str,
    ) -> list[ItemResult]:

        try:
            return [
                client.fetch_single(
                    identifier_type,
                    identifier,
                )
            ]

        except CancelledByUser:
            raise

        except AuthenticationError:
            raise

        except Exception as exc:
            return [
                ItemResult(
                    requested_identifier=
                        identifier,

                    identifier_type=
                        identifier_type,

                    status="error",

                    error=str(exc),
                )
            ]

    def _safe_batch(
        self,
        client: OpenAlexClient,
        identifier_type: str,
        identifiers: list[str],
    ) -> list[ItemResult]:

        try:
            return client.fetch_batch(
                identifier_type,
                identifiers,
            )

        except CancelledByUser:
            raise

        except AuthenticationError:
            raise

        except RateLimitError:
            # Se um batch persistentemente atingir 429,
            # tenta os itens por singleton gratuito.
            output = []

            for identifier in identifiers:
                if self.stop_event.is_set():
                    raise CancelledByUser()

                output.extend(
                    self._safe_single(
                        client,
                        identifier_type,
                        identifier,
                    )
                )

            return output

        except Exception as exc:
            return [
                ItemResult(
                    requested_identifier=
                        identifier,

                    identifier_type=
                        identifier_type,

                    status="error",

                    error=str(exc),
                )
                for identifier
                in identifiers
            ]

    def _run(
        self,
        config: ProcessingConfig,
    ) -> None:

        cache = None

        try:
            self.running = True

            self.emit(
                "state",
                "analyzing",
            )

            self.log(
                f"Iniciando {APP_NAME} "
                f"{APP_VERSION}."
            )

            # ------------------------------------------------
            # ENTRADA
            # ------------------------------------------------

            loaded = (
                InputReader.load_identifiers(
                    inspection=
                        config.inspection,

                    column_index=
                        config.column_index,

                    identifier_type=
                        config.identifier_type,
                )
            )

            identifiers = (
                loaded.identifiers
            )

            if not identifiers:
                if (
                    config.identifier_type
                    == "doi"
                ):
                    hint = (
                        "\n\nO tipo selecionado é DOI. "
                        "Se o arquivo contém números "
                        "como 2975492377, selecione "
                        "'ID OpenAlex'."
                    )

                else:
                    hint = (
                        "\n\nO tipo selecionado é "
                        "ID OpenAlex."
                    )

                raise ValueError(
                    "Nenhum identificador válido "
                    "foi encontrado na coluna "
                    "selecionada."
                    + hint
                )

            self.emit(
                "input_stats",
                {
                    "rows_read":
                        loaded.rows_read,

                    "valid":
                        loaded.valid_values,

                    "unique":
                        len(identifiers),

                    "duplicates":
                        loaded.duplicates,

                    "invalid":
                        loaded.invalid_values,

                    "blank":
                        loaded.blank_values,

                    "normalized":
                        loaded.normalized_values,

                    "added_w":
                        loaded.added_w_values,

                    "identifier_type":
                        config.identifier_type,
                },
            )

            self.log(
                f"{format_int(len(identifiers))} "
                "identificadores únicos."
            )

            if loaded.duplicates:
                self.log(
                    f"{format_int(loaded.duplicates)} "
                    "duplicados removidos."
                )

            if loaded.invalid_values:
                self.log(
                    f"{format_int(loaded.invalid_values)} "
                    "valor(es) inválido(s) ignorado(s)."
                )

            if loaded.added_w_values:
                self.log(
                    "Prefixo W adicionado "
                    "automaticamente a "
                    f"{format_int(loaded.added_w_values)} "
                    "ID(s)."
                )

            # ------------------------------------------------
            # CACHE
            # ------------------------------------------------

            cache = SQLiteCache(
                DB_PATH
            )

            completed = (
                cache.completed_map(
                    identifier_type=
                        config.identifier_type,

                    identifiers=
                        identifiers,
                )
            )

            ok_count = sum(
                1
                for status
                in completed.values()
                if status == "ok"
            )

            not_found_count = sum(
                1
                for status
                in completed.values()
                if status == "not_found"
            )

            resolved_count = (
                ok_count
                + not_found_count
            )

            pending = deque(
                identifier
                for identifier
                in identifiers
                if identifier
                not in completed
            )

            error_count = 0

            self.emit(
                "progress",
                {
                    "total":
                        len(identifiers),

                    "resolved":
                        resolved_count,

                    "ok":
                        ok_count,

                    "not_found":
                        not_found_count,

                    "errors":
                        error_count,

                    "cached":
                        resolved_count,
                },
            )

            self.log(
                "Cache: "
                f"{format_int(resolved_count)} "
                "já resolvidos; "
                f"{format_int(len(pending))} "
                "pendentes."
            )

            # ------------------------------------------------
            # CLIENTE OPENALEX
            # ------------------------------------------------

            client = OpenAlexClient(
                api_key=
                    config.api_key,

                threads=
                    config.threads,

                requests_per_second=
                    config.requests_per_second,

                stop_event=
                    self.stop_event,
            )

            # Verifica chave e saldo.
            rate_status = (
                client.get_rate_limit()
            )

            self.emit(
                "rate_status",
                rate_status,
            )

            budget = BudgetGuard()

            budget.initialize(
                rate_status
            )

            snapshot = (
                budget.snapshot()
            )

            list_cost = (
                snapshot[
                    "list_cost"
                ]
            )

            batch_calls_needed = (
                math.ceil(
                    len(pending)
                    / BATCH_SIZE
                )
                if pending
                else 0
            )

            estimated_batch_cost = (
                batch_calls_needed
                * list_cost
            )

            self.log(
                "Se todos os pendentes fossem "
                "consultados em batch: "
                f"{format_int(batch_calls_needed)} "
                "chamada(s), custo de orçamento "
                "estimado "
                f"{format_usd(estimated_batch_cost)}."
            )

            mode_names = {
                "automatic":
                    "Automático",

                "economy":
                    "Econômico",

                "fast":
                    "Rápido",
            }

            self.log(
                "Estratégia: "
                f"{mode_names.get(config.mode, config.mode)}; "
                f"threads: {config.threads}; "
                "limite interno: "
                f"{config.requests_per_second} req/s."
            )

            self.emit(
                "state",
                "running",
            )

            # ------------------------------------------------
            # PROCESSAMENTO
            # ------------------------------------------------

            inflight: dict[
                Any,
                tuple[str, list[str]],
            ] = {}

            last_rate_refresh = (
                time.monotonic()
            )

            with ThreadPoolExecutor(
                max_workers=
                    config.threads,

                thread_name_prefix=
                    "coletorOpenAlex",
            ) as executor:

                while (
                    pending
                    or inflight
                ):

                    if (
                        self.stop_event.is_set()
                        and not inflight
                    ):
                        break

                    # Pausar = não cria novas consultas.
                    # Consultas já iniciadas podem terminar.
                    if (
                        not self.stop_event.is_set()
                        and not self.pause_event.is_set()
                    ):

                        while (
                            len(inflight)
                            < config.threads

                            and pending

                            and not
                            self.stop_event.is_set()

                            and not
                            self.pause_event.is_set()
                        ):
                            task_type = "single"

                            can_batch = (
                                len(pending)
                                >= 2
                            )

                            if (
                                can_batch
                                and config.mode
                                == "automatic"
                            ):
                                if (
                                    budget
                                    .reserve_free_batch()
                                ):
                                    task_type = (
                                        "batch"
                                    )

                            elif (
                                can_batch
                                and config.mode
                                == "fast"
                            ):
                                if (
                                    budget
                                    .reserve_fast_batch(
                                        allow_prepaid=
                                            config.allow_prepaid,

                                        prepaid_cap_usd=
                                            config.prepaid_cap_usd,
                                    )
                                ):
                                    task_type = (
                                        "batch"
                                    )

                            # ECONOMY sempre singleton.

                            if task_type == "batch":
                                identifiers_for_task = []

                                while (
                                    pending
                                    and len(
                                        identifiers_for_task
                                    )
                                    < BATCH_SIZE
                                ):
                                    identifiers_for_task.append(
                                        pending.popleft()
                                    )

                                future = executor.submit(
                                    self._safe_batch,

                                    client,

                                    config.identifier_type,

                                    identifiers_for_task,
                                )

                                inflight[
                                    future
                                ] = (
                                    "batch",
                                    identifiers_for_task,
                                )

                            else:
                                identifier = (
                                    pending.popleft()
                                )

                                future = executor.submit(
                                    self._safe_single,

                                    client,

                                    config.identifier_type,

                                    identifier,
                                )

                                inflight[
                                    future
                                ] = (
                                    "single",
                                    [identifier],
                                )

                    if not inflight:
                        if self.stop_event.is_set():
                            break

                        time.sleep(
                            0.1
                        )

                        continue

                    completed_futures, _ = wait(
                        list(
                            inflight.keys()
                        ),
                        timeout=0.25,
                        return_when=
                            FIRST_COMPLETED,
                    )

                    for future in completed_futures:

                        task_type, task_identifiers = (
                            inflight.pop(
                                future
                            )
                        )

                        try:
                            results = (
                                future.result()
                            )

                        except CancelledByUser:
                            continue

                        except AuthenticationError:
                            self.stop_event.set()
                            raise

                        except Exception as exc:
                            results = [
                                ItemResult(
                                    requested_identifier=
                                        identifier,

                                    identifier_type=
                                        config.identifier_type,

                                    status="error",

                                    error=str(exc),
                                )
                                for identifier
                                in task_identifiers
                            ]

                        cache.save_results(
                            results
                        )

                        for result in results:

                            if result.status == "ok":
                                ok_count += 1
                                resolved_count += 1

                            elif (
                                result.status
                                == "not_found"
                            ):
                                not_found_count += 1
                                resolved_count += 1

                            elif (
                                result.status
                                == "error"
                            ):
                                error_count += 1

                                self.log(
                                    "Erro em "
                                    f"{result.requested_identifier}: "
                                    f"{result.error or 'erro desconhecido'}"
                                )

                        self.emit(
                            "progress",
                            {
                                "total":
                                    len(
                                        identifiers
                                    ),

                                "resolved":
                                    resolved_count,

                                "ok":
                                    ok_count,

                                "not_found":
                                    not_found_count,

                                "errors":
                                    error_count,

                                "cached":
                                    None,
                            },
                        )

                        self.emit(
                            "api_stats",
                            client.stats(),
                        )

                    # ----------------------------------------
                    # ATUALIZA SALDOS REAIS
                    # ----------------------------------------

                    now = time.monotonic()

                    if (
                        now
                        - last_rate_refresh
                        >= RATE_REFRESH_SECONDS

                        and not
                        self.stop_event.is_set()
                    ):
                        try:
                            rate_status = (
                                client
                                .get_rate_limit()
                            )

                            budget.conservative_sync(
                                rate_status
                            )

                            self.emit(
                                "rate_status",
                                rate_status,
                            )

                        except CancelledByUser:
                            pass

                        except Exception as exc:
                            self.log(
                                "Não foi possível "
                                "atualizar os saldos "
                                "temporariamente: "
                                f"{exc}"
                            )

                        last_rate_refresh = (
                            now
                        )

            stopped = (
                self.stop_event.is_set()
            )

            # ------------------------------------------------
            # EXPORTAÇÃO
            # ------------------------------------------------

            self.emit(
                "state",
                "exporting",
            )

            if stopped:
                self.log(
                    "Processamento interrompido. "
                    "Gerando CSV parcial com "
                    "o cache disponível."
                )

            else:
                self.log(
                    "Consultas finalizadas. "
                    "Gerando CSV."
                )

            rows_written = (
                WorkExporter.export(
                    cache=cache,

                    identifier_type=
                        config.identifier_type,

                    identifiers=
                        identifiers,

                    output_path=
                        config.output_path,

                    delimiter=
                        config.output_delimiter,
                )
            )

            # Atualização final do saldo,
            # desde que não tenha sido cancelado.
            if not stopped:
                try:
                    final_rate = (
                        client
                        .get_rate_limit()
                    )

                    self.emit(
                        "rate_status",
                        final_rate,
                    )

                except Exception:
                    pass

            self.emit(
                "api_stats",
                client.stats(),
            )

            self.log(
                "CSV salvo em: "
                f"{config.output_path}"
            )

            self.log(
                f"{format_int(rows_written)} "
                "Work(s) exportado(s), uma linha por Work."
            )

            self.emit(
                "finished",
                {
                    "stopped":
                        stopped,

                    "output_path":
                        str(
                            config.output_path
                        ),

                    "rows_written":
                        rows_written,

                    "errors":
                        error_count,

                    "resolved":
                        resolved_count,

                    "total":
                        len(
                            identifiers
                        ),
                },
            )

            self.emit(
                "state",
                (
                    "stopped"
                    if stopped
                    else "done"
                ),
            )

        except AuthenticationError as exc:

            self.log(
                "Erro de autenticação: "
                f"{exc}"
            )

            self.emit(
                "fatal_error",
                "Erro de autenticação OpenAlex:\n\n"
                f"{exc}",
            )

            self.emit(
                "state",
                "error",
            )

        except Exception as exc:

            LOGGER.write(
                traceback.format_exc()
            )

            self.emit(
                "fatal_error",
                f"{type(exc).__name__}: {exc}",
            )

            self.emit(
                "state",
                "error",
            )

        finally:
            if cache is not None:
                cache.close()

            self.running = False


# ============================================================
# INTERFACE
# ============================================================

class ColetorOpenAlexApp:

    QUERY_TYPE_LABELS = {
        "DOI": "doi",
        "ID OpenAlex": "openalex",
    }

    QUERY_TYPE_REVERSE = {
        "doi": "DOI",
        "openalex": "ID OpenAlex",
    }

    def __init__(
        self,
        root: tk.Tk,
    ) -> None:

        self.root = root

        self.root.title(
            f"{APP_NAME} {APP_VERSION}"
        )

        self.root.geometry(
            "980x850"
        )

        self.root.minsize(
            900,
            740,
        )

        self.events = (
            queue.Queue()
        )

        self.controller = (
            ProcessingController(
                self.events
            )
        )

        self.inspection = None

        self.column_values = {}

        self.prepaid_bar_reference = (
            None
        )

        self.balance_api_key = None

        self._build_variables()

        self._build_ui()

        self.root.protocol(
            "WM_DELETE_WINDOW",
            self.on_close,
        )

        self.root.after(
            150,
            self.process_events,
        )

        self.set_state(
            "idle"
        )

    # --------------------------------------------------------

    def _build_variables(
        self,
    ) -> None:

        self.input_var = (
            tk.StringVar()
        )

        self.output_var = (
            tk.StringVar()
        )

        # DOI é o padrão pedido.
        self.query_type_var = (
            tk.StringVar(
                value="DOI"
            )
        )

        self.column_var = (
            tk.StringVar()
        )

        self.api_key_var = (
            tk.StringVar()
        )

        self.mode_var = (
            tk.StringVar(
                value="automatic"
            )
        )

        self.threads_var = (
            tk.IntVar(
                value=DEFAULT_THREADS
            )
        )

        self.rps_var = (
            tk.IntVar(
                value=DEFAULT_RPS
            )
        )

        self.allow_prepaid_var = (
            tk.BooleanVar(
                value=False
            )
        )

        self.prepaid_cap_var = (
            tk.StringVar(
                value="1,00"
            )
        )

        self.delimiter_var = (
            tk.StringVar(
                value=";"
            )
        )

        self.status_var = (
            tk.StringVar(
                value="Pronto."
            )
        )

        self.input_stats_var = (
            tk.StringVar(
                value=(
                    "Selecione um arquivo "
                    "de entrada."
                )
            )
        )

        self.work_label_var = (
            tk.StringVar(
                value="Trabalhos: —"
            )
        )

        self.daily_label_var = (
            tk.StringVar(
                value="Saldo diário: —"
            )
        )

        self.prepaid_label_var = (
            tk.StringVar(
                value="Saldo pré-pago: —"
            )
        )

        self.api_stats_var = (
            tk.StringVar(
                value="Consultas: —"
            )
        )

    # --------------------------------------------------------

    def _build_ui(
        self,
    ) -> None:

        outer = ttk.Frame(
            self.root,
            padding=12,
        )

        outer.pack(
            fill="both",
            expand=True,
        )

        # ====================================================
        # ENTRADA
        # ====================================================

        input_frame = ttk.LabelFrame(
            outer,
            text="Entrada e saída",
            padding=10,
        )

        input_frame.pack(
            fill="x",
            pady=(0, 8),
        )

        input_frame.columnconfigure(
            1,
            weight=1,
        )

        ttk.Label(
            input_frame,
            text="Arquivo de entrada:",
        ).grid(
            row=0,
            column=0,
            sticky="w",
            padx=(0, 8),
            pady=4,
        )

        ttk.Entry(
            input_frame,
            textvariable=
                self.input_var,
        ).grid(
            row=0,
            column=1,
            sticky="ew",
            pady=4,
        )

        ttk.Button(
            input_frame,
            text="Selecionar...",
            command=
                self.browse_input,
        ).grid(
            row=0,
            column=2,
            padx=(8, 0),
            pady=4,
        )

        # Tipo de identificador.

        ttk.Label(
            input_frame,
            text="Consultar por:",
        ).grid(
            row=1,
            column=0,
            sticky="w",
            padx=(0, 8),
            pady=4,
        )

        self.query_type_combo = (
            ttk.Combobox(
                input_frame,

                textvariable=
                    self.query_type_var,

                state="readonly",

                values=(
                    "DOI",
                    "ID OpenAlex",
                ),
            )
        )

        self.query_type_combo.grid(
            row=1,
            column=1,
            sticky="w",
            pady=4,
        )

        # Coluna.

        ttk.Label(
            input_frame,
            text=(
                "Coluna com DOI "
                "ou Work ID:"
            ),
        ).grid(
            row=2,
            column=0,
            sticky="w",
            padx=(0, 8),
            pady=4,
        )

        self.column_combo = (
            ttk.Combobox(
                input_frame,

                textvariable=
                    self.column_var,

                state="readonly",

                values=(),
            )
        )

        self.column_combo.grid(
            row=2,
            column=1,
            sticky="ew",
            pady=4,
        )

        ttk.Label(
            input_frame,
            textvariable=
                self.input_stats_var,
        ).grid(
            row=3,
            column=1,
            columnspan=2,
            sticky="w",
            pady=(2, 6),
        )

        # Saída.

        ttk.Label(
            input_frame,
            text="Arquivo CSV de saída:",
        ).grid(
            row=4,
            column=0,
            sticky="w",
            padx=(0, 8),
            pady=4,
        )

        ttk.Entry(
            input_frame,
            textvariable=
                self.output_var,
        ).grid(
            row=4,
            column=1,
            sticky="ew",
            pady=4,
        )

        ttk.Button(
            input_frame,
            text="Escolher...",
            command=
                self.browse_output,
        ).grid(
            row=4,
            column=2,
            padx=(8, 0),
            pady=4,
        )

        ttk.Label(
            input_frame,
            text="Separador da saída:",
        ).grid(
            row=5,
            column=0,
            sticky="w",
            padx=(0, 8),
            pady=4,
        )

        ttk.Combobox(
            input_frame,

            textvariable=
                self.delimiter_var,

            state="readonly",

            values=(
                ",",
                ";",
                "\\t",
            ),

            width=8,
        ).grid(
            row=5,
            column=1,
            sticky="w",
            pady=4,
        )

        # ====================================================
        # OPENALEX
        # ====================================================

        api_frame = ttk.LabelFrame(
            outer,
            text="OpenAlex",
            padding=10,
        )

        api_frame.pack(
            fill="x",
            pady=(0, 8),
        )

        api_frame.columnconfigure(
            1,
            weight=1,
        )

        ttk.Label(
            api_frame,
            text="API key:",
        ).grid(
            row=0,
            column=0,
            sticky="w",
            padx=(0, 8),
            pady=4,
        )

        self.key_entry = (
            ttk.Entry(
                api_frame,

                textvariable=
                    self.api_key_var,

                show="•",
            )
        )

        self.key_entry.grid(
            row=0,
            column=1,
            sticky="ew",
            pady=4,
        )

        ttk.Button(
            api_frame,

            text="Testar chave / saldo",

            command=
                self.test_key,
        ).grid(
            row=0,
            column=2,
            padx=(8, 0),
            pady=4,
        )

        # ====================================================
        # ESTRATÉGIA
        # ====================================================

        strategy = ttk.LabelFrame(
            outer,
            text="Estratégia de consulta",
            padding=10,
        )

        strategy.pack(
            fill="x",
            pady=(0, 8),
        )

        ttk.Radiobutton(
            strategy,

            text=(
                "Automático — batch enquanto "
                "houver orçamento diário; "
                "depois singleton gratuito"
            ),

            variable=
                self.mode_var,

            value=
                "automatic",
        ).grid(
            row=0,
            column=0,
            columnspan=4,
            sticky="w",
            pady=2,
        )

        ttk.Radiobutton(
            strategy,

            text=(
                "Econômico — somente "
                "singleton gratuito"
            ),

            variable=
                self.mode_var,

            value=
                "economy",
        ).grid(
            row=1,
            column=0,
            columnspan=4,
            sticky="w",
            pady=2,
        )

        ttk.Radiobutton(
            strategy,

            text=(
                "Rápido — batch; pode "
                "consumir saldo pré-pago "
                "se autorizado"
            ),

            variable=
                self.mode_var,

            value=
                "fast",
        ).grid(
            row=2,
            column=0,
            columnspan=4,
            sticky="w",
            pady=2,
        )

        ttk.Label(
            strategy,
            text="Threads:",
        ).grid(
            row=3,
            column=0,
            sticky="w",
            pady=(8, 2),
        )

        ttk.Spinbox(
            strategy,

            from_=1,

            to=MAX_THREADS,

            textvariable=
                self.threads_var,

            width=7,
        ).grid(
            row=3,
            column=1,
            sticky="w",
            padx=(5, 20),
            pady=(8, 2),
        )

        ttk.Label(
            strategy,
            text="Limite interno req/s:",
        ).grid(
            row=3,
            column=2,
            sticky="w",
            pady=(8, 2),
        )

        ttk.Spinbox(
            strategy,

            from_=1,

            to=MAX_RPS,

            textvariable=
                self.rps_var,

            width=7,
        ).grid(
            row=3,
            column=3,
            sticky="w",
            padx=(5, 0),
            pady=(8, 2),
        )

        ttk.Checkbutton(
            strategy,

            text=(
                "Permitir saldo pré-pago "
                "no modo Rápido"
            ),

            variable=
                self.allow_prepaid_var,
        ).grid(
            row=4,
            column=0,
            columnspan=2,
            sticky="w",
            pady=(8, 2),
        )

        ttk.Label(
            strategy,

            text=(
                "Máximo pré-pago nesta "
                "execução (US$):"
            ),
        ).grid(
            row=4,
            column=2,
            sticky="e",
            pady=(8, 2),
        )

        ttk.Entry(
            strategy,

            textvariable=
                self.prepaid_cap_var,

            width=10,
        ).grid(
            row=4,
            column=3,
            sticky="w",
            padx=(5, 0),
            pady=(8, 2),
        )

        # ====================================================
        # BARRAS
        # ====================================================

        progress = ttk.LabelFrame(
            outer,
            text="Progresso e saldos",
            padding=10,
        )

        progress.pack(
            fill="x",
            pady=(0, 8),
        )

        # Trabalhos.

        ttk.Label(
            progress,
            textvariable=
                self.work_label_var,
        ).pack(
            anchor="w"
        )

        self.work_bar = (
            ttk.Progressbar(
                progress,
                maximum=1,
                value=0,
            )
        )

        self.work_bar.pack(
            fill="x",
            pady=(2, 8),
        )

        # Diário.

        ttk.Label(
            progress,
            textvariable=
                self.daily_label_var,
        ).pack(
            anchor="w"
        )

        self.daily_bar = (
            ttk.Progressbar(
                progress,
                maximum=1,
                value=0,
            )
        )

        self.daily_bar.pack(
            fill="x",
            pady=(2, 8),
        )

        # Pré-pago.

        ttk.Label(
            progress,
            textvariable=
                self.prepaid_label_var,
        ).pack(
            anchor="w"
        )

        self.prepaid_bar = (
            ttk.Progressbar(
                progress,
                maximum=1,
                value=0,
            )
        )

        self.prepaid_bar.pack(
            fill="x",
            pady=(2, 8),
        )

        ttk.Label(
            progress,
            textvariable=
                self.api_stats_var,
        ).pack(
            anchor="w"
        )

        # ====================================================
        # BOTÕES
        # ====================================================

        controls = ttk.Frame(
            outer
        )

        controls.pack(
            fill="x",
            pady=(0, 8),
        )

        self.start_button = (
            ttk.Button(
                controls,

                text="Iniciar",

                command=
                    self.start_processing,
            )
        )

        self.start_button.pack(
            side="left"
        )

        self.pause_button = (
            ttk.Button(
                controls,

                text="Pausar",

                command=
                    self.pause_processing,
            )
        )

        self.pause_button.pack(
            side="left",
            padx=(8, 0),
        )

        self.stop_button = (
            ttk.Button(
                controls,

                text="Parar",

                command=
                    self.stop_processing,
            )
        )

        self.stop_button.pack(
            side="left",
            padx=(8, 0),
        )

        ttk.Button(
            controls,

            text="Abrir pasta data",

            command=lambda:
                open_folder(
                    DATA_DIR
                ),
        ).pack(
            side="right"
        )

        # ====================================================
        # STATUS
        # ====================================================

        ttk.Label(
            outer,
            textvariable=
                self.status_var,
        ).pack(
            fill="x",
            pady=(0, 4),
        )

        # ====================================================
        # LOG
        # ====================================================

        log_frame = ttk.LabelFrame(
            outer,
            text="Log",
            padding=6,
        )

        log_frame.pack(
            fill="both",
            expand=True,
        )

        self.log_text = tk.Text(
            log_frame,
            height=10,
            wrap="word",
            state="disabled",
        )

        scrollbar = ttk.Scrollbar(
            log_frame,
            orient="vertical",
            command=
                self.log_text.yview,
        )

        self.log_text.configure(
            yscrollcommand=
                scrollbar.set
        )

        self.log_text.pack(
            side="left",
            fill="both",
            expand=True,
        )

        scrollbar.pack(
            side="right",
            fill="y",
        )

    # --------------------------------------------------------

    def append_log(
        self,
        text: str,
    ) -> None:

        stamp = (
            datetime.now()
            .strftime("%H:%M:%S")
        )

        self.log_text.configure(
            state="normal"
        )

        self.log_text.insert(
            "end",
            f"[{stamp}] {text}\n",
        )

        self.log_text.see(
            "end"
        )

        self.log_text.configure(
            state="disabled"
        )

    # --------------------------------------------------------

    def browse_input(
        self,
    ) -> None:

        filename = (
            filedialog.askopenfilename(
                title=(
                    "Selecionar arquivo "
                    "com DOI ou Work ID"
                ),

                filetypes=[
                    (
                        "CSV e texto",
                        "*.csv *.txt *.tsv"
                    ),
                    (
                        "CSV",
                        "*.csv"
                    ),
                    (
                        "Texto",
                        "*.txt"
                    ),
                    (
                        "Todos os arquivos",
                        "*.*"
                    ),
                ],
            )
        )

        if not filename:
            return

        self.input_var.set(
            filename
        )

        try:
            self.inspection = (
                InputReader.inspect(
                    filename
                )
            )

        except Exception as exc:
            messagebox.showerror(
                APP_NAME,

                "Não foi possível "
                "analisar o arquivo:\n\n"
                f"{exc}",
            )

            return

        self.column_values = {}

        combo_values = []

        for profile in (
            self.inspection.profiles
        ):
            type_hint = ""

            if (
                profile.detected_type
                == "doi"
            ):
                type_hint = (
                    "  [provável DOI]"
                )

            elif (
                profile.detected_type
                == "openalex"
            ):
                type_hint = (
                    "  [provável ID OpenAlex]"
                )

            label = (
                f"{profile.index + 1}: "
                f"{profile.name}"
                f"{type_hint}"
            )

            combo_values.append(
                label
            )

            self.column_values[
                label
            ] = profile.index

        self.column_combo.configure(
            values=combo_values
        )

        suggested_index = (
            self.inspection
            .suggested_column_index
        )

        selected_label = None

        if suggested_index is not None:
            for label, index in (
                self.column_values.items()
            ):
                if (
                    index
                    == suggested_index
                ):
                    selected_label = label
                    break

        if (
            selected_label is None
            and combo_values
        ):
            selected_label = (
                combo_values[0]
            )

        if selected_label:
            self.column_var.set(
                selected_label
            )

        detected_type = (
            self.inspection
            .suggested_identifier_type
        )

        if detected_type == "doi":
            probable = "DOI"

        elif detected_type == "openalex":
            probable = "ID OpenAlex"

        else:
            probable = "não determinado"

        delimiter_text = (
            "texto/linha"
            if self.inspection.delimiter
            is None

            else repr(
                self.inspection.delimiter
            )
        )

        selected_column_name = "—"

        if suggested_index is not None:
            selected_column_name = (
                self.inspection.headers[
                    suggested_index
                ]
            )

        self.input_stats_var.set(
            "Coluna detectada: "
            f"{selected_column_name} | "
            "conteúdo provável: "
            f"{probable} | "
            "codificação: "
            f"{self.inspection.encoding} | "
            "separador: "
            f"{delimiter_text}"
        )

        # DOI permanece o padrão da aplicação.
        # A detecção apenas informa o provável tipo.

        if not self.output_var.get():
            input_path = Path(
                filename
            )

            suggested_output = (
                input_path.with_name(
                    input_path.stem
                    + "_OpenAlex.csv"
                )
            )

            self.output_var.set(
                str(
                    suggested_output
                )
            )

    # --------------------------------------------------------

    def browse_output(
        self,
    ) -> None:

        filename = (
            filedialog.asksaveasfilename(
                title="Salvar CSV",

                defaultextension=
                    ".csv",

                filetypes=[
                    (
                        "CSV",
                        "*.csv"
                    ),
                    (
                        "Todos os arquivos",
                        "*.*"
                    ),
                ],
            )
        )

        if filename:
            self.output_var.set(
                filename
            )

    # --------------------------------------------------------

    def get_identifier_type(
        self,
    ) -> str:

        label = (
            self.query_type_var.get()
        )

        return (
            self.QUERY_TYPE_LABELS[
                label
            ]
        )

    # --------------------------------------------------------

    def get_delimiter(
        self,
    ) -> str:

        value = (
            self.delimiter_var.get()
        )

        if value == "\\t":
            return "\t"

        return value

    # --------------------------------------------------------

    def _reset_balance_reference_if_needed(
        self,
        api_key: str,
    ) -> None:

        if (
            self.balance_api_key
            != api_key
        ):
            self.balance_api_key = (
                api_key
            )

            self.prepaid_bar_reference = (
                None
            )

    # --------------------------------------------------------

    def test_key(
        self,
    ) -> None:

        if requests is None:
            messagebox.showerror(
                APP_NAME,

                "A biblioteca 'requests' "
                "não está instalada.\n\n"
                "Execute:\n"
                "pip install requests",
            )

            return

        api_key = (
            self.api_key_var
            .get()
            .strip()
        )

        if not api_key:
            messagebox.showwarning(
                APP_NAME,
                "Informe a API key OpenAlex.",
            )

            return

        self._reset_balance_reference_if_needed(
            api_key
        )

        self.status_var.set(
            "Consultando saldo OpenAlex..."
        )

        def worker():

            stop_event = (
                threading.Event()
            )

            try:
                client = OpenAlexClient(
                    api_key=api_key,
                    threads=1,
                    requests_per_second=10,
                    stop_event=stop_event,
                )

                status = (
                    client.get_rate_limit()
                )

                self.events.put(
                    (
                        "rate_status",
                        status,
                    )
                )

                self.events.put(
                    (
                        "key_test_ok",
                        "Chave válida. "
                        "Saldos atualizados.",
                    )
                )

            except Exception as exc:

                self.events.put(
                    (
                        "key_test_error",
                        str(exc),
                    )
                )

        threading.Thread(
            target=worker,
            daemon=True,
        ).start()

    # --------------------------------------------------------

    def _profile_for_selected_column(
        self,
    ) -> ColumnProfile | None:

        if self.inspection is None:
            return None

        label = (
            self.column_var.get()
        )

        column_index = (
            self.column_values.get(
                label
            )
        )

        if column_index is None:
            return None

        for profile in (
            self.inspection.profiles
        ):
            if (
                profile.index
                == column_index
            ):
                return profile

        return None

    # --------------------------------------------------------

    def start_processing(
        self,
    ) -> None:

        if requests is None:
            messagebox.showerror(
                APP_NAME,

                "A biblioteca 'requests' "
                "não está instalada.\n\n"
                "Execute:\n"
                "pip install requests",
            )

            return

        input_text = (
            self.input_var.get()
            .strip()
        )

        output_text = (
            self.output_var.get()
            .strip()
        )

        api_key = (
            self.api_key_var.get()
            .strip()
        )

        if not input_text:
            messagebox.showwarning(
                APP_NAME,
                "Selecione o arquivo "
                "de entrada.",
            )

            return

        if not output_text:
            messagebox.showwarning(
                APP_NAME,
                "Selecione o arquivo "
                "de saída.",
            )

            return

        if not api_key:
            messagebox.showwarning(
                APP_NAME,
                "Informe a API key OpenAlex.",
            )

            return

        self._reset_balance_reference_if_needed(
            api_key
        )

        input_path = Path(
            input_text
        )

        output_path = Path(
            output_text
        )

        try:
            if (
                input_path.resolve()
                == output_path.resolve()
            ):
                messagebox.showerror(
                    APP_NAME,
                    "Entrada e saída não podem "
                    "ser o mesmo arquivo.",
                )

                return

        except Exception:
            pass

        if self.inspection is None:
            try:
                self.inspection = (
                    InputReader.inspect(
                        input_path
                    )
                )

            except Exception as exc:
                messagebox.showerror(
                    APP_NAME,
                    str(exc),
                )

                return

        column_label = (
            self.column_var.get()
        )

        if (
            column_label
            not in self.column_values
        ):
            messagebox.showwarning(
                APP_NAME,
                "Selecione a coluna com "
                "DOI ou Work ID.",
            )

            return

        column_index = (
            self.column_values[
                column_label
            ]
        )

        identifier_type = (
            self.get_identifier_type()
        )

        # --------------------------------------------
        # Confere se o tipo escolhido parece coerente
        # com a coluna.
        # --------------------------------------------

        profile = (
            self._profile_for_selected_column()
        )

        if (
            profile is not None
            and profile.detected_type
            and profile.detected_type
            != identifier_type
        ):
            detected_label = (
                self.QUERY_TYPE_REVERSE[
                    profile.detected_type
                ]
            )

            selected_label = (
                self.QUERY_TYPE_REVERSE[
                    identifier_type
                ]
            )

            change = (
                messagebox.askyesno(
                    APP_NAME,

                    "A coluna selecionada parece "
                    f"conter {detected_label}, "
                    "mas o tipo de consulta está "
                    f"configurado como {selected_label}.\n\n"
                    f"Deseja alterar para {detected_label}?",
                )
            )

            if change:
                identifier_type = (
                    profile.detected_type
                )

                self.query_type_var.set(
                    detected_label
                )

        try:
            threads = int(
                self.threads_var.get()
            )

            rps = int(
                self.rps_var.get()
            )

            threads = min(
                MAX_THREADS,
                max(
                    1,
                    threads,
                ),
            )

            rps = min(
                MAX_RPS,
                max(
                    1,
                    rps,
                ),
            )

        except Exception:
            messagebox.showerror(
                APP_NAME,
                "Threads e req/s devem "
                "ser números inteiros.",
            )

            return

        try:
            prepaid_cap = float(
                self.prepaid_cap_var
                .get()
                .strip()
                .replace(
                    ",",
                    ".",
                )
            )

        except ValueError:
            messagebox.showerror(
                APP_NAME,
                "O limite de saldo pré-pago "
                "é inválido.",
            )

            return

        if prepaid_cap < 0:
            messagebox.showerror(
                APP_NAME,
                "O limite de saldo pré-pago "
                "não pode ser negativo.",
            )

            return

        config = ProcessingConfig(
            input_path=
                input_path,

            output_path=
                output_path,

            inspection=
                self.inspection,

            column_index=
                column_index,

            identifier_type=
                identifier_type,

            api_key=
                api_key,

            mode=
                self.mode_var.get(),

            threads=
                threads,

            requests_per_second=
                rps,

            allow_prepaid=
                self.allow_prepaid_var.get(),

            prepaid_cap_usd=
                prepaid_cap,

            output_delimiter=
                self.get_delimiter(),
        )

        try:
            self.controller.start(
                config
            )

        except Exception as exc:
            messagebox.showerror(
                APP_NAME,
                str(exc),
            )

    # --------------------------------------------------------

    def pause_processing(
        self,
    ) -> None:

        paused = (
            self.controller
            .toggle_pause()
        )

        self.pause_button.configure(
            text=(
                "Continuar"
                if paused
                else "Pausar"
            )
        )

    # --------------------------------------------------------

    def stop_processing(
        self,
    ) -> None:

        if not self.controller.running:
            return

        self.controller.stop()

        self.status_var.set(
            "Interrompendo novas consultas..."
        )

    # --------------------------------------------------------

    @staticmethod
    def _format_reset_time(
        value: Any,
    ) -> str:

        if not value:
            return ""

        try:
            text = str(
                value
            ).replace(
                "Z",
                "+00:00",
            )

            dt = (
                datetime
                .fromisoformat(
                    text
                )
                .astimezone()
            )

            return (
                dt.strftime(
                    "%d/%m/%Y %H:%M"
                )
            )

        except Exception:
            return str(value)

    # --------------------------------------------------------

    def update_rate_status(
        self,
        data: dict[str, Any],
    ) -> None:

        rate = (
            data.get(
                "rate_limit"
            )
            or {}
        )

        # --------------------------------------------
        # SALDO DIÁRIO
        # --------------------------------------------

        daily_budget = max(
            0.0,
            as_float(
                rate.get(
                    "daily_budget_usd"
                )
            ),
        )

        daily_remaining = max(
            0.0,
            as_float(
                rate.get(
                    "daily_remaining_usd"
                )
            ),
        )

        daily_used = max(
            0.0,
            as_float(
                rate.get(
                    "daily_used_usd"
                )
            ),
        )

        daily_max = max(
            daily_budget,
            0.0000001,
        )

        self.daily_bar.configure(
            maximum=daily_max
        )

        self.daily_bar["value"] = min(
            daily_remaining,
            daily_max,
        )

        daily_pct = (
            (
                daily_remaining
                / daily_budget
                * 100
            )
            if daily_budget > 0
            else 0.0
        )

        reset_text = (
            self._format_reset_time(
                rate.get(
                    "resets_at"
                )
            )
        )

        reset_suffix = (
            f" | reset: {reset_text}"
            if reset_text
            else ""
        )

        self.daily_label_var.set(
            "Saldo diário: "
            f"{format_usd(daily_remaining)} / "
            f"{format_usd(daily_budget)} "
            f"({daily_pct:.1f}% restante) | "
            "usado: "
            f"{format_usd(daily_used)}"
            f"{reset_suffix}"
        )

        # --------------------------------------------
        # SALDO PRÉ-PAGO
        # --------------------------------------------

        prepaid_balance = max(
            0.0,
            as_float(
                rate.get(
                    "prepaid_balance_usd"
                )
            ),
        )

        prepaid_remaining = max(
            0.0,
            as_float(
                rate.get(
                    "prepaid_remaining_usd"
                )
            ),
        )

        candidate_reference = max(
            prepaid_balance,
            prepaid_remaining,
        )

        # A referência fica congelada no primeiro saldo
        # observado para a chave. Assim a barra realmente
        # esvazia durante a execução.
        if (
            self.prepaid_bar_reference
            is None
        ):
            self.prepaid_bar_reference = (
                candidate_reference
            )

        elif (
            candidate_reference
            > self.prepaid_bar_reference
        ):
            # Caso o usuário compre créditos enquanto
            # o programa está aberto.
            self.prepaid_bar_reference = (
                candidate_reference
            )

        reference = max(
            self.prepaid_bar_reference
            or 0.0,

            0.0000001,
        )

        self.prepaid_bar.configure(
            maximum=reference
        )

        self.prepaid_bar["value"] = min(
            prepaid_remaining,
            reference,
        )

        prepaid_pct = (
            (
                prepaid_remaining
                / reference
                * 100
            )
            if reference > 0
            else 0.0
        )

        expires = (
            self._format_reset_time(
                rate.get(
                    "prepaid_expires_at"
                )
            )
        )

        expires_suffix = (
            f" | expira: {expires}"
            if expires
            else ""
        )

        self.prepaid_label_var.set(
            "Saldo pré-pago: "
            f"{format_usd(prepaid_remaining)} | "
            "referência da sessão: "
            f"{format_usd(reference)} "
            f"({prepaid_pct:.1f}% restante)"
            f"{expires_suffix}"
        )

    # --------------------------------------------------------

    def update_progress(
        self,
        data: dict[str, Any],
    ) -> None:

        total = int(
            data.get("total")
            or 0
        )

        resolved = int(
            data.get("resolved")
            or 0
        )

        ok_count = int(
            data.get("ok")
            or 0
        )

        not_found = int(
            data.get("not_found")
            or 0
        )

        errors = int(
            data.get("errors")
            or 0
        )

        self.work_bar.configure(
            maximum=max(
                1,
                total,
            )
        )

        self.work_bar["value"] = (
            resolved
        )

        pct = (
            (
                resolved
                / total
                * 100
            )
            if total > 0
            else 0.0
        )

        self.work_label_var.set(
            "Trabalhos: "
            f"{format_int(resolved)} / "
            f"{format_int(total)} "
            f"({pct:.1f}%) | "
            "OK: "
            f"{format_int(ok_count)} | "
            "não encontrados: "
            f"{format_int(not_found)} | "
            "erros: "
            f"{format_int(errors)}"
        )

    # --------------------------------------------------------

    def update_api_stats(
        self,
        data: dict[str, Any],
    ) -> None:

        self.api_stats_var.set(
            "Consultas HTTP: "
            f"{format_int(int(data.get('http_requests', 0)))}"
            " | singleton: "
            f"{format_int(int(data.get('singleton_requests', 0)))}"
            " | batch: "
            f"{format_int(int(data.get('batch_requests', 0)))}"
            " | retries: "
            f"{format_int(int(data.get('retry_requests', 0)))}"
            " | custo reportado: "
            f"{format_usd(data.get('reported_cost_usd', 0))}"
        )

    # --------------------------------------------------------

    def set_state(
        self,
        state: str,
    ) -> None:

        if state == "idle":

            self.status_var.set(
                "Pronto."
            )

            self.start_button.configure(
                state="normal"
            )

            self.pause_button.configure(
                state="disabled",
                text="Pausar",
            )

            self.stop_button.configure(
                state="disabled"
            )

        elif state == "analyzing":

            self.status_var.set(
                "Lendo identificadores "
                "e verificando cache..."
            )

            self.start_button.configure(
                state="disabled"
            )

            self.pause_button.configure(
                state="disabled"
            )

            self.stop_button.configure(
                state="normal"
            )

        elif state == "running":

            self.status_var.set(
                "Processando..."
            )

            self.start_button.configure(
                state="disabled"
            )

            self.pause_button.configure(
                state="normal",
                text="Pausar",
            )

            self.stop_button.configure(
                state="normal"
            )

        elif state == "paused":

            self.status_var.set(
                "Pausado. Consultas já "
                "iniciadas podem terminar."
            )

            self.pause_button.configure(
                state="normal",
                text="Continuar",
            )

        elif state == "exporting":

            self.status_var.set(
                "Gerando CSV a partir "
                "do cache..."
            )

            self.pause_button.configure(
                state="disabled"
            )

        elif state in {
            "done",
            "stopped",
            "error",
        }:

            self.start_button.configure(
                state="normal"
            )

            self.pause_button.configure(
                state="disabled",
                text="Pausar",
            )

            self.stop_button.configure(
                state="disabled"
            )

            if state == "done":
                self.status_var.set(
                    "Concluído."
                )

            elif state == "stopped":
                self.status_var.set(
                    "Interrompido. "
                    "O cache foi preservado."
                )

            else:
                self.status_var.set(
                    "Processamento encerrado "
                    "com erro."
                )

    # --------------------------------------------------------

    def process_events(
        self,
    ) -> None:

        try:
            while True:
                event, payload = (
                    self.events.get_nowait()
                )

                if event == "log":

                    self.append_log(
                        str(payload)
                    )

                elif event == "state":

                    self.set_state(
                        str(payload)
                    )

                elif event == "input_stats":

                    type_label = (
                        "DOI"
                        if payload[
                            "identifier_type"
                        ]
                        == "doi"

                        else "ID OpenAlex"
                    )

                    extra_w = ""

                    if payload[
                        "identifier_type"
                    ] == "openalex":

                        extra_w = (
                            " | W adicionados: "
                            f"{format_int(payload['added_w'])}"
                        )

                    self.input_stats_var.set(
                        f"Tipo: {type_label}"
                        " | linhas: "
                        f"{format_int(payload['rows_read'])}"
                        " | válidos: "
                        f"{format_int(payload['valid'])}"
                        " | únicos: "
                        f"{format_int(payload['unique'])}"
                        " | duplicados: "
                        f"{format_int(payload['duplicates'])}"
                        " | inválidos: "
                        f"{format_int(payload['invalid'])}"
                        f"{extra_w}"
                    )

                elif event == "progress":

                    self.update_progress(
                        payload
                    )

                elif event == "rate_status":

                    self.update_rate_status(
                        payload
                    )

                elif event == "api_stats":

                    self.update_api_stats(
                        payload
                    )

                elif event == "key_test_ok":

                    self.status_var.set(
                        str(payload)
                    )

                    self.append_log(
                        str(payload)
                    )

                elif event == "key_test_error":

                    self.status_var.set(
                        "Falha ao testar a chave."
                    )

                    messagebox.showerror(
                        APP_NAME,

                        "Não foi possível validar "
                        "a chave:\n\n"
                        f"{payload}",
                    )

                elif event == "fatal_error":

                    messagebox.showerror(
                        APP_NAME,
                        str(payload),
                    )

                elif event == "finished":

                    output_path = (
                        payload[
                            "output_path"
                        ]
                    )

                    if payload[
                        "stopped"
                    ]:
                        messagebox.showinfo(
                            APP_NAME,

                            "Processamento interrompido.\n\n"
                            "O cache foi preservado e "
                            "um CSV parcial foi gerado "
                            "com os resultados disponíveis.\n\n"
                            f"Arquivo:\n{output_path}",
                        )

                    else:
                        messagebox.showinfo(
                            APP_NAME,

                            "Processamento concluído.\n\n"
                            "Linhas exportadas: "
                            f"{format_int(payload['rows_written'])}\n\n"
                            f"Arquivo:\n{output_path}",
                        )

        except queue.Empty:
            pass

        self.root.after(
            150,
            self.process_events,
        )

    # --------------------------------------------------------

    def on_close(
        self,
    ) -> None:

        if self.controller.running:

            answer = (
                messagebox.askyesno(
                    APP_NAME,

                    "Há um processamento "
                    "em andamento.\n\n"
                    "Deseja interromper e fechar?\n\n"
                    "Os resultados já gravados "
                    "no SQLite serão preservados.",
                )
            )

            if not answer:
                return

            self.controller.stop()

        self.root.destroy()


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    try:
        ensure_data_directories()

    except Exception as exc:

        root = tk.Tk()

        root.withdraw()

        messagebox.showerror(
            APP_NAME,

            "Não foi possível criar a pasta "
            "'data' ao lado do programa.\n\n"
            "Mova o .py/.exe para uma pasta "
            "onde você tenha permissão "
            "de escrita.\n\n"
            f"Detalhes:\n{exc}",
        )

        root.destroy()

        return

    root = tk.Tk()

    ColetorOpenAlexApp(
        root
    )

    root.mainloop()


if __name__ == "__main__":
    main()
