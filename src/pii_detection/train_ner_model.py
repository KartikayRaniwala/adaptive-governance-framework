# ============================================================================
# Adaptive Data Governance Framework
# src/pii_detection/train_ner_model.py
# ============================================================================
# Fine-tune a DistilBERT model for NER on synthetic Indian e-commerce PII.
# Trains on generated data containing PERSON, EMAIL, PHONE, AADHAAR, PAN.
# Outputs a saved model + tokeniser ready for inference via pii_detector.py.
# ============================================================================

from __future__ import annotations

import json
import os
import random
import string
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
from loguru import logger


# ---------------------------------------------------------------------------
# Label scheme (BIO tagging)
# ---------------------------------------------------------------------------

LABEL_LIST = [
    "O",
    "B-PERSON", "I-PERSON",
    "B-EMAIL", "I-EMAIL",
    "B-PHONE", "I-PHONE",
    "B-AADHAAR", "I-AADHAAR",
    "B-PAN", "I-PAN",
    "B-ADDRESS", "I-ADDRESS",
]

LABEL2ID = {label: i for i, label in enumerate(LABEL_LIST)}
ID2LABEL = {i: label for i, label in enumerate(LABEL_LIST)}


# ============================================================================
# Synthetic training-data generator
# ============================================================================

def _random_indian_name() -> str:
    first_names = [
        "Aarav", "Priya", "Rajesh", "Ananya", "Vikram", "Sneha", "Arun",
        "Deepika", "Karthik", "Meera", "Rohan", "Kavita", "Amit", "Pooja",
        "Suresh", "Neha", "Manish", "Divya", "Rahul", "Lakshmi",
    ]
    last_names = [
        "Sharma", "Patel", "Iyer", "Gupta", "Kumar", "Singh", "Reddy",
        "Verma", "Nair", "Joshi", "Mehta", "Rao", "Pillai", "Das",
    ]
    return f"{random.choice(first_names)} {random.choice(last_names)}"


def _random_email(name: str) -> str:
    domains = ["gmail.com", "yahoo.co.in", "outlook.com", "hotmail.com"]
    parts = name.lower().replace(" ", ".")
    return f"{parts}@{random.choice(domains)}"


def _random_phone() -> str:
    return f"+91-{random.randint(7000000000, 9999999999)}"


def _random_aadhaar() -> str:
    return f"{random.randint(2000,9999)} {random.randint(1000,9999)} {random.randint(1000,9999)}"


def _random_pan() -> str:
    return (
        "".join(random.choices(string.ascii_uppercase, k=5))
        + "".join(random.choices(string.digits, k=4))
        + random.choice(string.ascii_uppercase)
    )


_TEMPLATES = [
    "Please deliver to {PERSON}, email {EMAIL}, phone {PHONE}.",
    "Customer {PERSON} placed an order. Aadhaar: {AADHAAR}.",
    "Contact {PERSON} at {EMAIL} for returns.",
    "Ship to {PERSON}, PAN {PAN}, phone {PHONE}.",
    "Review by {PERSON}: Great product! Reach me at {EMAIL}.",
    "Aadhaar {AADHAAR} PAN {PAN} name {PERSON} phone {PHONE}.",
    "{PERSON} verified with Aadhaar {AADHAAR}.",
    "Order for {PERSON}. Call {PHONE} or mail {EMAIL}.",
]


def generate_training_sample() -> Tuple[List[str], List[str]]:
    """Generate one (tokens, labels) sample using a random template.

    Returns
    -------
    tuple[list[str], list[str]]
        Aligned token and BIO-label lists.
    """
    template = random.choice(_TEMPLATES)

    # Generate replacements
    name = _random_indian_name()
    replacements = {
        "{PERSON}": (name, "PERSON"),
        "{EMAIL}": (_random_email(name), "EMAIL"),
        "{PHONE}": (_random_phone(), "PHONE"),
        "{AADHAAR}": (_random_aadhaar(), "AADHAAR"),
        "{PAN}": (_random_pan(), "PAN"),
    }

    tokens: List[str] = []
    labels: List[str] = []

    parts = template.split()
    for part in parts:
        matched = False
        for placeholder, (value, entity_type) in replacements.items():
            if placeholder in part:
                # Handle surrounding punctuation
                prefix = part[: part.index(placeholder[0])] if not part.startswith("{") else ""
                suffix_start = part.index("}") + 1 if "}" in part else len(part)
                suffix = part[suffix_start:]

                if prefix:
                    tokens.append(prefix)
                    labels.append("O")

                value_tokens = value.split()
                for i, vt in enumerate(value_tokens):
                    tokens.append(vt)
                    labels.append(f"B-{entity_type}" if i == 0 else f"I-{entity_type}")

                if suffix:
                    tokens.append(suffix)
                    labels.append("O")

                matched = True
                break

        if not matched:
            tokens.append(part)
            labels.append("O")

    return tokens, labels


def generate_training_dataset(n: int = 10_000) -> List[Dict]:
    """Generate *n* training samples in HF-datasets format."""
    dataset = []
    for _ in range(n):
        tokens, labels = generate_training_sample()
        label_ids = [LABEL2ID.get(l, 0) for l in labels]
        dataset.append({
            "tokens": tokens,
            "ner_tags": label_ids,
        })
    return dataset


# ============================================================================
# Training entry-point
# ============================================================================

def train_ner_model(
    output_dir: str = "models/pii_ner",
    model_name: str = "distilbert-base-uncased",
    num_train_samples: int = 10_000,
    num_eval_samples: int = 2_000,
    epochs: int = 3,
    batch_size: int = 16,
    learning_rate: float = 2e-5,
) -> None:
    """Fine-tune a DistilBERT model for PII NER.

    Parameters
    ----------
    output_dir : str
        Where to save the trained model.
    model_name : str
        Base model from Hugging Face Hub.
    num_train_samples : int
        Number of synthetic training samples.
    num_eval_samples : int
        Number of evaluation samples.
    epochs : int
        Training epochs.
    batch_size : int
        Per-device batch size.
    learning_rate : float
        AdamW learning rate.
    """

    try:
        from datasets import Dataset
        from transformers import (
            AutoModelForTokenClassification,
            AutoTokenizer,
            DataCollatorForTokenClassification,
            Trainer,
            TrainingArguments,
        )
    except ImportError:
        logger.error(
            "transformers and datasets libraries are required. "
            "Install with: pip install transformers datasets"
        )
        return

    logger.info("Generating synthetic NER training data …")
    train_data = generate_training_dataset(num_train_samples)
    eval_data = generate_training_dataset(num_eval_samples)

    train_ds = Dataset.from_list(train_data)
    eval_ds = Dataset.from_list(eval_data)

    logger.info("Loading tokeniser and model: {}", model_name)
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForTokenClassification.from_pretrained(
        model_name,
        num_labels=len(LABEL_LIST),
        id2label=ID2LABEL,
        label2id=LABEL2ID,
    )

    # Tokenise and align labels
    def tokenize_and_align(examples):
        tokenized = tokenizer(
            examples["tokens"],
            truncation=True,
            is_split_into_words=True,
            max_length=128,
        )
        all_labels = []
        for i, label_ids in enumerate(examples["ner_tags"]):
            word_ids = tokenized.word_ids(batch_index=i)
            aligned = []
            prev_word = None
            for wid in word_ids:
                if wid is None:
                    aligned.append(-100)
                elif wid != prev_word:
                    aligned.append(label_ids[wid])
                else:
                    aligned.append(-100)
                prev_word = wid
            all_labels.append(aligned)
        tokenized["labels"] = all_labels
        return tokenized

    train_tokenized = train_ds.map(tokenize_and_align, batched=True)
    eval_tokenized = eval_ds.map(tokenize_and_align, batched=True)

    # Metrics
    def compute_metrics(pred):
        predictions = np.argmax(pred.predictions, axis=-1)
        labels = pred.label_ids

        true_preds = []
        true_labels = []
        for p_seq, l_seq in zip(predictions, labels):
            for p, l in zip(p_seq, l_seq):
                if l != -100:
                    true_preds.append(ID2LABEL[p])
                    true_labels.append(ID2LABEL[l])

        correct = sum(p == l for p, l in zip(true_preds, true_labels))
        total = len(true_labels)
        return {"accuracy": correct / total if total else 0}

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    training_args = TrainingArguments(
        output_dir=str(out),
        num_train_epochs=epochs,
        per_device_train_batch_size=batch_size,
        per_device_eval_batch_size=batch_size,
        learning_rate=learning_rate,
        weight_decay=0.01,
        eval_strategy="epoch",
        save_strategy="epoch",
        logging_steps=50,
        load_best_model_at_end=True,
        report_to="none",
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_tokenized,
        eval_dataset=eval_tokenized,
        data_collator=DataCollatorForTokenClassification(tokenizer),
        compute_metrics=compute_metrics,
    )

    logger.info("Starting training for {} epochs …", epochs)
    trainer.train()
    trainer.save_model(str(out))
    tokenizer.save_pretrained(str(out))

    # Save label mapping
    with open(out / "label_mapping.json", "w") as f:
        json.dump({"label2id": LABEL2ID, "id2label": ID2LABEL}, f, indent=2)

    logger.info("Model saved to {}", out)


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Train PII NER model")
    parser.add_argument("--output", default="models/pii_ner")
    parser.add_argument("--model", default="distilbert-base-uncased")
    parser.add_argument("--samples", type=int, default=10_000)
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=16)
    args = parser.parse_args()

    train_ner_model(
        output_dir=args.output,
        model_name=args.model,
        num_train_samples=args.samples,
        epochs=args.epochs,
        batch_size=args.batch_size,
    )
