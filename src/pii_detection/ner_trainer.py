# ============================================================================
# Adaptive Data Governance Framework
# src/pii_detection/ner_trainer.py
# ============================================================================
# NER Training Dataset Generator
#
# Generates BIO-tagged (Begin-Inside-Outside) training data for fine-tuning
# custom Named Entity Recognition models on Indian PII patterns.
#
# Entities supported:
#   PERSON, EMAIL, PHONE, AADHAAR, PAN, ADDRESS, ORGANISATION
#
# Each sample is a tokenized sentence with corresponding BIO tags.
# ============================================================================

from __future__ import annotations

import random
from typing import Dict, List, Tuple

from loguru import logger

# ---------------------------------------------------------------------------
# Name / entity pools (Indian context)
# ---------------------------------------------------------------------------

_FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh",
    "Ayaan", "Krishna", "Ishaan", "Ananya", "Diya", "Myra", "Sara",
    "Aadhya", "Ira", "Anika", "Prisha", "Kavya", "Navya", "Rahul",
    "Priya", "Amit", "Neha", "Sanjay", "Pooja", "Rajesh", "Meena",
    "Kartik", "Shreya", "Rohan", "Tanvi", "Vikram", "Anjali", "Deepak",
]

_LAST_NAMES = [
    "Sharma", "Singh", "Patel", "Kumar", "Gupta", "Reddy", "Joshi",
    "Mehta", "Verma", "Iyer", "Nair", "Rao", "Desai", "Malhotra",
    "Chopra", "Bhat", "Shah", "Das", "Mukherjee", "Srinivasan",
    "Raniwala", "Agarwal", "Kapoor", "Banerjee", "Thakur",
]

_ORGANISATIONS = [
    "Tata Consultancy Services", "Infosys Limited", "Wipro Technologies",
    "Reliance Industries", "HDFC Bank", "ICICI Bank", "Flipkart",
    "Zomato", "Swiggy", "Paytm", "PhonePe", "Razorpay",
    "State Bank of India", "Bajaj Finance", "Tech Mahindra",
]

_CITIES = [
    "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Kolkata",
    "Pune", "Ahmedabad", "Jaipur", "Lucknow", "Noida", "Gurugram",
]

_STATES = [
    "Maharashtra", "Karnataka", "Tamil Nadu", "Telangana", "Delhi",
    "Uttar Pradesh", "Gujarat", "Rajasthan", "West Bengal", "Kerala",
]

_TEMPLATES = [
    "My name is {PERSON} and I work at {ORG} .",
    "{PERSON} can be reached at {EMAIL} or {PHONE} .",
    "Please contact {PERSON} at {EMAIL} for details .",
    "The Aadhaar number of {PERSON} is {AADHAAR} .",
    "{PERSON} 's PAN card number is {PAN} .",
    "Invoice addressed to {PERSON} at {ADDRESS} .",
    "The customer {PERSON} placed an order from {CITY} .",
    "{PERSON} from {ORG} reported an issue with Aadhaar {AADHAAR} .",
    "Account holder {PERSON} with PAN {PAN} resides in {CITY} .",
    "Delivery for {PERSON} to {ADDRESS} confirmed via {PHONE} .",
    "{PERSON} registered with email {EMAIL} and phone {PHONE} .",
    "Employee {PERSON} at {ORG} has Aadhaar {AADHAAR} and PAN {PAN} .",
    "The order was placed by {PERSON} from {CITY} , {STATE} .",
    "{PERSON} submitted documents including PAN {PAN} .",
    "Contact {PERSON} at {ORG} , {CITY} for further assistance .",
]


# ---------------------------------------------------------------------------
# Random entity generators
# ---------------------------------------------------------------------------

def _random_person() -> str:
    return f"{random.choice(_FIRST_NAMES)} {random.choice(_LAST_NAMES)}"


def _random_email(person: str) -> str:
    parts = person.lower().split()
    domain = random.choice(["gmail.com", "yahoo.co.in", "outlook.com", "company.in"])
    sep = random.choice([".", "_", ""])
    return f"{parts[0]}{sep}{parts[1]}@{domain}"


def _random_phone() -> str:
    prefix = random.choice(["9", "8", "7", "6"])
    return f"+91 {prefix}{''.join(str(random.randint(0, 9)) for _ in range(9))}"


def _random_aadhaar() -> str:
    return f"{random.randint(1000,9999)} {random.randint(1000,9999)} {random.randint(1000,9999)}"


def _random_pan() -> str:
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return (
        "".join(random.choices(letters, k=5))
        + "".join(str(random.randint(0, 9)) for _ in range(4))
        + random.choice(letters)
    )


def _random_address() -> str:
    house = random.randint(1, 999)
    sector = random.randint(1, 50)
    city = random.choice(_CITIES)
    pin = random.randint(100000, 999999)
    return f"H.No {house} Sector {sector} {city} {pin}"


# ---------------------------------------------------------------------------
# BIO tagging helpers
# ---------------------------------------------------------------------------

def _tag_tokens(tokens: List[str], entity_text: str, entity_type: str,
                tags: List[str]) -> List[str]:
    """Find *entity_text* tokens inside *tokens* and apply BIO tags."""
    ent_tokens = entity_text.split()
    for i in range(len(tokens) - len(ent_tokens) + 1):
        if tokens[i: i + len(ent_tokens)] == ent_tokens:
            # Only tag if currently 'O'
            if all(tags[i + j] == "O" for j in range(len(ent_tokens))):
                tags[i] = f"B-{entity_type}"
                for j in range(1, len(ent_tokens)):
                    tags[i + j] = f"I-{entity_type}"
                break
    return tags


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_training_sample() -> Tuple[List[str], List[str]]:
    """Generate a single BIO-tagged NER training sample.

    Returns
    -------
    tuple[list[str], list[str]]
        ``(tokens, tags)`` where each tag is one of
        ``O``, ``B-PERSON``, ``I-PERSON``, ``B-EMAIL``, ``B-PHONE``,
        ``B-AADHAAR``, ``I-AADHAAR``, ``B-PAN``, ``B-ADDRESS``,
        ``I-ADDRESS``, ``B-ORG``, ``I-ORG``, etc.
    """
    template = random.choice(_TEMPLATES)

    # Generate entities
    person = _random_person()
    email = _random_email(person)
    phone = _random_phone()
    aadhaar = _random_aadhaar()
    pan = _random_pan()
    address = _random_address()
    org = random.choice(_ORGANISATIONS)
    city = random.choice(_CITIES)
    state = random.choice(_STATES)

    # Fill template
    sentence = template.replace("{PERSON}", person)
    sentence = sentence.replace("{EMAIL}", email)
    sentence = sentence.replace("{PHONE}", phone)
    sentence = sentence.replace("{AADHAAR}", aadhaar)
    sentence = sentence.replace("{PAN}", pan)
    sentence = sentence.replace("{ADDRESS}", address)
    sentence = sentence.replace("{ORG}", org)
    sentence = sentence.replace("{CITY}", city)
    sentence = sentence.replace("{STATE}", state)

    # Tokenize (simple whitespace split)
    tokens = sentence.split()
    tags = ["O"] * len(tokens)

    # Apply BIO tags for each entity that appears in the sentence
    entity_map = [
        (person, "PERSON"),
        (email, "EMAIL"),
        (phone, "PHONE"),
        (aadhaar, "AADHAAR"),
        (pan, "PAN"),
        (address, "ADDRESS"),
        (org, "ORG"),
        (city, "LOCATION"),
        (state, "LOCATION"),
    ]

    for entity_text, entity_type in entity_map:
        if entity_text in sentence:
            tags = _tag_tokens(tokens, entity_text, entity_type, tags)

    return tokens, tags


def generate_training_dataset(n: int = 100) -> List[Dict]:
    """Generate *n* BIO-tagged training samples.

    Parameters
    ----------
    n : int
        Number of samples to generate.

    Returns
    -------
    list[dict]
        Each dict has keys ``tokens`` (list[str]) and ``tags`` (list[str]).
    """
    dataset = []
    for _ in range(n):
        tokens, tags = generate_training_sample()
        dataset.append({"tokens": tokens, "tags": tags})

    logger.info("Generated {} NER training samples (BIO format)", n)
    return dataset
