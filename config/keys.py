# FILE: config/keys.py
# DATE: 2026-01-25
# PURPOSE: Project key registry (committed). Store ONLY encrypted values in git; plaintext lives only in-process via load_keys.py.

from __future__ import annotations

from typing import Dict, TypedDict


class KeyItem(TypedDict):
    encrypted: str
    decrypted: str


# NOTE:
# - Key ID == env var name.
# - Commit rule: decrypted MUST stay "" in git.
# - encrypted format: "v1:gcm:<base64url(nonce|ciphertext|tag)>" (produced by config/load_keys.py)
KEYS: Dict[str, KeyItem] = {
    
    "SERENITY_PASS_KEY": {"encrypted": 'v1:gcm:_tFpY9jHTGe_MLrDsXmvshZ9E20wqGyKRzu5_Fdpo0m8kjBpMLLlIpZKmbQJVpeysD8UM30G4hjy33BV6R2BVFb_PcXH2yo', "decrypted": ''},
    #GPT
    "OPENAI_API_KEY": {"encrypted": 'v1:gcm:UUJEChGl_M-JJpxpKHBP4yw5ChM0PCVvbyZWmSvGr031LQSxxRwCyXv6y4hxQ759iUOsNGwtJdA7YUxCuEHAEtcBNvIZbgWWQxrQfCNeRBzaVFbR7mOijGg2EBTULivuksds6S7zLcUI4ozDHB5ShPBAvMC5sKVec_63V-9wgF5bO8_Ng5-5cvO8T36aaYkjYoKnBbgjwr0-mAnbzix-KSrOZhtR3AM90dgIrcX-_16cQ33hsog5zAo11S_ipzd6', "decrypted": ''},
    
    # Database
    "DB_HOST": {"encrypted": 'v1:gcm:CNFMbLnmyW33yQ1NAGQmuW5Q1JIuV1qbrttL4oGcMUrYmMS7qw', "decrypted": ""},
    "DB_NAME": {"encrypted": 'v1:gcm:OmdMTwqRZDCBYGus2PVkp_vUu03AJvkhMFbCDB-hPGxPSE288w', "decrypted": ""},
    "DB_PASSWORD": {"encrypted": 'v1:gcm:9Qsvm6dVqQqvV5WYtrQeEXnIxcELrj--CpS6eZGJ7MapbQ', "decrypted": ""},
    "DB_PORT": {"encrypted": 'v1:gcm:Y7t5kDfjgckEo-ElOtjq0fYLGyxZZCQd732j7gd6ySc', "decrypted": ""},
    "DB_USER": {"encrypted": 'v1:gcm:sg49x4qSDLvWqg0jWpnu9ZZB1dHmsaUGaSRmFUTUW4cDW-PcmbV5uw1u', "decrypted": ""},
    
    
}
