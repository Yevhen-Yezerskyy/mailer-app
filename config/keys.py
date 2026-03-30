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
    "DJANGO_SECRET_KEY": {"encrypted": 'v1:gcm:ExytGg0s0YzojQSiMRP6NqhR2EdVQ56gfi9vHVsmvzZ1lTQ9-LxUlNQVMs5nc2Zjo0ElIAfClhbzDLCjdp-3M4UWuJOhSUngVBc6cpFQLvlbEG3_8Wm_J9RKDglX5tJCqQS-CNJg4YPcpsTjLkThD7KXXie4BMEkfg8jcwTc-fh3pA', "decrypted": ''},
    
    # Database
    "DB_HOST": {"encrypted": 'v1:gcm:CNFMbLnmyW33yQ1NAGQmuW5Q1JIuV1qbrttL4oGcMUrYmMS7qw', "decrypted": ""},
    "DB_NAME": {"encrypted": 'v1:gcm:OmdMTwqRZDCBYGus2PVkp_vUu03AJvkhMFbCDB-hPGxPSE288w', "decrypted": ""},
    "DB_PASSWORD": {"encrypted": 'v1:gcm:9Qsvm6dVqQqvV5WYtrQeEXnIxcELrj--CpS6eZGJ7MapbQ', "decrypted": ""},
    "DB_PORT": {"encrypted": 'v1:gcm:Y7t5kDfjgckEo-ElOtjq0fYLGyxZZCQd732j7gd6ySc', "decrypted": ""},
    "DB_USER": {"encrypted": 'v1:gcm:sg49x4qSDLvWqg0jWpnu9ZZB1dHmsaUGaSRmFUTUW4cDW-PcmbV5uw1u', "decrypted": ""},

    # Core crawler tunnels
    "CORE_CRAWLER_TUNNELS_11880_JSON": {"encrypted": 'v1:gcm:1FIatbSzz_fuC8hJRlnttvu_gdm705agG4bo7oSokXcOUSoH0mTr-J6CqfFH7Vnz-LP5j7hFZiYrAIo_a2yVdcLdQFCImo17B8zlxjcblEZ9vIFs8DVjrVIBL30NeMf8fu_a52IKC-UgvwdgzYsTPd7jL59mM7WP8BoRVYNt9zVqYQUeMHm8lPWLg0PqAe9Zn2oELbg6Aov6WtzAxbvBCuzYzjppQ9te9KPBk-dR0rFtJSlqGlZl9Kg51B8p-sXdRZJaRSmUb0OMQ5wsUq9iIkRruG5ik3cj7C_cvxjSFSxXlnl6s1BcVo9l3OtPriI5i1Cl2yfFTsE_So0rPgXbjiKzMgil4Ikzg6QkhdmarQ-H7lwAOOu2BuJF2GdiY4GG0bn1QDk2ViM8qBIPfN3QpWg0KN4teWWMqt6TUMAPrrsUWKwuC1aGMp8reptuJDD323nA62ZPHvayEzsbB-Dew_aDLRTZyyIV-zcQVBK8EWNantnVUrpcYu58br3jgu2PiSKLPrCVVecCANuYPZDgMMmtePSG3nFNwp3WJN-P1pFwSw1px-q0SAcT2hXnt_EFs2cplTyLtLqb7RkdyTeFUTH0UIkjr_0gyAapFqcO_uPIVBDz8iq2aB9DdsEXyHtNHGLMec1UQedFq7K3i28bvbO7g6YpLB_LIMr5utqGFwMLdYydg-5SUBpihJDd1JDZkktIl306jO2KnAOR9dVo33EU_TLF6RFnPQhgYdQviawh_mAypsdLF8Kkb-DGFdz3u4TAQKDtMbPx_zplXOS90tVeQSAm9HHFhMQv6MGZZr-Kqop2rlG8Ho6rKA_m2atz08t0DX_B-hWjjkStTo_96YMpMdokwSTJPlQAeJi7S_0db3CkuXZokOXmO8jpAilBEnoM0OQTF_0WAmjw2VNv_qILpc9tE5q4F4zIf1MTl_0IpXu-W7MTyHXseO_UFBkOE9EBjM5RiAiwsAwC7HICeGfr4BW4-F4IzfEe-OVkS4LxPDG2EpgoPc95L_NqvZC4rtNX2cg77mZG_r0kYWe7SsWoIzcnd6gSpf8XlbAnDvS3wNeH6D3QXv2RqxCb4e__omhGlrP3bx-_4p8G0wdyLXy1wvZ0SsqaeU9MuSPUYeDAtgWD-3CZf9FOanE4VtKCU3ZoIp6x-wmRtLcco_qh0ESkYjzBipn2m3VCd9vo10FqedvMLmGeoqOSw51RFH4_zFEiGxzwuzVPnXpWHjw5G5T-UFXIQpdO5HCLPJyg8LoJ23A76YYiSN60mfz-npWrcGhTCHvclIIFmcKQ7VpzOjCJlvGw5DykQtqII6m8JTQbbeAjlN3I34cNt7Z68wrb0MHnPP9kDs7tcRqB5-xY6QDfEFKR0RvH2V2kt4CcYUzN32CKCsfVKMZ_8kwv9lGVeW3ahW0inYHw4D6BquHI86P2EJ2x_iWovvGVYV4iV9ED1nX6u0tEXMASdqtJEU9MenVKb_i6BQABH45LqWhyoFc-1-fbesWsevoWfkCiW8iP2KSXpY7foT16WUP7ZyIBtBnlvBAbRCF2jKT2I8QEDTNJesJWLwcry8TCrXTt70ix1-cwDsPIF9--Z7ukAmHBNeemKaoBevIB55WfT9xhgBZ1TdeLolrLsBz3JkSzIV-tRYeRRB9D-c_9ErKHLrFWjo8hstRnqUyuPqi3vrE8_RMPl6Q3ggAyivwR3x-9YftDkZgPVPMexFNz-WBT6cVYlKAySzTEjt2IJ8MEbaRFQrBMMAgCOJgJ1mmKIDpTJ3m5TZfKaQ0wVkQIjIAnwgSgMzhlmzwA4_k9nbZ-tLb-YIoT8JbXV__t1B8T9jHZa1hGn5BFEjkXsKi1OD7JLvJq3KiCoGfHh5CMiimeI84SbSn-PFW86czCoF_AT_QWQTSNUdzzFXFC4eQD_UUMBKsaySJO4QcGQUwo6OKnwAS5z7fXYe3SLwbC7Md10m5qHTcLZiAcuYJhLQvlwerZ5N9P2jBViMKPXz-LgeADaWrzDYvB_bR__yEboYr9wpKEEnFPYaMdlp3sGGxHCgIxlSOvMU0TKIa2eM8rlWqIhkKlg10Ct3_NG38Yv5ibqxBHqPRzbYXspMxbdOCOEXBSfJ3DedqIFGmzWxqk3-YBGo2Y5raFBPz4rDCpf8KXilZmicBxSeRCL1dZsn9lPGQBTA', "decrypted": ''},
    
    
}
