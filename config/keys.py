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
    "CORE_CRAWLER_TUNNELS_11880_JSON": {"encrypted": 'v1:gcm:yG2D-rdAmyh3x_FUglenHNUeQ54Tyi6bihSD7W1Sp_GAsZqZlrHMYiSpZXu2n97GcpFY4GYwwfjTAYbC-7NM6_NajPSDPmMilQm7D66uK7XMywF_oNdMAkhzE9VqRI3q0qgLEhPXWONUpS7djdBE63i4SW7yZ4o8lioCmt0ORcPg7tRd6M-Bivst2Evgmpu1vVfRNyLWfXVV0yzwpWKF1zGotma0bh2f-fApeqreAakyDljh4uVByJHQsZsO1K_mXaU6puoS9Fm9sY54F3uxNvB0aIBCnEWlIh4Zg0xobr52HhHNTWIoJweV0ZRDlKO_DixCwmZPyQshD-Ekr07ioRgxJOkyj0AaC6zAUtrZKaMKLL3G2RMHuvLHvaqtBmi460Jns8zv-lt6QNW2Lf9_JOkYXNzcOIQXvJyrE68FdCVxWHj4KpQg_iqmv68BKuQH_kZmj_KaV2rJI1eYYJJs4SQ4StRMEtlWJqcezDLpcrmcOeani0Vnun7Q1T21n8W72-QYynTIqz3SDB_paw4oPXWdrpXEBTdVTEuQ3_Azr9TFWePWbk0hK9rmyQK6S890Y7Nl59zELK8hl2d9riLOlwg0TTZcqKEnaylwuv6i_v83ODTY4IYWPT1b_edJWEe6H6hf35hDYUyuA95LTfY3QxgZnzQ0PUHk48nmG_vSUpmOXR0LAx0CswdYPq1_YYBPmWw3MJwcrjqPNayDnvAffdAlWdZsHKtmfS9Sk68uflay1jyWsbzUgumsMxDkW69m304ETH9_zHUnU9WZ2mgyUQdqcGAhHCKTGf-aobuNg6cxfyBJQSLzbDHW8xHEpibPA1LzOQea6IytrNfJn5COmpIUk5ZeO5qK0GIipBVAQKjyVHhWLFFcEav9X7MT2vq1w0maFLYJAwk6DIRw-iY0zWF21MOjCV34zYoQA0FL2Num-ZfKoroH06Up9wXMFNh7u0piwGUbmez1G-mJbTzE_SolTw_I1BABkgcvUBNk6mclfqnWyclHBZ5lfir6A_1cm3FRqPXCdUA8C_6KxjODSb-LcIm6z3Wi8EXvgvYNbLYDZp6JGmGv9GRv3ChJ9mCOyZlWyQPGBMRp-l1xO5uEgsVYo5kgx2uebN5sG1m7alHZh_uSu0JVo01F9R87bIn3EpvSMZg96GwVN2CgZKx55QsFiy7a2gy1jbscdNAQBWEe19svDUtm_31Nn1xrXfE3biuK71NCClfoPkGOMWQYAUjaqidV2fyKt2NvZNV4VbOWcZOfxCVYSvyIdSCRxGA4kSSqq_IsgNQHnz2UIDdpLzWVK1TWnGN-JXZ5-CbFgAwv1xTKdPuS-JUen2t9oZ5sql4ULDwuicEnP7Xot9l-ZZ3TM2PgYR9xoxGVy4macc7HAH4gytATRekjTjVD56WKJuM5z-mNiVHFdHadL2mLSPEiKMGCwjSiX3UzCouEl9pIHazModJdv_RrR-U0eEVlqxoZsYavUs49Rz-rl4OoGlMUoHfARktcaIJ2CsCCGS1fOrAShASFilNqQKT5oF779u1NtlmYyMGxFaoLnDW1C8vwV1LW9GGHV23BQeVHGrTQPZV275p2YqqdjQ_b8gEw_EsjDtdspyB8q7zcJjpeNEeM7nlGnxHoxjI-LmYBaIgTxUeLLsUK0k4NB283HpeHqMZG3FirTs4GkRRVUA', "decrypted": ''},
    
    
}
