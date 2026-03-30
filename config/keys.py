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
    "CORE_CRAWLER_TUNNELS_11880_JSON": {"encrypted": 'v1:gcm:66nqB2UrnHguZefbKhp-XIl5JhsHZoOkl6u-jDY-GMtNYOkPBltVb_suR6h2EdEiMQgRvwurme3MDLU9lU6XChu3pECVMG8KN5oFBUQDjnemr3NA_Ftt-lVUz5u7uY-ReVMxfXEulw5RtsEaK_aoEZ9lYZm0nDzCmu-dNg4wZj6n39WotaiRkhibvvjQjrm76IbN2Vm9wKbPLjHJS6qMI__8aHh_ahl0wqJ4Vl47itoBkuukFngDXLHki0eunwUkQT86KTfFi3kpu98ZGH5zh2r-TcXnNBDUzqIexXhaxUS4ACqk9uHMh9g84B0FjKtsW6dkCgC9-3IwtRy1wrmwH5oSwzROtvTayK5OkXiUmzEZiSgTBYbE4RMe_pSW9EVn3uU9-9QNFNGaA5i_JdcrOhlpSD3x6JgkV0b-AUI_irc_MjNnM9vCt0ht9A220HMgLjdXSNuFfa_Ap4MVrKbHm12bb1QcQQv6339c7-HCldqPgTsEy9hELGaJPOBET-Rdpvcy-0Z6xUHxFit4UpvgFXhEbJFnfo9AfFUuGLhEx0Wy2-rjmHFhT_2g59NjejuKTpWQ24Pju7-yarG79efLsCrUVgW_0j_eH4vIwIrCtdbTE3dbIp5ZorYGhnEkOm232i6pf3OfSdU_oC1lqv2mnC5cglMVWb5uOInT0AjP91IoxOzXGjq_vMgTL9vrV9bkaMQAAP9vOEyjLIGPITFW2uvubteMYDNw3S55xhuuk95lOd_daa6er1k8_FYd4fBAysZn6VbjDbLCHsolH5GyhEbxzMBUd4BtFrrKQd09sFnhj5NMIYRAXKyAHbKv0mhzAyGI-2XV28OLiiFrh155jumqHBlmEz_0QED6uTdg0EoMD9dBEYNqXsAEnbJay8_aSyazfdN6aLuSWWO1PdT_w0uhPXC3QNLFJXB-GL33-eCj4UmLaakwqMPWmSd6iJTnPQijWw2pg_oisZGe8yls2Z3R4_ilxJF0VEiyIrA_GF7r_fir2jMXyre4yabmXz2mt2KIuqyqRXBTd3PkJ750g9g_aIgmm7TT3XvCxYXr8oRM6qTjt4STHLEMX7pZ_HkXf0yTyvCbWk4jXPGSkM7NRLnIHRDY6MxG91UGPh7BO_TisV8fBWVv4Mpw_kSwgzJIeoWUbRW_L4T8xdTAnLQg3sKzQR-eJcfu9_8N113jtoC7UPZwkOD0mp3LeTmzSJ9JnPqMwSoA904nbosNJ7D8A8LcBlF7vW0rgbMs6Cke5TbAebD_KwzEsdfnxCQaV5_iylrSxoUpUY41zjC-yY6wxIvjXWMnu49CEoxDttVmswRknCh0oac3lRlYM_T2jbio5xNVh2DZBqjS2zCHueS2fiHGuFggPJnmegSPO0mUqFOKLyOeZwKIP1eg09jJof2MgxioDsKDigyCS_BQB18JErJAVTfWkoM2rXvNcUHumMyJWCDgkwId9YFkVCmF-13Kew_hBXHZMrNY5dKDhhlu4plG8h8ro6P5h_bV8y3msWmevlxDPnVhoH6lDlCVIATYFOef3RfSoGGiNLS8PelHf-GtgJfc5zdXN5e77f3dXnyvXVCqOCdYxU3kksmDKuFMuAiYYe-BenoZ4pOtuYFrl8411L_1q0CQF9_mfDE6_6vQqWxGHFxFFXoW2ClLOKcM1bQ9li2I6owig08Zk698KR8eF9ywVghIhUcc67M6PG6H_3EU1zlQ4b9qtIs4Z_Lt3jcs_TiTHnI_qXHnmC5CmGdPMqLYgpSjq1TbLUzyaCRTzy0R4KI97u-IayQ7g6K-twGEZSLUz510tOD348jrnPoYHiuq0GXqg9k-P2nmVlvPhgySwdyjJaglbMwh-mXFCs2dXdrlsnWdifY2biA2nuAQonq76DVKWG1FYsRMlkJ0eOJLJn7VW_g5CoQKZmhO6mKM5OnIJMXC3pUuw9pGT8h3f4Qjb71TrZUodghm7F3AfwIuwqNMGvjwzBbKbDhAP4UlPs_VG6IMXfeeFF8v9oIZ2Efzp3oi0nFmJ8ZFnNAu0O5W7eKHpQcnFKKSIuwiN5ZgX-AE1b7YjZlN6lFFZSmbDJyfdjebHu5sIoLLw6BaiS8GBpdpGcHn7sJ1HjLWnqKq2bUOpj4PPkCfZkZDYfUDeEsSsxIwXdZNEKfOx43D5cm_Yy3D1d3YoTywoDmFL7ZLjEO6Ocgpu2JyXcvQ0VMhPtU8ej18b6U-Z7fCdmsAkS9zlj5IMvOn_ke-aG7LhDHph2LFK_PQ4SbLwoASyPuC-VmnSIfGMxRt_v3uiVDc0_n-DtqB47pKXcqoEJCVUM2cmZtmfje9on3bgrXnvhgnWkOcV5Jn2Vnb', "decrypted": ''},
    
    
}
