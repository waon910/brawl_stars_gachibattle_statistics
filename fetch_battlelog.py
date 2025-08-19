import os
import json
import requests
from dotenv import load_dotenv
from urllib.parse import quote

def main():
    load_dotenv()

    api_key = os.getenv("BRAWL_STARS_API_KEY")
    tag_raw = os.getenv("PLAYER_TAG")      # 例: "#PQLOJ9RQG"（オプション）

    if not api_key:
        raise RuntimeError("環境変数 BRAWL_STARS_API_KEY が設定されていません。")

    if not tag_raw:
        raise RuntimeError("PLAYER_TAG を .env に設定してください。")
    tag_enc = quote(tag_raw, safe="")  # "#PQLOJ9RQG" -> "%23PQLOJ9RQG"

    url = f"https://api.brawlstars.com/v1/players/{tag_enc}/battlelog"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }

    print(f"GET {url}")
    resp = requests.get(url, headers=headers, timeout=15)

    # エラーハンドリング（内容も表示）
    try:
        resp.raise_for_status()
    except requests.HTTPError:
        print(f"HTTP {resp.status_code}")
        print(resp.text)
        raise

    data = resp.json()
    print(json.dumps(data, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
