import os
import json
import requests
from dotenv import load_dotenv
from urllib.parse import quote
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field

# 逆結果マップ
OPPOSITE = {"victory": "defeat", "defeat": "victory"}

@dataclass
class ResultLog:
    result: str = "不明"
    brawlers: list[str] = field(default_factory=list)

def main():
    load_dotenv()

    api_key = os.getenv("BRAWL_STARS_API_KEY")
    tag_raw = os.getenv("PLAYER_TAG")      # 例: "#PQLOJ9RQG"（オプション）

    if not api_key:
        raise RuntimeError("環境変数 BRAWL_STARS_API_KEY が設定されていません。")

    player_tag = tag_raw
    print(f"プレイヤータグ: {player_tag}")
    if not tag_raw:
        raise RuntimeError("PLAYER_TAG を .env に設定してください。")
    tag_enc = quote(player_tag, safe="")  # "#PQLOJ9RQG" -> "%23PQLOJ9RQG"

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

    out_dir = Path("out")
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"battlelog_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"保存しました: {output_path.resolve()}")

    print(f"取得したバトルログの数: {len(data.get('items', []))}")

    battle_logs = data.get("items", [])
    if not battle_logs:
        print("バトルログが見つかりませんでした。")
        return
    print("バトルログの詳細:")

    rank_id = ""
    for battle in battle_logs:
        rank=0
        battle_detail = battle.get("battle", {})
        if battle_detail.get("type") not in ["soloRanked"]:
            print(f"スキップ: {battle_detail.get('type', '不明')}")
            continue
        battle_map = battle.get("event", {}).get("map", "不明")
        battle_time = battle.get("battleTime", "不明")
        battle_duration = battle_detail.get("duration", "不明")
        battle_id = f"{battle_time}_{battle_duration}_{battle_map}"
        print(f"バトルID: {battle_id}")
        star_player = battle_detail.get("starPlayer") or {}
        tag = star_player.get("tag", "不明")
        if tag != "不明":
            rank_id = f"{battle_time}_{tag}"
            # ここですでに存在しているランクマッチ化を確認
            # まだ存在していなければ選ばれているキャラクターをカウント もしくはフラグを立てる
        print(f"rank_id: {rank_id}")
        result = battle_detail.get("result", "不明")
        teams = battle_detail.get("teams", [])
        resultInfo: list[ResultLog] = []

        my_side_idx = None  # 自分がいるチーム(0/1)

        for side_idx,team in enumerate(teams):
            resultLog = ResultLog()
            for player in team:
                resultLog.brawlers.append(player.get("brawler", {}).get("id", "不明"))
                if player.get("tag") == player_tag:
                    my_side_idx = side_idx
                    resultLog.result = result
                if rank < player.get("brawler", {}).get("trophies", 0):
                    rank = player.get("brawler", {}).get("trophies", 0)
            resultInfo.append(resultLog)
        

        if my_side_idx is not None and len(resultInfo) == 2 and result in OPPOSITE:
            other = 1 - my_side_idx
            # まだ埋まっていない場合のみ上書き
            if getattr(resultInfo[other], "result", "不明") in (None, "", "不明"):
                resultInfo[other].result = OPPOSITE[result]

        print(f"ランク: {rank} 結果：{resultInfo}")

    print("バトルログの取得が完了しました。")
            


if __name__ == "__main__":
    main()
